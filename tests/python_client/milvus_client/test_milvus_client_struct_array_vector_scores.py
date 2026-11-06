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

"""Struct array vector scores regression cases."""

import copy

import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType

DIM = 64

KINDS = [
    pytest.param(DataType.FLOAT16_VECTOR, "L2", id="float16_l2"),
    pytest.param(DataType.BFLOAT16_VECTOR, "IP", id="bfloat16_ip"),
    pytest.param(DataType.INT8_VECTOR, "COSINE", id="int8_cosine"),
    pytest.param(DataType.BINARY_VECTOR, "HAMMING", id="binary_hamming"),
]


def vector(kind, seed):
    rng = np.random.RandomState(seed)
    if kind == DataType.BINARY_VECTOR:
        return rng.randint(0, 256, DIM // 8, dtype=np.uint8).tobytes()
    if kind == DataType.INT8_VECTOR:
        return rng.randint(-100, 101, DIM, dtype=np.int8)
    dtype = np.float16 if kind == DataType.FLOAT16_VECTOR else cf.bfloat16
    return rng.normal(size=DIM).astype(dtype)


def decoded(value, kind):
    # Current SDK exposes binary children as [bytes] and BF16 children as uint16 bits.
    if kind == DataType.BINARY_VECTOR and isinstance(value, list) and len(value) == 1:
        value = value[0]
    if kind == DataType.BFLOAT16_VECTOR and np.asarray(value).dtype == np.uint16:
        return np.asarray(value, dtype=np.uint16).view(cf.bfloat16).astype(np.float64)
    if isinstance(value, bytes):
        dtype = {
            DataType.BINARY_VECTOR: np.uint8,
            DataType.FLOAT16_VECTOR: np.float16,
            DataType.BFLOAT16_VECTOR: cf.bfloat16,
            DataType.INT8_VECTOR: np.int8,
        }[kind]
        return np.frombuffer(value, dtype=dtype).astype(np.float64)
    return np.asarray(value, dtype=np.float64)


def distance(left, right, kind, metric):
    left, right = decoded(left, kind), decoded(right, kind)
    if metric == "HAMMING":
        return int(np.unpackbits(np.bitwise_xor(left.astype(np.uint8), right.astype(np.uint8))).sum())
    if metric == "L2":
        return float(np.sum((left - right) ** 2))
    dot = float(np.dot(left, right))
    return dot if metric == "IP" else dot / float(np.linalg.norm(left) * np.linalg.norm(right))


def row(number, kind, revision=0):
    shape = (number + revision) % 7
    elements = None if shape == 0 else []
    if shape > 1:
        elements = [
            {"value": number + offset + revision, "emb": vector(kind, number * 31 + offset + revision * 100003)}
            for offset in range(shape)
        ]
    return {"id": number, "revision": revision, "normal_vector": [float(number % 13)] * DIM, "clips": elements}


class TestMilvusClientStructArrayVectorScores(TestMilvusClientV2Base):
    def _create(self, client, kind, metric, index_type):
        name = cf.gen_unique_str("struct_precision")
        schema, _ = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("revision", DataType.INT64)
        schema.add_field("normal_vector", DataType.FLOAT_VECTOR, dim=DIM)
        struct, _ = self.create_struct_field_schema(client)
        struct.add_field("value", DataType.INT64)
        struct.add_field("emb", kind, dim=DIM)
        schema.add_field(
            "clips", DataType.ARRAY, element_type=DataType.STRUCT, struct_schema=struct, max_capacity=8, nullable=True
        )
        indexes, _ = self.prepare_index_params(client)
        indexes.add_index("normal_vector", index_type="FLAT", metric_type="L2")
        indexes.add_index(
            "clips[emb]",
            index_type=index_type,
            metric_type=metric,
            params={"M": 16, "efConstruction": 200} if index_type == "HNSW" else {},
        )
        self.create_collection(client, name, schema=schema, index_params=indexes)
        return name

    def _verify(self, client, name, model, kind, metric, index_type):
        rows, _ = self.query(
            client,
            name,
            filter="id >= 0",
            output_fields=["id", "revision", "clips"],
            limit=len(model) + 10,
            consistency_level="Strong",
        )
        actual = {item["id"]: item for item in rows}
        assert len(rows) == len(model) and set(actual) == set(model)
        for pk, expected in model.items():
            got = actual[pk]
            assert got["revision"] == expected["revision"]
            if expected["clips"] is None:
                assert got["clips"] is None
                continue
            assert len(got["clips"]) == len(expected["clips"])
            for left, right in zip(got["clips"], expected["clips"]):
                assert left["value"] == right["value"]
                np.testing.assert_array_equal(decoded(left["emb"], kind), decoded(right["emb"], kind))
        filters = [
            ("", lambda pk, element: True),
            ("element_filter(clips, $[value] % 3 == 0)", lambda pk, element: element["value"] % 3 == 0),
            (
                "id >= 700 && element_filter(clips, $[value] % 3 == 1)",
                lambda pk, element: pk >= 700 and element["value"] % 3 == 1,
            ),
        ]
        queries = [vector(kind, seed) for seed in (19, 59, 997)]
        ascending = metric in ("L2", "HAMMING")
        for expression, accept in filters:
            results, _ = self.search(
                client,
                name,
                data=queries,
                anns_field="clips[emb]",
                filter=expression,
                limit=20,
                consistency_level="Strong",
                output_fields=["revision"],
                search_params={"metric_type": metric, "params": {"ef": 512} if index_type == "HNSW" else {}},
            )
            assert len(results) == len(queries)
            for query, hits in zip(queries, results):
                truth = {
                    (pk, offset): distance(query, element["emb"], kind, metric)
                    for pk, item in model.items()
                    for offset, element in enumerate(item["clips"] or [])
                    if accept(pk, element)
                }
                ordered = sorted(truth.values(), reverse=not ascending)
                keys = [(hit["id"], hit["offset"]) for hit in hits]
                assert len(keys) == 20 and len(set(keys)) == 20
                scores = []
                for hit, key in zip(hits, keys):
                    assert key in truth
                    assert hit["distance"] == pytest.approx(truth[key], abs=3e-3, rel=3e-3)
                    assert hit["entity"]["revision"] == model[key[0]]["revision"]
                    scores.append(hit["distance"])
                assert scores == sorted(scores, reverse=not ascending)
                cutoff = ordered[19]

                def at_cutoff(score):
                    return score <= cutoff + 1e-7 if ascending else score >= cutoff - 1e-7

                if index_type != "HNSW":
                    assert all(at_cutoff(truth[key]) for key in keys)
                    strict = (
                        {key for key, score in truth.items() if score < cutoff - 1e-7}
                        if ascending
                        else {key for key, score in truth.items() if score > cutoff + 1e-7}
                    )
                    assert strict <= set(keys)
                else:
                    # Tie-aware quality threshold for this fixed corpus, not a universal ANN guarantee.
                    assert sum(at_cutoff(truth[key]) for key in keys) / 20 >= 0.95

    @pytest.mark.tags(CaseLabel.L1)
    def test_struct_array_int8_flat_rejection_preserves_data_and_recovers(self):
        """
        target: reject the unsupported INT8/FLAT combination without changing stored rows
        method: replace an existing index with FLAT, assert rejection, restore HNSW and verify
        expected: error 1100, no failed index left behind, exact rows and rankings after recovery
        """
        client = self._client()
        kind, metric = DataType.INT8_VECTOR, "COSINE"
        name = self._create(client, kind, metric, "HNSW")
        rows = [row(i, kind) for i in range(1100)]
        model = {item["id"]: copy.deepcopy(item) for item in rows}
        self.insert(client, name, rows)
        self.flush(client, name)
        self.release_collection(client, name)
        names, _ = self.list_indexes(client, name, field_name="clips[emb]")
        assert len(names) == 1
        self.drop_index(client, name, names[0])
        invalid, _ = self.prepare_index_params(client)
        invalid.add_index("clips[emb]", index_name="unsupported_flat", index_type="FLAT", metric_type=metric)
        self.create_index(
            client,
            name,
            invalid,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "can't build with this index FLAT"},
        )
        names, _ = self.list_indexes(client, name, field_name="clips[emb]")
        assert names == []
        valid, _ = self.prepare_index_params(client)
        valid.add_index("clips[emb]", index_type="HNSW", metric_type=metric, params={"M": 16, "efConstruction": 200})
        self.create_index(client, name, valid)
        self.load_collection(client, name)
        self._verify(client, name, model, kind, metric, "HNSW")

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "kind,metric,algorithm",
        [
            pytest.param(p.values[0], p.values[1], algorithm, id=f"{algorithm}-{p.id}")
            for p in KINDS
            for algorithm in ("exact", "hnsw")
            if not (p.values[0] == DataType.INT8_VECTOR and algorithm == "exact")
        ],
    )
    def test_struct_array_vector_precision_offsets_after_mutations(self, kind, metric, algorithm):
        """
        target: preserve typed vector bytes, nullable row shapes, element coordinates and scores
        method: compare 1100 rows and independent rankings, then replace/delete/reload and repeat
        expected: exact vector payloads and valid tie-aware top-k across FLAT and HNSW paths
        """
        client = self._client()
        index_type = "HNSW" if algorithm == "hnsw" else ("BIN_FLAT" if kind == DataType.BINARY_VECTOR else "FLAT")
        name = self._create(client, kind, metric, index_type)
        rows = [row(i, kind) for i in range(1100)]
        model = {item["id"]: copy.deepcopy(item) for item in rows}
        self.insert(client, name, rows)
        self._verify(client, name, model, kind, metric, index_type)
        self.flush(client, name)
        self.release_collection(client, name)
        self.load_collection(client, name)
        self._verify(client, name, model, kind, metric, index_type)
        removed = list(range(0, 1100, 11))
        self.delete(client, name, ids=removed)
        for pk in removed:
            del model[pk]
        updates = [row(i, kind, revision=3) for i in range(3, 1150, 17)]
        self.upsert(client, name, updates)
        model.update({item["id"]: copy.deepcopy(item) for item in updates})
        self.flush(client, name)
        self.release_collection(client, name)
        self.load_collection(client, name)
        self._verify(client, name, model, kind, metric, index_type)
