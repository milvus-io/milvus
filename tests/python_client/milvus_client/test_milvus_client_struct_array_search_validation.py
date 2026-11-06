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

"""Struct array search validation regression cases."""

import pytest
from common.common_type import CaseLabel, CheckTasks
from common.struct_array_nullable_helpers import (
    StructArrayNullableBase,
    _clips,
    _core_children,
    _record,
    _vector,
)
from pymilvus import AnnSearchRequest, DataType, RRFRanker
from pymilvus.client.embedding_list import EmbeddingList


class TestMilvusClientStructArraySearchValidation(StructArrayNullableBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_element_filter_cannot_cross_struct_boundaries(self):
        """
        target: reject element filters that refer to another Struct with identical child names
        method: opposing NULL masks in two structs, cross-filter rejection and same-struct control
        expected: explicit same-struct restriction and exact coordinates from the allowed search
        """
        client = self._client()
        name = self._create(client, _core_children())
        other = client.create_struct_field_schema()
        other.add_field("score", DataType.INT64, nullable=True)
        other.add_field("emb", DataType.FLOAT_VECTOR, dim=8, nullable=True)
        client.add_collection_struct_field(name, "other", other, max_capacity=8, nullable=True)
        index = client.prepare_index_params()
        index.add_index("other[emb]", index_type="FLAT", metric_type="L2")
        self.create_index(client, name, index)
        rows = [
            _record(n, [{"anchor": 0, "score": None, "label": None, "emb": _vector(n), "tags": None}]) for n in range(4)
        ]
        for row in rows:
            row["other"] = [{"score": 1, "emb": _vector(row["id"] + 10)}]
        model = self._seed(client, name, rows)
        self._reload(client, name)
        result, ok = self.search(
            client,
            name,
            [_vector(1)],
            anns_field="clips[emb]",
            limit=10,
            filter="element_filter(other, $[score] IS NULL)",
            search_params={"metric_type": "L2"},
            check_task=CheckTasks.check_nothing,
        )
        assert not ok and getattr(result, "code", None) == 1100, result
        assert "same struct" in str(result), result
        hits = client.search(
            name,
            [_vector(1)],
            anns_field="clips[emb]",
            limit=10,
            filter="element_filter(clips, $[score] IS NULL)",
            search_params={"metric_type": "L2"},
        )[0]
        assert {(hit["id"], hit["offset"]) for hit in hits} == {(n, 0) for n in range(4)}
        self._check(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("metric", ["MAX_SIM", "MAX_SIM_L2", "MAX_SIM_IP", "MAX_SIM_COSINE"])
    def test_max_sim_family_rejects_nullable_vector_index(self, metric):
        """
        target: reject each floating-point MAX_SIM family member on nullable vectors
        method: remove index while released, reject unsupported metric, rebuild a valid index
        expected: explicit nullable-vector restriction and full data preserved through recovery
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(12)])
        self.release_collection(client, name)
        self.drop_index(client, name, index_name="clips[emb]")
        index = client.prepare_index_params()
        index.add_index("clips[emb]", index_type="HNSW", metric_type=metric, params={"M": 16, "efConstruction": 64})
        result, ok = self.create_index(client, name, index, check_task=CheckTasks.check_nothing)
        assert not ok and getattr(result, "code", None) == 1100, result
        assert "element" in str(result).lower() and "nullable" in str(result).lower(), result
        index = client.prepare_index_params()
        index.add_index("clips[emb]", index_type="FLAT", metric_type="L2")
        self.create_index(client, name, index)
        self.load_collection(client, name)
        self._check(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("surface", ["search", "hybrid"])
    def test_embedding_list_rejected_at_nullable_vector_search_boundary(self, surface):
        """
        target: reject embedding-list placeholders before metric/ranking processing
        method: Search and HybridSearch with embedding lists, followed by an ordinary-vector control
        expected: exact nullable embedding-list rejection and valid element search remains usable
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(12)])
        embeddings = EmbeddingList()
        embeddings.add(_vector(1))
        if surface == "search":
            result, ok = self.search(
                client,
                name,
                [embeddings],
                anns_field="clips[emb]",
                limit=5,
                search_params={"metric_type": "L2"},
                check_task=CheckTasks.check_nothing,
            )
        else:
            requests = [AnnSearchRequest([embeddings], "clips[emb]", {"metric_type": "L2"}, 5)]
            result, ok = self.hybrid_search(
                client, name, requests, RRFRanker(), limit=5, check_task=CheckTasks.check_nothing
            )
        assert not ok and getattr(result, "code", None) == 1100, result
        assert "embedding-list search is not supported for element-nullable" in str(result), result
        hits = client.search(name, [_vector(1)], anns_field="clips[emb]", limit=5, search_params={"metric_type": "L2"})
        assert hits[0]
        self._check(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    def test_nested_scalar_index_rejected(self):
        """
        target: reject scalar indexing on a nested Array child
        method: attempt INVERTED indexing and verify index inventory and full data
        expected: parameter rejection, no orphan index, successful supported query
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(12)])
        before = set(client.list_indexes(name))
        index = client.prepare_index_params()
        index.add_index("clips[tags]", index_type="INVERTED")
        result, ok = self.create_index(client, name, index, check_task=CheckTasks.check_nothing)
        assert not ok and getattr(result, "code", None) == 1100, result
        assert "indexing recursive ARRAY field clips[tags] is not supported" in str(result), result
        assert set(client.list_indexes(name)) == before
        self._check(client, name, model)

    @pytest.mark.tags(CaseLabel.L2)
    def test_partial_load_excludes_unindexed_new_vector(self):
        """
        target: require an index only when loading a newly added vector child
        method: immediate read, full-load rejection, partial-load control, index and full reload
        expected: NULL backfill is immediate and partial loading ordinary fields remains usable
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(12)])
        self.add_collection_field(client, name, "clips[newvec]", DataType.FLOAT_VECTOR, dim=8, nullable=True)
        for row in model.values():
            for element in row["clips"] or []:
                element["newvec"] = None
        self._check(client, name, model)
        self.release_collection(client, name)
        result, ok = self.load_collection(client, name, check_task=CheckTasks.check_nothing)
        assert not ok and "index" in str(result).lower(), result
        self.load_collection(client, name, load_fields=["id", "revision", "normal_vector"])
        actual = client.query(name, filter="id >= 0", output_fields=["id", "revision"], consistency_level="Strong")
        assert {row["id"]: row["revision"] for row in actual} == {pk: row["revision"] for pk, row in model.items()}
        self.release_collection(client, name)
        index = client.prepare_index_params()
        index.add_index("clips[newvec]", index_type="FLAT", metric_type="L2")
        self.create_index(client, name, index)
        self.load_collection(client, name)
        self._reload(client, name)
        self._check(client, name, model)

    @pytest.mark.tags(CaseLabel.L2)
    def test_heterogeneous_hybrid_has_independent_rrf_oracle(self):
        """
        target: fuse ordinary rows with nullable struct elements using row identities
        method: deterministic FLAT distances, independent best-element collapse and RRF calculation
        expected: every PK and fused score matches the independent model before/after reload
        """
        client = self._client()
        name = self._create(client, _core_children())
        rows = []
        for n in range(10):
            row = _record(
                n,
                [
                    {
                        "anchor": j,
                        "score": None,
                        "label": None,
                        "emb": None if j == 0 or n == 9 else [float(20 - n * 2 + j)] + [0.0] * 7,
                        "tags": [None],
                    }
                    for j in range(3)
                ],
            )
            row["normal_vector"] = [float(n + 1)] + [0.0] * 7
            rows.append(row)
        model = self._seed(client, name, rows)
        query = [0.0] * 8
        normal_order = sorted(model, key=lambda pk: sum(v * v for v in model[pk]["normal_vector"]))
        child_distances = {
            pk: min(sum(v * v for v in e["emb"]) for e in row["clips"] if e["emb"] is not None)
            for pk, row in model.items()
            if any(e["emb"] is not None for e in row["clips"])
        }
        child_order = sorted(child_distances, key=child_distances.get)
        scores = {pk: 1 / (60 + rank) for rank, pk in enumerate(normal_order, 1)}
        for rank, pk in enumerate(child_order, 1):
            scores[pk] += 1 / (60 + rank)
        for phase in range(2):
            requests = [
                AnnSearchRequest([query], field, {"metric_type": "L2"}, 100)
                for field in ("normal_vector", "clips[emb]")
            ]
            hits = client.hybrid_search(name, requests, RRFRanker(k=60), limit=10, output_fields=["clips"])[0]
            assert len(hits) == len(scores) and {h["id"] for h in hits} == set(scores)
            assert [h["distance"] for h in hits] == sorted((h["distance"] for h in hits), reverse=True)
            for hit in hits:
                assert hit["distance"] == pytest.approx(scores[hit["id"]], abs=1e-6)
            self._check(client, name, model)
            if phase == 0:
                self._reload(client, name)
