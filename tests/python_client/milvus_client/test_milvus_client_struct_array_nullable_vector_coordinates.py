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

"""Struct array nullable vector coordinates regression cases."""

import numpy as np
import pytest
from common.common_type import CaseLabel
from common.struct_array_nullable_helpers import (
    StructArrayNullableBase,
    _clips,
    _core_children,
    _equal,
    _record,
    _vector,
)


class TestMilvusClientStructArrayNullableVectorCoordinates(StructArrayNullableBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric", ["L2", "IP", "COSINE"])
    @pytest.mark.parametrize("index_type", ["FLAT", "HNSW", "DISKANN"])
    def test_nullable_vector_search_keeps_logical_coordinates(self, metric, index_type):
        """
        target: exclude null vectors without renumbering logical struct element positions
        method: independent flattened-vector oracle before/after indexing and mutations
        expected: legal unique coordinates, exact distances and exact or high-recall top-k
        """
        client = self._client()
        name = self._create(client, _core_children(), child_index=index_type, metric=metric)
        rows = [_record(n, _clips(n)) for n in range(1200)]
        # A separate all-null vector population still owns logical element slots.
        for row in rows[:100]:
            for element in row["clips"] or []:
                element["emb"] = None
        model = self._seed(client, name, rows)
        for phase in range(3):
            for query in [_vector(99999), _vector(77777)]:
                for filtered in (False, True):
                    scored = {}
                    for pk, row in model.items():
                        for offset, element in enumerate(row["clips"] or []):
                            if element["emb"] is None or (filtered and element["score"] is not None):
                                continue
                            a, b = np.asarray(query, dtype=np.float64), np.asarray(element["emb"], dtype=np.float64)
                            score = float(np.sum((a - b) ** 2)) if metric == "L2" else float(np.dot(a, b))
                            if metric == "COSINE":
                                score /= float(np.linalg.norm(a) * np.linalg.norm(b))
                            scored[(pk, offset)] = score
                    result, _ = self.search(
                        client,
                        name,
                        [query],
                        anns_field="clips[emb]",
                        limit=20,
                        filter="element_filter(clips, $[score] IS NULL)" if filtered else "",
                        search_params={"metric_type": metric, "params": {"ef": 200, "search_list": 200}},
                        output_fields=["clips"],
                    )
                    hits = result[0]
                    assert len(hits) == min(20, len(scored))
                    coordinates = [(h["id"], h["offset"]) for h in hits]
                    assert len(set(coordinates)) == len(coordinates)
                    assert set(coordinates).issubset(scored)
                    for hit, coordinate in zip(hits, coordinates):
                        assert hit["distance"] == pytest.approx(scored[coordinate], abs=2e-5, rel=2e-5)
                        _equal(hit["entity"]["clips"], model[hit["id"]]["clips"])
                    ranking = sorted(scored, key=scored.get, reverse=metric != "L2")[: len(hits)]
                    if index_type == "FLAT":
                        assert set(coordinates) == set(ranking)
                    else:
                        assert len(set(coordinates) & set(ranking)) / len(ranking) >= 0.95
                    distances = [h["distance"] for h in hits]
                    assert distances == sorted(distances, reverse=metric != "L2")
            self._check(client, name, model)
            if phase == 0:
                self._reload(client, name)
                assert self.wait_for_index_ready(client, name, index_name="clips[emb]", timeout=300)
            elif phase == 1:
                deleted = list(range(0, 1200, 13))
                self.delete(client, name, ids=deleted)
                for pk in deleted:
                    del model[pk]
                changes = [_record(n, _clips(n, 5), revision=5) for n in range(1, 1200, 11)]
                self.upsert(client, name, changes)
                model.update({r["id"]: r for r in changes})
                self._reload(client, name)
