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

"""Struct array indexed subfields regression cases."""

import pytest
from common.common_type import CaseLabel, CheckTasks
from common.struct_array_nullable_helpers import (
    StructArrayNullableBase,
    _child,
    _clips,
    _core_children,
    _record,
    _vector,
)
from pymilvus import DataType


class TestMilvusClientStructArrayIndexedSubfields(StructArrayNullableBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index_type", ["FLAT", "HNSW", "DISKANN"])
    def test_all_null_vector_index_rebuild_and_visibility(self, index_type):
        """
        target: support an index whose nullable child has no searchable elements
        method: seal all-null vectors, rebuild the index, insert a vector and null it again
        expected: zero hits, then the exact logical coordinate, then zero hits without data loss
        """
        client = self._client()
        name = self._create(client, _core_children(), child_index=index_type)
        rows = [
            _record(n, [{"anchor": j, "score": None, "label": None, "emb": None, "tags": [None]} for j in range(n % 5)])
            for n in range(1200)
        ]
        model = self._seed(client, name, rows, split=False)
        self._reload(client, name)
        for phase in range(3):
            result, _ = self.search(
                client,
                name,
                [_vector(8)],
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
            self._check(client, name, model)
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
                model[7]["clips"][1]["emb"] = _vector(8) if phase == 0 else None
                self.upsert(client, name, [model[7]])
                self._reload(client, name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_max_sim_rejection_and_original_provider_reassignment(self):
        """
        target: reject unsupported MAX_SIM and preserve offsets after dropping the first original
        method: reject an index change, drop the original anchor and compare every remaining value
        expected: the parameter error is non-mutating and the remaining originals supply correct offsets
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(100)])
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
        self._check(client, name, model)
        self.drop_collection_field(client, name, field_name="clips[anchor]")
        for row in model.values():
            for element in row["clips"] or []:
                del element["anchor"]
        self._check(client, name, model)
        self._reload(client, name)
        self._check(client, name, model)
        self._predicates(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_keeps_original_provider_and_rejects_scalar_indexes(self):
        """
        target: enforce original-provider and unsupported-index boundaries without data loss
        method: add nullable child, reject dropping all originals and reject scalar indexes
        expected: schema and complete rows survive every rejection and remain queryable
        """
        client = self._client()
        name = self._create(client, {"original": _child(DataType.INT64, False)})
        rows = [_record(n, None if n % 3 == 0 else [{"original": n}]) for n in range(12)]
        model = self._seed(client, name, rows)
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
        self._reload(client, name)
        self._check(client, name, model)
