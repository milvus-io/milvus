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

"""Struct array added subfield types regression cases."""

import copy

import pytest
from common.common_type import CaseLabel
from common.struct_array_nullable_helpers import (
    SCALAR_IDS,
    SCALARS,
    _clips,
    _core_children,
    _record,
    _value,
)
from common.struct_array_typed_vector_helpers import (
    StructArrayTypedVectorHelpersBase,
    _typed_vector,
)
from pymilvus import DataType


class TestMilvusClientStructArrayAddedSubfieldTypes(StructArrayTypedVectorHelpersBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dtype", SCALARS, ids=SCALAR_IDS)
    @pytest.mark.parametrize("nested", [False, True], ids=["scalar", "nested"])
    def test_added_scalar_types_backfill_write_and_reload(self, dtype, nested):
        """
        target: exercise every scalar and nested leaf type through AddCollectionField
        method: populated shared schema, NULL backfill, selected replacements and full reload
        expected: old NULL/empty parents remain distinct and new values retain their type
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(pk, _clips(pk)) for pk in range(32)])
        options = {"nullable": True}
        if dtype == DataType.VARCHAR:
            options["max_length"] = 32
        if nested:
            options.update(element_type=dtype, max_capacity=4, element_nullable=True)
        self.add_collection_field(client, name, "clips[added]", DataType.ARRAY if nested else dtype, **options)
        for row in model.values():
            for element in row["clips"] or []:
                element["added"] = None
        self._check(client, name, model)
        changes = []
        for pk in range(2, 32, 3):
            row = copy.deepcopy(model[pk])
            for j, element in enumerate(row["clips"] or []):
                element["added"] = [_value(dtype, pk + j), None] if nested else _value(dtype, pk + j)
            changes.append(row)
            model[pk] = row
        self.upsert(client, name, changes, partial_update=True)
        self._reload(client, name)
        self._check(client, name, model)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "dtype",
        [
            DataType.FLOAT_VECTOR,
            DataType.FLOAT16_VECTOR,
            DataType.BFLOAT16_VECTOR,
            DataType.INT8_VECTOR,
            DataType.BINARY_VECTOR,
        ],
        ids=lambda d: d.name,
    )
    def test_added_typed_vector_null_backfill_and_search(self, dtype):
        """
        target: distinguish typed vector creation from nullable typed vector addition
        method: add to sealed rows, index/reload, write one logical position then clear it
        expected: NULL backfill, exactly one coordinate after replacement, zero hits after NULL
        """
        client = self._client()
        kind = "BIN_FLAT" if dtype == DataType.BINARY_VECTOR else "HNSW" if dtype == DataType.INT8_VECTOR else "FLAT"
        metric = "HAMMING" if dtype == DataType.BINARY_VECTOR else "L2"
        name = self._typed_collection(client, dtype, kind, metric)
        model = self._seed(client, name, self._typed_rows(dtype), split=False)
        self._reload(client, name)
        self.add_collection_field(
            client, name, "clips[newvec]", dtype, nullable=True, dim=128 if dtype == DataType.BINARY_VECTOR else 32
        )
        for row in model.values():
            for element in row["clips"] or []:
                element["newvec"] = None
        self._typed_check(client, name, model, dtype)
        index, _ = self.prepare_index_params(client)
        index.add_index(
            "clips[newvec]",
            index_type=kind,
            metric_type=metric,
            params={"M": 16, "efConstruction": 200} if kind == "HNSW" else {},
        )
        self.create_index(client, name, index)
        query = _typed_vector(dtype, 500001)
        for phase in range(3):
            self._reload(client, name)
            hits, _ = self.search(
                client, name, [query], anns_field="clips[newvec]", limit=10, search_params={"metric_type": metric}
            )
            if phase == 1:
                assert len(hits[0]) == 1
                assert (hits[0][0]["id"], hits[0][0]["offset"]) == (7, 1)
                assert hits[0][0]["distance"] == pytest.approx(0, abs=1e-6)
            else:
                assert hits == [[]]
            self._typed_check(client, name, model, dtype)
            if phase < 2:
                model[7]["clips"][1]["newvec"] = query if phase == 0 else None
                self.upsert(client, name, [model[7]], partial_update=True)
