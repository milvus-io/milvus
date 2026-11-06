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

"""Struct array nested array children regression cases."""

import copy

import pytest
from common.common_type import CaseLabel
from common.struct_array_nullable_helpers import (
    SCALAR_IDS,
    SCALARS,
    StructArrayNullableBase,
    _child,
    _clips,
    _core_children,
    _record,
    _value,
    _vector,
)
from pymilvus import CollectionSchema, DataType


class TestMilvusClientStructArrayNestedArrayChildren(StructArrayNullableBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dtype", SCALARS, ids=SCALAR_IDS)
    @pytest.mark.parametrize(
        "inner_nullable,leaf_nullable", [(False, False), (False, True), (True, False), (True, True)]
    )
    def test_array_child_independent_null_levels(self, dtype, inner_nullable, leaf_nullable):
        """
        target: preserve inner-array and leaf validity independently for every scalar leaf type
        method: write empty, boundary-length, null-inner and null-leaf values; replace and reload
        expected: exact nested values, independent capacities and no null-to-zero coercion
        """
        client = self._client()
        options = dict(element_type=dtype, max_capacity=4, element_nullable=leaf_nullable)
        if dtype == DataType.VARCHAR:
            options["max_length"] = 32
        name = self._create(
            client, {"anchor": _child(DataType.INT64, False), "tags": _child(DataType.ARRAY, inner_nullable, **options)}
        )
        rows = []
        for n in range(64):
            clips = []
            for j in range(n % 7):
                tags = [
                    None if leaf_nullable and (n + j + k) % 3 == 0 else _value(dtype, n + j + k)
                    for k in range((n + j) % 5)
                ]
                if inner_nullable and (n + j) % 4 == 0:
                    tags = None
                clips.append({"anchor": j, "tags": tags})
            rows.append(_record(n, None if n % 13 == 0 else clips))
        model = self._seed(client, name, rows)
        self._check(client, name, model)
        for n in range(0, 64, 5):
            patch = {"id": n, "clips": [{"anchor": 0, "tags": []}, {"anchor": 1, "tags": [_value(dtype, n)]}]}
            self.upsert(client, name, [patch], partial_update=True)
            model[n].update(copy.deepcopy(patch))
        desc, _ = self.describe_collection(client, name)
        restored = CollectionSchema.construct_from_dict(desc).struct_fields[0]
        tags = next(field for field in restored.fields if field.name == "tags")
        assert restored.max_capacity == 8 and restored.nullable
        assert tags.nullable == inner_nullable and tags.element_nullable == leaf_nullable
        assert tags.element_type == dtype and tags.params["max_capacity"] == 4
        if not inner_nullable:
            self._reject_insert(client, name, [_record(900, [{"anchor": 0, "tags": None}])], model)
        if not leaf_nullable:
            self._reject_insert(client, name, [_record(901, [{"anchor": 0, "tags": [None]}])], model)
        self._reload(client, name)
        self._check(client, name, model)
        actual, _ = self.query(
            client, name, filter="MATCH_ANY(clips, array_length($[tags]) == 0)", output_fields=["id"], limit=1000
        )
        assert {r["id"] for r in actual} == {
            pk for pk, r in model.items() if any(e["tags"] == [] for e in r["clips"] or [])
        }

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "malformation",
        [
            "required_null",
            "required_missing",
            "null_struct_element",
            "outer_capacity",
            "inner_capacity",
            "leaf_type",
            "vector_dimension",
            "string_length",
        ],
    )
    @pytest.mark.parametrize("bad_position", [0, 1, 2], ids=["first", "middle", "last"])
    def test_invalid_nested_batch_preserves_data_and_recovers(self, malformation, bad_position):
        """
        target: reject invalid child payloads without admitting valid sibling rows
        method: place one malformed row at each batch position, compare model, repair and retry
        expected: parameter rejection, no row changes, complete successful recovery
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(24)])
        clips = [{"anchor": 0, "score": 1, "label": "ok", "emb": _vector(3), "tags": [1, None]}]
        good = [_record(300 + i, clips) for i in range(3)]
        bad = copy.deepcopy(good)
        elements = bad[bad_position]["clips"]
        if malformation == "required_null":
            elements[0]["anchor"] = None
        elif malformation == "required_missing":
            del elements[0]["anchor"]
        elif malformation == "null_struct_element":
            elements[0] = None
        elif malformation == "outer_capacity":
            bad[bad_position]["clips"] = elements * 9
        elif malformation == "inner_capacity":
            elements[0]["tags"] = [None] * 5
        elif malformation == "leaf_type":
            elements[0]["tags"] = ["wrong"]
        elif malformation == "vector_dimension":
            elements[0]["emb"] = [1.0] * 7
        else:
            elements[0]["label"] = "雪" * 33
        self._reject_insert(client, name, bad, model)
        self.insert(client, name, good)
        model.update({r["id"]: r for r in good})
        self._reload(client, name)
        self._check(client, name, model)
