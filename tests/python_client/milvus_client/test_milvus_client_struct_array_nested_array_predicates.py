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

"""Struct array nested array predicates regression cases."""

import copy
import json

import pytest
from common.common_type import CaseLabel
from common.struct_array_nullable_helpers import (
    SCALAR_IDS,
    SCALARS,
    _child,
    _record,
    _value,
)
from common.struct_array_typed_vector_helpers import (
    StructArrayTypedVectorHelpersBase,
)
from pymilvus import DataType


class TestMilvusClientStructArrayNestedArrayPredicates(StructArrayTypedVectorHelpersBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dtype", SCALARS, ids=SCALAR_IDS)
    def test_nested_leaf_predicates_preserve_null_and_empty(self, dtype):
        """
        target: evaluate nested predicates for all supported scalar leaf representations
        method: shared scalar generator with null, empty and mixed arrays, before/after reload
        expected: contains and length results match independent input-derived identities
        """
        client = self._client()
        options = {"element_type": dtype, "max_capacity": 4, "element_nullable": True}
        if dtype == DataType.VARCHAR:
            options["max_length"] = 32
        name = self._create(
            client, {"anchor": _child(DataType.INT64, False), "tags": _child(DataType.ARRAY, **options)}
        )
        needle = _value(dtype, 2)
        shapes = [None, [], [None], [needle, None], [_value(dtype, 3)], [needle, needle]]
        rows = [
            _record(
                pk,
                None
                if pk == 0
                else []
                if pk == 1
                else [{"anchor": j, "tags": copy.deepcopy(shapes[(pk + j) % len(shapes)])} for j in range(3)],
            )
            for pk in range(20)
        ]
        model = self._seed(client, name, rows)
        for phase in range(2):
            for operation in ("array_contains", "array_contains_any", "array_contains_all"):
                argument = json.dumps(needle if operation == "array_contains" else [needle], ensure_ascii=False)
                result, _ = self.query(
                    client,
                    name,
                    filter=f"element_filter(clips, {operation}($[tags], {argument}))",
                    output_fields=["id", "clips"],
                    limit=100,
                )
                expected = {
                    (pk, j)
                    for pk, row in model.items()
                    for j, e in enumerate(row["clips"] or [])
                    if e["tags"] is not None and needle in e["tags"]
                }
                assert {(r["id"], r["offset"]) for r in result} == expected
                assert len(result) == len(expected)
            result, _ = self.query(
                client,
                name,
                filter="element_filter(clips, array_length($[tags]) == 2)",
                output_fields=["id"],
                limit=100,
            )
            assert {(r["id"], r["offset"]) for r in result} == {
                (pk, j)
                for pk, row in model.items()
                for j, e in enumerate(row["clips"] or [])
                if e["tags"] is not None and len(e["tags"]) == 2
            }
            self._check(client, name, model)
            if phase == 0:
                self._reload(client, name)
