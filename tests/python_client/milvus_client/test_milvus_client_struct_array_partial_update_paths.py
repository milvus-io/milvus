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

"""Struct array partial update paths regression cases."""

import copy

import pytest
from common.common_type import CaseLabel
from common.struct_array_nested_protobuf_helpers import _field, _struct
from common.struct_array_nullable_helpers import (
    StructArrayNullableBase,
    _core_children,
    _record,
    _vector,
)
from pymilvus.client.utils import check_status


class TestMilvusClientStructArrayPartialUpdatePaths(StructArrayNullableBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("target", ["score", "emb", "tags", "element"])
    @pytest.mark.parametrize("null_value", [False, True])
    def test_path_replace_preserves_unselected_nullable_children(self, target, null_value):
        """
        target: update one logical child or element while preserving other values and validity
        method: send PATH_REPLACE by raw RPC because the current SDK drops its path member
        expected: exactly the selected position changes and the whole model survives reload
        """
        client = self._client()
        name = self._create(client, _core_children())
        clips = [
            {
                "anchor": j,
                "score": None if j % 2 else j,
                "label": None,
                "emb": None if j % 2 else _vector(j),
                "tags": None if j % 2 else [j, None],
            }
            for j in range(4)
        ]
        rows = [_record(n, clips) for n in range(6)]
        model = self._seed(client, name, rows)
        element = {
            "anchor": 9,
            "score": None if null_value else 7,
            "label": None,
            "emb": None if null_value else _vector(90),
            "tags": None if null_value else [None, 7],
        }
        handler = client._get_connection()
        request = handler._prepare_row_upsert_request(name, [{"id": 2, "clips": [element]}], partial_update=True)
        # The ordinary SDK row encoder only sets VectorArray.dim. Raw PATH_REPLACE
        # validates both dimensions before the normal insert normalization runs.
        for field in _struct(request).fields:
            if field.HasField("vectors") and field.vectors.HasField("vector_array"):
                field.vectors.dim = field.vectors.vector_array.dim
        if target != "element":
            selected = copy.deepcopy(_field(request, target))
            del _struct(request).fields[:]
            _struct(request).fields.append(selected)
        path = "[1]" if target == "element" else f"[1][{target}]"
        request.field_ops.add(field_name="clips", op=3, path=path)
        check_status(handler._stub.Upsert(request, timeout=30).status)
        if target == "element":
            model[2]["clips"][1] = element
        else:
            model[2]["clips"][1][target] = element[target]
        self._check(client, name, model)
        self._reload(client, name)
        self._check(client, name, model)
