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

"""Struct array nested validity regression cases."""

import copy

import pytest
from common.common_type import CaseLabel
from common.struct_array_nested_protobuf_helpers import _field
from common.struct_array_nullable_helpers import (
    StructArrayNullableBase,
    _clips,
    _core_children,
    _record,
    _vector,
)
from pymilvus.client.utils import check_status


class TestMilvusClientStructArrayNestedValidity(StructArrayNullableBase):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("level", ["scalar", "vector", "inner", "leaf"])
    @pytest.mark.parametrize("corruption", ["short", "long", "payload_count"])
    def test_invalid_inner_validity_reaches_proxy_and_recovers(self, level, corruption):
        """
        target: enforce logical validity and compact payload lengths at every inner level
        method: corrupt a real SDK-generated RPC, retain a valid control, compare full rows
        expected: Proxy parameter rejection without side effects and valid retry succeeds
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(20)])
        clips = [
            {"anchor": 0, "score": 1, "label": "one", "emb": _vector(4), "tags": [1, None, 3]},
            {"anchor": 1, "score": None, "label": None, "emb": None, "tags": None},
            {"anchor": 2, "score": 3, "label": "three", "emb": _vector(6), "tags": []},
        ]
        row = _record(100, clips)
        handler = client._get_connection()
        good = handler._prepare_row_insert_request(name, [row])
        bad = copy.deepcopy(good)
        if level == "scalar":
            mask = _field(bad, "score").scalars.array_data.data[0].valid_data
        elif level == "vector":
            mask = _field(bad, "emb").vectors.vector_array.data[0].valid_data
        elif level == "inner":
            mask = _field(bad, "tags").scalars.array_data.data[0].valid_data
        else:
            mask = _field(bad, "tags").scalars.array_data.data[0].array_data.data[0].valid_data
        assert list(mask) == [True, False, True], (level, list(mask))
        if corruption == "short":
            del mask[-1]
        elif corruption == "long":
            mask.append(True)
        else:
            mask[1] = True
        response = handler._stub.Insert(bad, timeout=30)
        assert response.status.code == 1100, response.status
        self._check(client, name, model)
        check_status(handler._stub.Insert(good, timeout=30).status)
        model[row["id"]] = row
        self._reload(client, name)
        self._check(client, name, model)
