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

"""Struct array partial update rejection regression cases."""

import copy

import pytest
from common.common_type import CaseLabel
from common.struct_array_nullable_helpers import (
    StructArrayNullableBase,
    _clips,
    _core_children,
    _record,
)
from pymilvus.client.utils import check_status


class TestMilvusClientStructArrayPartialUpdateRejection(StructArrayNullableBase):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("op", [1, 2], ids=["append", "remove"])
    @pytest.mark.parametrize("target", ["clips", "clips[score]", "clips[tags]"])
    def test_unsupported_struct_array_ops_are_non_mutating(self, op, target):
        """
        target: reject ARRAY_APPEND/REMOVE at the actual struct operation boundary
        method: send SDK-encoded existing/new PK batches through raw Upsert RPC
        expected: explicit struct-operation rejection, unchanged rows and valid replacement
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(12)])
        before = client.describe_collection(name)
        rows = [_record(n, _clips(n, 1), revision=1) for n in (2, 100, 3)]
        handler = client._get_connection()
        request = handler._prepare_row_upsert_request(name, rows, partial_update=True)
        request.field_ops.add(field_name=target, op=op)
        status = handler._stub.Upsert(request, timeout=30).status
        assert status.code == 1100, status
        assert "not supported for struct field" in status.reason, status
        assert client.describe_collection(name) == before
        self._check(client, name, model)
        del request.field_ops[:]
        check_status(handler._stub.Upsert(request, timeout=30).status)
        model.update({row["id"]: row for row in rows})
        self._reload(client, name)
        self._check(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("position", [0, 1, 2])
    def test_invalid_partial_batch_preserves_existing_and_new_rows(self, position):
        """
        target: reject malformed partial updates atomically at Proxy
        method: corrupt required child payload in a batch containing new and existing PKs
        expected: no acknowledged sibling change; repaired request persists all rows
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(12)])
        rows = [
            _record(n, [{"anchor": n, "score": None, "label": None, "emb": None, "tags": [None]}], revision=1)
            for n in (2, 100, 3)
        ]
        handler = client._get_connection()
        good = handler._prepare_row_upsert_request(name, rows, partial_update=True)
        bad = copy.deepcopy(good)
        struct = next(f for f in bad.fields_data if f.field_name == "clips").struct_arrays
        anchor = next(f for f in struct.fields if f.field_name in ("anchor", "clips[anchor]"))
        del anchor.scalars.array_data.data[position].long_data.data[:]
        status = handler._stub.Upsert(bad, timeout=30).status
        assert status.code == 1100, status
        self._check(client, name, model)
        check_status(handler._stub.Upsert(good, timeout=30).status)
        model.update({row["id"]: row for row in rows})
        self._reload(client, name)
        self._check(client, name, model)
