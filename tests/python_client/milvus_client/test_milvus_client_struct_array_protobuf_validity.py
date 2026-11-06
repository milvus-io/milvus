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

"""Struct array protobuf validity regression cases."""

import copy

import pytest
from common.common_type import CaseLabel
from common.struct_array_protobuf_helpers import _encode_masks, _fields, _set_masks
from common.struct_array_row_model import StructArrayRowModelBase, _row
from utils.util_log import test_log as log


class TestMilvusClientStructArrayProtobufValidity(StructArrayRowModelBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("location", ["legacy", "current", "dual"])
    @pytest.mark.parametrize("profile", ["all_null", "mixed"])
    def test_struct_array_wire_validity_locations_are_equivalent(self, location, profile):
        """
        target: accept legacy, field-specific and matching dual row masks recursively
        method: prepare protobuf input, explicitly rewrite masks, submit directly to Proxy and reload
        expected: identical complete rows and three-valued predicates for all supported encodings
        """
        client = self._client()
        name = self._create_contract_collection(client)
        rows = [_row(i) for i in range(21)]
        if profile == "all_null":
            for row in rows:
                row["clips"] = None
                row["note"] = None
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        handler = client._get_connection()
        template = handler._prepare_row_insert_request(name, [_row(4)])
        request = _encode_masks(handler._prepare_row_insert_request(name, rows), location, template)
        response = handler._stub.Insert(request, timeout=30)
        assert response.status.code == 0, response.status
        assert response.insert_cnt == len(rows)
        log.info(f"WIRE_ACCEPT location={location} profile={profile} rows={response.insert_cnt}")
        self._assert_model(client, name, model)
        self._reload(client, name)
        self._assert_model(client, name, model)
        self._assert_predicates(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("target", ["note", "clips[value]", "clips[emb]"])
    @pytest.mark.parametrize("malformation", ["conflict", "short", "long"])
    def test_struct_array_wire_invalid_mask_has_no_side_effects(self, target, malformation):
        """
        target: reject conflicting or misaligned masks at the actual Proxy boundary
        method: corrupt a direct protobuf batch, compare full retained data, retry original payload
        expected: parameter rejection, no valid sibling writes, then complete successful recovery
        """
        client = self._client()
        name = self._create_contract_collection(client)
        existing = [_row(i) for i in range(14)]
        model = {row["id"]: copy.deepcopy(row) for row in existing}
        self.insert(client, name, existing)
        self._reload(client, name)
        rows = [_row(100 + i) for i in range(7)]
        handler = client._get_connection()
        good = _encode_masks(handler._prepare_row_insert_request(name, rows), "dual")
        bad = copy.deepcopy(good)
        field = dict(_fields(bad))[target]
        mask = list(field.valid_data)
        assert len(mask) == len(rows)
        if malformation == "conflict":
            other = mask[:]
            other[len(other) // 2] = not other[len(other) // 2]
            _set_masks(field, mask, other)
        else:
            malformed = mask[:-1] if malformation == "short" else mask + [True]
            _set_masks(field, malformed, malformed)
        response = handler._stub.Insert(bad, timeout=30)
        log.info(f"WIRE_REJECT target={target} malformed={malformation} status={response.status}")
        assert response.status.code == 1100, response.status
        assert response.status.reason
        if malformation == "conflict":
            assert "different legacy and field-specific valid_data" in response.status.reason
        self._assert_model(client, name, model)
        response = handler._stub.Insert(good, timeout=30)
        assert response.status.code == 0, response.status
        assert response.insert_cnt == len(rows)
        model.update({row["id"]: row for row in rows})
        self._reload(client, name)
        self._assert_model(client, name, model)
