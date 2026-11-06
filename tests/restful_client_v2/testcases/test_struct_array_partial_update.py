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

"""Struct array partial update regression cases."""

import copy

import pytest
from base.struct_array_helpers import StructArrayHelpersBase
from utils.constant import CaseLabel


class TestStructArrayPartialUpdate(StructArrayHelpersBase):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("operation", ["ARRAY_APPEND", "ARRAY_REMOVE"])
    def test_partial_upsert_rejects_array_operators_without_row_changes(self, operation):
        """
        target: reject unsupported struct partial operators through the REST fieldOps surface
        method: mixed existing/new PK batch, exact non-mutation check and legal replacement
        expected: actual struct-op restriction without partial writes; corrected upsert succeeds
        """
        self._create(True)
        rows = self._rows(True)[:12]
        self._ok(self.vector_client.vector_insert({"collectionName": self.name, "data": rows}))
        model = {row["id"]: row for row in rows}
        incoming = copy.deepcopy(rows[2:5])
        incoming[1]["id"] = 100
        for row in incoming:
            row["clips"] = [{"score": 99, "lang": None, "emb": None, "tags": [None]}]
        payload = {
            "collectionName": self.name,
            "data": incoming,
            "partialUpdate": True,
            "fieldOps": [{"fieldName": "clips", "op": operation}],
        }
        response = self.vector_client.vector_upsert(payload)
        assert response["code"] == 1100 and "not supported for struct field" in response["message"], response
        self._check(model)
        del payload["fieldOps"]
        self._ok(self.vector_client.vector_upsert(payload))
        model.update({row["id"]: row for row in incoming})
        self._check(model)
