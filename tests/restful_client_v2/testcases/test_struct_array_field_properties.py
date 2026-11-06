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

"""Struct array field properties regression cases."""

import pytest
from base.struct_array_helpers import StructArrayHelpersBase
from utils.constant import CaseLabel


class TestStructArrayFieldProperties(StructArrayHelpersBase):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("target", ["clips", "clips[lang]"])
    def test_alter_struct_description_preserves_schema_and_rows(self, target):
        """
        target: enforce immutable layout properties and allowed descriptions through REST
        method: reject capacity changes with metadata/data checks, then change the description
        expected: failed alteration is non-mutating and the allowed description is decoded by describe
        """
        self._create(True)
        rows = self._rows(True)[:12]
        self._ok(self.vector_client.vector_insert({"collectionName": self.name, "data": rows}))
        model = {row["id"]: row for row in rows}
        before = self._document_metadata()
        response = self.collection_client.alter_field_properties(self.name, target, {"max_capacity": "16"})
        assert response["code"] == 1100 and "can not be altered on struct" in response["message"], response
        assert self._document_metadata() == before
        self._check(model)
        self._ok(
            self.collection_client.alter_field_properties(self.name, target, {"field.description": "REST contract"})
        )
        desc = self._ok(self.collection_client.collection_describe(self.name))
        parent = next(f for f in desc["structFields"] if f["name"] == "clips")
        field = parent if target == "clips" else next(f for f in parent["fields"] if f["name"] in ("lang", target))
        assert field["description"] == "REST contract", field
        self._check(model)
