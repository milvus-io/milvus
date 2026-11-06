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

"""Struct array subfield evolution regression cases."""

import copy

import pytest
from base.struct_array_helpers import StructArrayHelpersBase
from utils.constant import CaseLabel
from utils.struct_array_data import clips_struct_field, gen_vector


class TestStructArraySubfieldEvolution(StructArrayHelpersBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("kind", ["scalar", "vector", "array"])
    @pytest.mark.parametrize("nullable", [False, True])
    def test_add_drop_subfield_json_round_trip(self, kind, nullable):
        """
        target: evolve scalar/vector/Array children with independent null levels through REST
        method: add scalar/vector/nested-array JSON schemas, write values and drop the child
        expected: supplied child types and NULL values round trip through REST
        """
        self._create(nullable)
        rows = self._rows(nullable)[:12]
        self._ok(self.vector_client.vector_insert({"collectionName": self.name, "data": rows}))
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        self._check(model)
        schema = {
            "fieldName": "clips[added]",
            "elementNullable": True,
            **{
                "scalar": {
                    "dataType": "Array",
                    "elementDataType": "VarChar",
                    "elementTypeParams": {"max_length": "32"},
                },
                "vector": {
                    "dataType": "ArrayOfVector",
                    "elementDataType": "FloatVector",
                    "elementTypeParams": {"dim": "8"},
                },
                "array": {
                    "dataType": "Array",
                    "elementDataType": "Array",
                    "typeSchema": {
                        "arrayElement": {
                            "typeParams": {"max_capacity": "3"},
                            "arrayElement": {"leafType": "Int64", "nullable": True},
                        },
                    },
                },
            }[kind],
        }
        self._ok(self.collection_client.add_field(self.name, schema))
        for row in model.values():
            for element in row["clips"] or []:
                element["added"] = None
        if kind == "vector":
            self._ok(
                self.index_client.index_create(
                    {
                        "collectionName": self.name,
                        "indexParams": [
                            {
                                "fieldName": "clips[added]",
                                "indexName": "added",
                                "indexType": "FLAT",
                                "metricType": "L2",
                                "params": {},
                            }
                        ],
                    }
                )
            )
        self._reload_after_schema_change()
        self._check(model)

        changed = []
        for n in [2, 3, 7, 49]:
            value = {"scalar": '雪"a', "vector": gen_vector(99), "array": [None, 99, -99]}[kind]
            clips = [
                {"score": n, "lang": None, "emb": None, "tags": [], "added": value},
                {"score": -n, "lang": "λ", "emb": gen_vector(3), "tags": None, "added": None},
            ]
            row = {"id": n, "normal_vector": gen_vector(n), "clips": clips}
            changed.append(row)
            model[n] = copy.deepcopy(row)
        self._ok(self.vector_client.vector_upsert({"collectionName": self.name, "data": changed}))
        # The partial update omits the parent struct and must preserve all child validity.
        self._ok(
            self.vector_client.vector_upsert(
                {
                    "collectionName": self.name,
                    "partialUpdate": True,
                    "data": [{"id": 2, "normal_vector": gen_vector(100)}],
                }
            )
        )
        model[2]["normal_vector"] = gen_vector(100)
        self._check(model)
        # Dropping a sub-field cascades its index through the schema update.
        self._ok(self.collection_client.drop_field(self.name, field_name="clips[added]"))
        for row in model.values():
            for element in row["clips"] or []:
                del element["added"]
        self._check(model)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "bad", ["required_null", "required_missing", "null_element", "inner_capacity", "leaf_type", "vector_dimension"]
    )
    def test_invalid_nested_batch_preserves_existing_rows(self, bad):
        """
        target: reject malformed nested JSON without partially ingesting the batch
        method: put a malformed row between valid rows, compare state, then retry repaired rows
        expected: a parameter error, unchanged full data and successful corrected retry
        """
        self._create(True)
        rows = self._rows(True)[:12]
        self._ok(self.vector_client.vector_insert({"collectionName": self.name, "data": rows}))
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        good = [
            {
                "id": n,
                "normal_vector": gen_vector(n),
                "clips": [{"score": n, "lang": None, "emb": None, "tags": ["a", None]}],
            }
            for n in range(100, 103)
        ]
        malformed = copy.deepcopy(good)
        element = malformed[1]["clips"][0]
        if bad == "required_null":
            element["score"] = None
        elif bad == "required_missing":
            del element["score"]
        elif bad == "null_element":
            malformed[1]["clips"] = [None]
        elif bad == "inner_capacity":
            element["tags"] = [None] * 4
        elif bad == "leaf_type":
            element["tags"] = [42]
        else:
            element["emb"] = [1.0]
        response = self.vector_client.vector_insert({"collectionName": self.name, "data": malformed})
        # REST JSON decoding and Proxy validation have separate parameter-error codes.
        assert response["code"] in (1100, 1804), response
        self._check(model)
        self._ok(self.vector_client.vector_insert({"collectionName": self.name, "data": good}))
        model.update({row["id"]: row for row in good})
        self._check(model)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_required_struct_is_rejected_without_row_changes(self):
        """
        target: reject a required appended Struct through REST
        method: submit a required parent then compare complete metadata and historical data
        expected: nullable-parent validation without schema or row mutation
        """
        self._create(True)
        rows = self._rows(True)[:12]
        self._ok(self.vector_client.vector_insert({"collectionName": self.name, "data": rows}))
        model = {row["id"]: row for row in rows}
        before = self._document_metadata()
        field = clips_struct_field(False)
        field["fieldName"] = "other"
        field["dataType"] = "ArrayOfStruct"
        response = self.collection_client.add_struct_field(self.name, field)
        assert response["code"] == 1100 and "nullable" in response["message"], response
        assert self._document_metadata() == before
        self._check(model)
