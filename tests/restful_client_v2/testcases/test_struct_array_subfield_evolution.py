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

"""Struct Array subfield evolution REST round trips."""

import copy

import pytest
from base.struct_array import StructArrayRestTestBase
from utils.constant import CaseLabel
from utils.struct_array_data import clips_struct_field, gen_vector, make_mutation_rows


class TestStructArraySubfieldEvolution(StructArrayRestTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("kind", ["scalar", "vector", "array"])
    @pytest.mark.parametrize("nullable", [False, True])
    def test_add_drop_subfield_json_round_trip(self, kind, nullable):
        """
        target: evolve scalar/vector/Array children with independent null levels through REST
        method: add scalar/vector/nested-array JSON schemas, write values and drop the child
        expected: supplied child types and NULL values round trip through REST
        """
        self.create_struct_collection(nullable)
        rows = make_mutation_rows(nullable)[:12]
        self.assert_success(self.vector_client.vector_insert({"collectionName": self.name, "data": rows}))
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        self.assert_rows(model)
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
        self.assert_success(self.collection_client.add_field(self.name, schema))
        for row in model.values():
            for element in row["clips"] or []:
                element["added"] = None
        if kind == "vector":
            self.assert_success(
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
        self.reload_collection()
        self.assert_rows(model)

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
        self.assert_success(self.vector_client.vector_upsert({"collectionName": self.name, "data": changed}))
        # The partial update omits the parent struct and must preserve all child validity.
        self.assert_success(
            self.vector_client.vector_upsert(
                {
                    "collectionName": self.name,
                    "partialUpdate": True,
                    "data": [{"id": 2, "normal_vector": gen_vector(100)}],
                }
            )
        )
        model[2]["normal_vector"] = gen_vector(100)
        self.assert_rows(model)
        # Dropping a sub-field cascades its index through the schema update.
        self.assert_success(self.collection_client.drop_field(self.name, field_name="clips[added]"))
        for row in model.values():
            for element in row["clips"] or []:
                del element["added"]
        self.assert_rows(model)

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
        self.create_struct_collection(True)
        rows = make_mutation_rows(True)[:12]
        self.assert_success(self.vector_client.vector_insert({"collectionName": self.name, "data": rows}))
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
        self.assert_rows(model)
        self.assert_success(self.vector_client.vector_insert({"collectionName": self.name, "data": good}))
        model.update({row["id"]: row for row in good})
        self.assert_rows(model)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_required_struct_is_rejected_without_row_changes(self):
        """
        target: reject a required appended Struct through REST
        method: submit a required parent then compare complete metadata and historical data
        expected: nullable-parent validation without schema or row mutation
        """
        self.create_struct_collection(True)
        rows = make_mutation_rows(True)[:12]
        self.assert_success(self.vector_client.vector_insert({"collectionName": self.name, "data": rows}))
        model = {row["id"]: row for row in rows}
        before = self.describe_schema_and_indexes()
        field = clips_struct_field(False)
        field["fieldName"] = "other"
        field["dataType"] = "ArrayOfStruct"
        response = self.collection_client.add_struct_field(self.name, field)
        assert response["code"] == 1100 and "nullable" in response["message"], response
        assert self.describe_schema_and_indexes() == before
        self.assert_rows(model)


class TestStructArrayFieldProperties(StructArrayRestTestBase):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("target", ["clips", "clips[lang]"])
    def test_alter_struct_description_preserves_schema_and_rows(self, target):
        """
        target: enforce immutable layout properties and allowed descriptions through REST
        method: reject capacity changes with metadata/data checks, then change the description
        expected: failed alteration is non-mutating and the allowed description is decoded by describe
        """
        self.create_struct_collection(True)
        rows = make_mutation_rows(True)[:12]
        self.assert_success(self.vector_client.vector_insert({"collectionName": self.name, "data": rows}))
        model = {row["id"]: row for row in rows}
        before = self.describe_schema_and_indexes()
        response = self.collection_client.alter_field_properties(self.name, target, {"max_capacity": "16"})
        assert response["code"] == 1100 and "can not be altered on struct" in response["message"], response
        assert self.describe_schema_and_indexes() == before
        self.assert_rows(model)
        self.assert_success(
            self.collection_client.alter_field_properties(self.name, target, {"field.description": "REST contract"})
        )
        desc = self.assert_success(self.collection_client.collection_describe(self.name))
        parent = next(f for f in desc["structFields"] if f["name"] == "clips")
        field = parent if target == "clips" else next(f for f in parent["fields"] if f["name"] in ("lang", target))
        assert field["description"] == "REST contract", field
        self.assert_rows(model)
