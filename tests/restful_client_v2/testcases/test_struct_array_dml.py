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

"""Struct Array dml REST round trips."""

import copy
import json

import pytest
from base.struct_array import StructArrayRestTestBase
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.struct_array_data import make_mutation_rows
from utils.utils import gen_collection_name


def pk(number, kind):
    return number if kind == "Int64" else f"雪_{number:04d}"


def row(number, kind, revision=0):
    shape = (number + revision) % 5
    profile = None if shape == 0 else []
    if shape > 1:
        profile = [{"value": number * 10 + offset, "tag": f"雪_{revision}_{offset}"} for offset in range(shape)]
    return {
        "id": pk(number, kind),
        "revision": revision,
        "normal_vector": [float(number + revision)] * 8,
        "profile": profile,
    }


class TestStructArrayDml(TestBase):
    def create_struct_collection(self, kind):
        self.name = gen_collection_name()
        rsp = self.collection_client.collection_create(
            {
                "collectionName": self.name,
                "schema": {
                    "autoID": False,
                    "enableDynamicField": False,
                    "fields": [
                        {
                            "fieldName": "id",
                            "dataType": kind,
                            "isPrimary": True,
                            "elementTypeParams": {"max_length": "64"} if kind == "VarChar" else {},
                        },
                        {"fieldName": "revision", "dataType": "Int64"},
                        {"fieldName": "normal_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": "8"}},
                    ],
                    "structFields": [
                        {
                            "fieldName": "profile",
                            "nullable": True,
                            "typeParams": {"max_capacity": "8"},
                            "fields": [
                                {
                                    "fieldName": "value",
                                    "dataType": "Array",
                                    "elementDataType": "Int64",
                                    "elementTypeParams": {"max_capacity": "8"},
                                },
                                {
                                    "fieldName": "tag",
                                    "dataType": "Array",
                                    "elementDataType": "VarChar",
                                    "elementTypeParams": {"max_capacity": "8", "max_length": "64"},
                                },
                            ],
                        }
                    ],
                },
                "indexParams": [
                    {
                        "fieldName": "normal_vector",
                        "indexName": "normal_vector",
                        "metricType": "L2",
                        "indexType": "FLAT",
                    }
                ],
                "params": {"consistencyLevel": "Strong"},
            }
        )
        assert rsp["code"] == 0, rsp
        self.wait_load_completed(self.name, timeout=60)

    def write_rows(self, rows, upsert=False, partial=False):
        payload = {"collectionName": self.name, "data": rows}
        if upsert:
            payload["partialUpdate"] = partial
        rsp = (self.vector_client.vector_upsert if upsert else self.vector_client.vector_insert)(payload)
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["upsertCount" if upsert else "insertCount"] == len(rows), rsp

    def assert_projection_and_search(self, model, kind):
        base_filter = "id >= 0" if kind == "Int64" else 'id != ""'
        for fields in (
            ["id", "revision", "normal_vector", "profile"],
            ["id", "revision", "normal_vector", "profile[value]", "profile[tag]"],
        ):
            rsp = self.vector_client.vector_query(
                {
                    "collectionName": self.name,
                    "filter": base_filter,
                    "outputFields": fields,
                    "limit": len(model) + 10,
                    "consistencyLevel": "Strong",
                }
            )
            assert rsp["code"] == 0, rsp
            assert len(rsp["data"]) == len(model)
            assert {item["id"]: item for item in rsp["data"]} == model
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.name,
                "data": [[2.25] * 8],
                "annsField": "normal_vector",
                "limit": len(model),
                "outputFields": ["id", "revision", "profile"],
                "searchParams": {"metricType": "L2", "params": {}},
                "consistencyLevel": "Strong",
            }
        )
        assert rsp["code"] == 0, rsp
        assert len(rsp["data"]) == len(model) and {hit["id"] for hit in rsp["data"]} == set(model)
        scores = []
        for hit in rsp["data"]:
            expected = model[hit["id"]]
            assert hit["profile"] == expected["profile"] and hit["revision"] == expected["revision"]
            score = sum((value - 2.25) ** 2 for value in expected["normal_vector"])
            assert hit["distance"] == pytest.approx(score)
            scores.append(hit["distance"])
        assert scores == sorted(scores)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("kind", ["Int64", "VarChar"])
    @pytest.mark.parametrize("initial", ["mixed", "all_null"])
    def test_mutation_json_round_trips_struct_rows(self, kind, initial):
        """
        target: preserve nullable structs through REST mutation and result conversion
        method: exercise each JSON mutation option once and read parent/child projections
        expected: mutation counts, primary keys and decoded query/search rows match the input
        """
        self.create_struct_collection(kind)
        rows = [row(i, kind) for i in range(8)]
        if initial == "all_null":
            for item in rows:
                item["profile"] = None
        model = {item["id"]: copy.deepcopy(item) for item in rows}
        self.write_rows(rows)
        self.assert_projection_and_search(model, kind)
        revision = 1
        changes = []
        for number in (0, 1, 7, 24 + revision):
            item = row(number, kind, revision)
            item.pop("profile")
            changes.append(item)
            expected = copy.deepcopy(model.get(item["id"], {"profile": None}))
            expected.update(item)
            model[item["id"]] = expected
        self.write_rows(changes, upsert=True, partial=True)
        self.assert_projection_and_search(model, kind)
        replacements = [row(number, kind, revision) for number in range(0, 8, 3)]
        self.write_rows(replacements, upsert=True)
        model.update({item["id"]: copy.deepcopy(item) for item in replacements})
        self.assert_projection_and_search(model, kind)
        patches = [
            {"id": pk(number, kind), "profile": [] if number % 2 else row(4, kind)["profile"]}
            for number in (1, 2, 3, 4)
        ]
        self.write_rows(patches, upsert=True, partial=True)
        for patch in patches:
            model[patch["id"]].update(patch)
        removed = [pk(number, kind) for number in (5, 6)]
        rsp = self.vector_client.vector_delete({"collectionName": self.name, "filter": f"id in {json.dumps(removed)}"})
        assert rsp["code"] == 0, rsp
        for key in removed:
            del model[key]
        self.assert_projection_and_search(model, kind)
        replacements = [row(number, kind, revision) for number in (5, 6)]
        self.write_rows(replacements)
        model.update({item["id"]: copy.deepcopy(item) for item in replacements})
        self.assert_projection_and_search(model, kind)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("kind", ["Int64", "VarChar"])
    def test_struct_array_rest_all_null_partial_upsert(self, kind):
        """
        target: encode explicit all-null partial upsert independently through REST
        method: replace two existing structs and add one new key with null structs
        expected: all three decoded rows contain JSON null
        """
        self.create_struct_collection(kind)
        rows = [row(number, kind) for number in (0, 2)]
        self.write_rows(rows)
        changes = [dict(row(number, kind, 1), profile=None) for number in (0, 2, 25)]
        self.write_rows(changes, upsert=True, partial=True)
        model = {item["id"]: item for item in changes}
        self.assert_projection_and_search(model, kind)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "invalid,message",
        [
            ("missing", "missing sub-field"),
            ("null_element", "must be a JSON object"),
            ("object", "expects a JSON array"),
        ],
    )
    @pytest.mark.parametrize("position", [0, 1, 2])
    def test_struct_array_rest_invalid_batch_recovery(self, invalid, message, position):
        """
        target: reject malformed REST struct batches without partial side effects
        method: place an invalid row before/between/after valid rows, then repair and retry
        expected: code 1804, unchanged data and successful corrected JSON batch
        """
        self.create_struct_collection("Int64")
        rows = [row(number, "Int64") for number in range(8)]
        model = {item["id"]: copy.deepcopy(item) for item in rows}
        self.write_rows(rows)
        batch = [row(number, "Int64") for number in (12, 13, 14)]
        original = copy.deepcopy(batch)
        if invalid == "missing":
            del batch[position]["profile"][0]["tag"]
        elif invalid == "null_element":
            batch[position]["profile"] = [None]
        else:
            batch[position]["profile"] = {"value": 1, "tag": "invalid"}
        rsp = self.vector_client.vector_insert({"collectionName": self.name, "data": batch})
        assert rsp["code"] == 1804 and message in rsp["message"], rsp
        self.assert_projection_and_search(model, "Int64")
        self.write_rows(original)
        model.update({item["id"]: item for item in original})
        self.assert_projection_and_search(model, "Int64")


class TestStructArrayPartialUpdate(StructArrayRestTestBase):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("operation", ["ARRAY_APPEND", "ARRAY_REMOVE"])
    def test_partial_upsert_rejects_array_operators_without_row_changes(self, operation):
        """
        target: reject unsupported struct partial operators through the REST fieldOps surface
        method: mixed existing/new PK batch, exact non-mutation check and legal replacement
        expected: actual struct-op restriction without partial writes; corrected upsert succeeds
        """
        self.create_struct_collection(True)
        rows = make_mutation_rows(self.schema)[:12]
        self.assert_success(self.vector_client.vector_insert({"collectionName": self.name, "data": rows}))
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
        self.assert_rows(model)
        del payload["fieldOps"]
        self.assert_success(self.vector_client.vector_upsert(payload))
        model.update({row["id"]: row for row in incoming})
        self.assert_rows(model)
