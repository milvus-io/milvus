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

"""Shared setup, data models and assertions for struct enhancement contracts."""

import copy
import time

import numpy as np
from base.testbase import TestBase
from utils.struct_array_data import clips_struct_field
from utils.utils import gen_collection_name


class StructArrayRestTestBase(TestBase):
    connect_with_pymilvus = False

    def describe_schema_and_indexes(self):
        result = self.assert_success(self.collection_client.collection_describe(self.name))
        # Index order is not part of DescribeCollection's contract; field order is.
        result["indexes"] = sorted(result.get("indexes", []), key=lambda item: item["indexName"])
        return result

    def assert_success(self, response):
        assert response["code"] == 0, response
        return response.get("data")

    def create_struct_collection(self, nullable, dim=8, capacity=4, extra_fields=()):
        self.name = gen_collection_name("struct_enhance_rest")
        schema = {
            "autoID": False,
            "enableDynamicField": False,
            "fields": [
                {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                {"fieldName": "normal_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                *extra_fields,
            ],
            "structFields": [clips_struct_field(nullable)],
        }
        schema["structFields"][0]["typeParams"]["max_capacity"] = str(capacity)
        self.schema = schema
        self.assert_success(
            self.collection_client.collection_create(
                {
                    "collectionName": self.name,
                    "schema": schema,
                    "indexParams": [
                        {"fieldName": field, "indexName": index, "metricType": "L2", "indexType": "FLAT", "params": {}}
                        for field, index in [("normal_vector", "normal"), ("clips[emb]", "element")]
                    ],
                    "params": {"consistencyLevel": "Strong"},
                }
            )
        )
        self.wait_load_completed(self.name, timeout=90)

    def assert_rows(self, model):
        rows = self.assert_success(
            self.vector_client.vector_query(
                {
                    "collectionName": self.name,
                    "filter": "id >= 0",
                    "limit": 1000,
                    "outputFields": ["*"],
                }
            )
        )
        assert len(rows) == len(model)
        expected = copy.deepcopy(model)
        # REST emits shortest float32 decimals; compare the exact stored values.
        for row in [*rows, *expected.values()]:
            for field in self.schema["fields"]:
                if field["dataType"] == "FloatVector" and row.get(field["fieldName"]) is not None:
                    row[field["fieldName"]] = np.asarray(row[field["fieldName"]], dtype=np.float32).tolist()
            for struct in self.schema["structFields"]:
                for element in row.get(struct["fieldName"]) or []:
                    for field in struct["fields"]:
                        if (
                            field.get("elementDataType") == "FloatVector"
                            and element.get(field["fieldName"]) is not None
                        ):
                            element[field["fieldName"]] = np.asarray(
                                element[field["fieldName"]], dtype=np.float32
                            ).tolist()
        assert {row["id"]: row for row in rows} == expected

    def reload_collection(self):
        # Flush has a low default quota. Retry only explicit limiter rejection;
        # all other errors and a sustained rejection still fail this test.
        deadline = time.monotonic() + 45
        while True:
            response = self.collection_client.flush(self.name)
            if response["code"] != 1807 or time.monotonic() >= deadline:
                self.assert_success(response)
                break
            time.sleep(2)
        self.assert_success(self.collection_client.collection_release(collection_name=self.name))
        self.assert_success(self.collection_client.collection_load(collection_name=self.name))
        self.wait_load_completed(self.name, timeout=90)
