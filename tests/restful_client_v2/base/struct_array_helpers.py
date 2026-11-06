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

import time

from base.testbase import TestBase
from utils.struct_array_data import clips_struct_field, gen_vector
from utils.utils import gen_collection_name


class StructArrayHelpersBase(TestBase):
    connect_with_pymilvus = False

    def _document_metadata(self):
        result = self._ok(self.collection_client.collection_describe(self.name))
        # Index order is not part of DescribeCollection's contract; field order is.
        result["indexes"] = sorted(result.get("indexes", []), key=lambda item: item["indexName"])
        return result

    def _ok(self, response):
        assert response["code"] == 0, response
        return response.get("data")

    def _create(self, nullable):
        self.name = gen_collection_name("struct_enhance_rest")
        self._ok(
            self.collection_client.collection_create(
                {
                    "collectionName": self.name,
                    "schema": {
                        "autoID": False,
                        "enableDynamicField": False,
                        "fields": [
                            {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                            {
                                "fieldName": "normal_vector",
                                "dataType": "FloatVector",
                                "elementTypeParams": {"dim": "8"},
                            },
                        ],
                        "structFields": [clips_struct_field(nullable)],
                    },
                    "indexParams": [
                        {"fieldName": field, "indexName": index, "metricType": "L2", "indexType": "FLAT", "params": {}}
                        for field, index in [("normal_vector", "normal"), ("clips[emb]", "element")]
                    ],
                    "params": {"consistencyLevel": "Strong"},
                }
            )
        )
        self.wait_load_completed(self.name, timeout=90)

    def _check(self, model):
        rows = self._ok(
            self.vector_client.vector_query(
                {
                    "collectionName": self.name,
                    "filter": "id >= 0",
                    "limit": 1000,
                    "outputFields": ["id", "clips", "normal_vector"],
                }
            )
        )
        assert len(rows) == len(model)
        assert {row["id"]: row for row in rows} == model

    def _reload_after_schema_change(self):
        # Flush has a low default quota. Retry only explicit limiter rejection;
        # all other errors and a sustained rejection still fail this test.
        deadline = time.monotonic() + 45
        while True:
            response = self.collection_client.flush(self.name)
            if response["code"] != 1807 or time.monotonic() >= deadline:
                self._ok(response)
                break
            time.sleep(2)
        self._ok(self.collection_client.collection_release(collection_name=self.name))
        self._ok(self.collection_client.collection_load(collection_name=self.name))
        self.wait_load_completed(self.name, timeout=90)

    def _rows(self, nullable):
        return [
            {
                "id": n,
                "normal_vector": gen_vector(n),
                "clips": None
                if nullable and n % 7 == 0
                else [
                    {
                        "score": n + j,
                        "lang": None if j % 2 else "雪",
                        "emb": None if (n + j) % 3 else gen_vector(n + j),
                        "tags": [None, [], [None], ["a", None, "雪"]][(n + j) % 4],
                    }
                    for j in range(n % 5)
                ],
            }
            for n in range(48)
        ]
