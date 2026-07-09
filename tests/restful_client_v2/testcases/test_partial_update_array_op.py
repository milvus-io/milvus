import time

import pytest
from base.testbase import TestBase
from pymilvus import Collection
from utils.constant import CaseLabel
from utils.utils import gen_collection_name


@pytest.mark.tags(CaseLabel.L0)
class TestPartialUpdateArrayOp(TestBase):
    def _create_array_collection(self):
        name = gen_collection_name()
        self.name = name
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "name", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    {
                        "fieldName": "tags",
                        "dataType": "Array",
                        "elementDataType": "Int64",
                        "elementTypeParams": {"max_capacity": "16"},
                    },
                    {
                        "fieldName": "labels",
                        "dataType": "Array",
                        "elementDataType": "VarChar",
                        "elementTypeParams": {"max_capacity": "16", "max_length": "64"},
                    },
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": "4"}},
                ]
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_index", "metricType": "L2"}],
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        return name

    def _insert_base_rows(self, name):
        rows = [
            {
                "id": 0,
                "name": "row_0",
                "tags": [1, 2],
                "labels": ["a", "b", "a"],
                "vector": [0.1, 0.2, 0.3, 0.4],
            },
            {
                "id": 1,
                "name": "row_1",
                "tags": [10, 20, 10, 30],
                "labels": ["x", "y"],
                "vector": [0.2, 0.3, 0.4, 0.5],
            },
        ]
        rsp = self.vector_client.vector_insert({"collectionName": name, "data": rows})
        assert rsp["code"] == 0, rsp
        Collection(name).flush()
        time.sleep(1)

    def _query_rows(self, name):
        rsp = self.vector_client.vector_query(
            {
                "collectionName": name,
                "filter": "id >= 0",
                "outputFields": ["id", "name", "tags", "labels"],
            }
        )
        assert rsp["code"] == 0, rsp
        rows = {row["id"]: row for row in rsp["data"]}
        for row in rows.values():
            row["tags"] = self._array_data(row["tags"])
            row["labels"] = self._array_data(row["labels"])
        return rows

    @staticmethod
    def _array_data(value):
        if not isinstance(value, dict):
            return value

        data = value.get("Data", value.get("data"))
        if not isinstance(data, dict):
            return value

        for field_name in ("LongData", "StringData", "BoolData", "FloatData", "DoubleData", "IntData"):
            typed_data = data.get(field_name)
            if isinstance(typed_data, dict) and "data" in typed_data:
                return typed_data["data"]
        return value

    def test_partial_update_array_append(self):
        """
        target: verify REST upsert fieldOps ARRAY_APPEND appends array payloads
        method: insert rows, upsert only tags with ARRAY_APPEND, then query rows
        expected: tags are appended and non-updated fields are preserved
        """
        name = self._create_array_collection()
        self._insert_base_rows(name)

        payload = {
            "collectionName": name,
            "data": [{"id": 0, "tags": [3, 4]}, {"id": 1, "tags": [40]}],
            "partialUpdate": True,
            "fieldOps": [{"fieldName": "tags", "op": "ARRAY_APPEND"}],
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp["code"] == 0, rsp

        rows = self._query_rows(name)
        assert rows[0]["tags"] == [1, 2, 3, 4]
        assert rows[1]["tags"] == [10, 20, 10, 30, 40]
        assert rows[0]["name"] == "row_0"

    def test_partial_update_array_remove(self):
        """
        target: verify REST upsert fieldOps ARRAY_REMOVE removes matching array elements
        method: insert repeated array values, remove one value through fieldOps
        expected: all matching values are removed from the target array field
        """
        name = self._create_array_collection()
        self._insert_base_rows(name)

        payload = {
            "collectionName": name,
            "data": [{"id": 1, "tags": [10]}],
            "partialUpdate": True,
            "fieldOps": [{"fieldName": "tags", "op": "ARRAY_REMOVE"}],
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp["code"] == 0, rsp

        rows = self._query_rows(name)
        assert rows[1]["tags"] == [20, 30]
        assert rows[1]["labels"] == ["x", "y"]

    def test_partial_update_array_multiple_field_ops(self):
        """
        target: verify REST upsert accepts multiple array fieldOps in one request
        method: append Int64 array field and remove VarChar array field together
        expected: each field uses its own op and unchanged fields are preserved
        """
        name = self._create_array_collection()
        self._insert_base_rows(name)

        payload = {
            "collectionName": name,
            "data": [{"id": 0, "tags": [3], "labels": ["a"]}],
            "partialUpdate": True,
            "fieldOps": [
                {"fieldName": "tags", "op": "ARRAY_APPEND"},
                {"fieldName": "labels", "op": "ARRAY_REMOVE"},
            ],
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp["code"] == 0, rsp

        rows = self._query_rows(name)
        assert rows[0]["tags"] == [1, 2, 3]
        assert rows[0]["labels"] == ["b"]
        assert rows[0]["name"] == "row_0"

    def test_partial_update_array_op_rejects_non_array_field(self):
        """
        target: verify REST rejects ARRAY_APPEND on a non-Array field
        method: send fieldOps ARRAY_APPEND for VarChar field name
        expected: request fails with a clear non-Array field validation error
        """
        name = self._create_array_collection()
        self._insert_base_rows(name)

        payload = {
            "collectionName": name,
            "data": [{"id": 0, "name": "bad_append"}],
            "partialUpdate": True,
            "fieldOps": [{"fieldName": "name", "op": "ARRAY_APPEND"}],
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp["code"] != 0, rsp
        assert 'op ARRAY_APPEND requires Array field, but field "name" is VarChar' in rsp["message"]
