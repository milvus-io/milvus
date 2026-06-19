import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name

REST_VECTOR_DIM = 8


def gen_vector(seed: int) -> list[float]:
    return [float(seed) for _ in range(REST_VECTOR_DIM)]


def gen_profile(row_id: int) -> list[dict]:
    return [
        {"p_int": row_id * 10, "p_tag": f"profile_{row_id}_0"},
        {"p_int": row_id * 10 + 1, "p_tag": f"profile_{row_id}_1"},
    ]


def assert_profile_equal(actual, expected):
    assert actual == expected


@pytest.mark.tags(CaseLabel.L1)
class TestRestfulStructArrayNullable(TestBase):
    dim = REST_VECTOR_DIM

    def _create_struct_array_collection(self, name: str, struct_nullable: bool | None = None):
        struct_field = {
            "fieldName": "profile",
            "typeParams": {"max_capacity": "4"},
            "fields": [
                {
                    "fieldName": "p_int",
                    "dataType": "Array",
                    "elementDataType": "Int64",
                    "elementTypeParams": {"max_capacity": "4"},
                },
                {
                    "fieldName": "p_tag",
                    "dataType": "Array",
                    "elementDataType": "VarChar",
                    "elementTypeParams": {"max_capacity": "4", "max_length": "128"},
                },
            ],
        }
        if struct_nullable is not None:
            struct_field["nullable"] = struct_nullable

        payload = {
            "collectionName": name,
            "schema": {
                "autoID": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "normal_vector",
                        "dataType": "FloatVector",
                        "elementTypeParams": {"dim": str(self.dim)},
                    },
                    {
                        "fieldName": "doc_tag",
                        "dataType": "VarChar",
                        "elementTypeParams": {"max_length": "128"},
                    },
                ],
                "structFields": [struct_field],
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
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0
        self.wait_load_completed(name, timeout=60)

    def test_rest_v2_struct_array_create_query_search_output(self):
        """
        target: test REST v2 create/query/search output for Struct Array
        method: create a Struct Array collection via schema.structFields, insert empty/non-empty profile rows,
            then query/search with parent and sub-field output fields
        expected: REST conversion preserves Struct Array rows and search requery output matches source data
        """
        name = gen_collection_name()
        self.name = name
        self._create_struct_array_collection(name)

        rows = [
            {"id": 1, "normal_vector": gen_vector(1), "doc_tag": "row_1", "profile": gen_profile(1)},
            {"id": 2, "normal_vector": gen_vector(2), "doc_tag": "row_2", "profile": []},
            {"id": 3, "normal_vector": gen_vector(3), "doc_tag": "row_3", "profile": gen_profile(3)},
        ]
        source_by_id = {row["id"]: row for row in rows}

        rsp = self.vector_client.vector_insert({"collectionName": name, "data": rows})
        assert rsp["code"] == 0
        assert rsp["data"]["insertCount"] == len(rows)

        rsp = self.collection_client.flush(name)
        assert rsp["code"] == 0

        rsp = self.vector_client.vector_query(
            {
                "collectionName": name,
                "filter": "id >= 0",
                "outputFields": ["id", "doc_tag", "profile"],
                "limit": len(rows),
            }
        )
        assert rsp["code"] == 0
        assert {row["id"] for row in rsp["data"]} == set(source_by_id)
        for row in rsp["data"]:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert_profile_equal(row["profile"], expected["profile"])

        rsp = self.vector_client.vector_query(
            {
                "collectionName": name,
                "filter": "id >= 0",
                "outputFields": ["id", "profile[p_int]", "profile[p_tag]"],
                "limit": len(rows),
            }
        )
        assert rsp["code"] == 0
        assert {row["id"] for row in rsp["data"]} == set(source_by_id)
        for row in rsp["data"]:
            assert_profile_equal(row["profile"], source_by_id[row["id"]]["profile"])

        rsp = self.vector_client.vector_query(
            {
                "collectionName": name,
                "filter": "element_filter(profile, $[p_int] == 30)",
                "outputFields": ["id", "profile"],
                "limit": len(rows),
            }
        )
        assert rsp["code"] == 0
        assert [row["id"] for row in rsp["data"]] == [3]
        assert_profile_equal(rsp["data"][0]["profile"], source_by_id[3]["profile"])

        rsp = self.vector_client.vector_search(
            {
                "collectionName": name,
                "data": [gen_vector(3)],
                "annsField": "normal_vector",
                "limit": len(rows),
                "outputFields": ["id", "doc_tag", "profile"],
                "searchParams": {"metricType": "L2", "params": {}},
            }
        )
        assert rsp["code"] == 0
        assert {hit["id"] for hit in rsp["data"]} == set(source_by_id)
        assert rsp["data"][0]["id"] == 3
        for hit in rsp["data"]:
            expected = source_by_id[hit["id"]]
            assert hit["doc_tag"] == expected["doc_tag"]
            assert_profile_equal(hit["profile"], expected["profile"])

    def test_rest_v2_add_struct_array_field_rejected(self):
        """
        target: test REST v2 dynamic add rejects unsupported Struct Array field shape
        method: create a regular collection, then call /collections/fields/add with dataType=ArrayOfStruct
        expected: request is rejected instead of creating a malformed struct field
        """
        name = gen_collection_name()
        self.name = name
        payload = {
            "collectionName": name,
            "schema": {
                "autoID": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "normal_vector",
                        "dataType": "FloatVector",
                        "elementTypeParams": {"dim": str(self.dim)},
                    },
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
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0

        rsp = self.collection_client.add_field(
            name,
            {
                "fieldName": "profile",
                "dataType": "ArrayOfStruct",
                "nullable": True,
                "elementTypeParams": {"max_capacity": "4"},
            },
        )
        assert rsp["code"] != 0
        # The server rejects ArrayOfStruct via the regular field-add endpoint and
        # directs the caller to the dedicated struct_fields/add endpoint.
        assert "struct_fields/add" in rsp["message"]

        rsp = self.collection_client.collection_describe(name)
        assert rsp["code"] == 0
        assert "profile" not in {field["name"] for field in rsp["data"]["fields"]}
        assert rsp["data"]["structFields"] == []

    def test_rest_v2_add_array_struct_field_rejected(self):
        """
        target: test REST v2 dynamic add rejects Array<Struct> field shape
        method: create a regular collection, then call /collections/fields/add with dataType=Array and
            elementDataType=Struct
        expected: request is rejected instead of creating an unusable Array<Struct> field without sub-fields
        """
        name = gen_collection_name()
        self.name = name
        payload = {
            "collectionName": name,
            "schema": {
                "autoID": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "normal_vector",
                        "dataType": "FloatVector",
                        "elementTypeParams": {"dim": str(self.dim)},
                    },
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
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0

        rsp = self.collection_client.add_field(
            name,
            {
                "fieldName": "profile",
                "dataType": "Array",
                "elementDataType": "Struct",
                "nullable": True,
                "elementTypeParams": {"max_capacity": "4"},
            },
        )
        assert rsp["code"] != 0
        assert "Struct" in rsp["message"]

    @pytest.mark.xfail(
        strict=True,
        reason="REST v2 schema.structFields nullable=true is silently ignored instead of preserved in describe output",
    )
    def test_rest_v2_nullable_struct_array_schema_propagation(self):
        """
        target: test REST v2 nullable propagation for Struct Array
        method: create schema.structFields with nullable=true and inspect REST describe output
        expected: REST either preserves nullable=true on Struct Array in describe output, or rejects unsupported nullable explicitly
        """
        name = gen_collection_name()
        self.name = name
        self._create_struct_array_collection(name, struct_nullable=True)

        rsp = self.collection_client.collection_describe(name)
        assert rsp["code"] == 0
        profile = next(field for field in rsp["data"].get("structFields", []) if field["name"] == "profile")
        assert profile["nullable"] is True
