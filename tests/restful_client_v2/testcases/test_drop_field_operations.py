import time

import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name


@pytest.mark.tags(CaseLabel.L1)
class TestCollectionDropField(TestBase):
    """Supplemental REST v2 parameter coverage for collection field drops."""

    def _create_collection(self):
        collection_name = gen_collection_name(prefix=self.__class__.__name__)
        rsp = self.collection_client.collection_create(
            {
                "collectionName": collection_name,
                "schema": {
                    "autoId": False,
                    "enableDynamicField": False,
                    "fields": [
                        {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                        {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": "4"}},
                        {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                    ],
                },
                "indexParams": [
                    {
                        "fieldName": "dense",
                        "indexName": "dense_idx",
                        "indexType": "AUTOINDEX",
                        "metricType": "L2",
                    }
                ],
            }
        )
        assert rsp["code"] == 0, rsp
        return collection_name

    def _wait_index_ready(self, collection_name, index_name, timeout=30):
        t0 = time.time()
        while time.time() - t0 < timeout:
            rsp = self.index_client.index_describe(collection_name=collection_name, index_name=index_name)
            assert rsp["code"] == 0, rsp
            assert len(rsp["data"]) == 1, rsp
            index = rsp["data"][0]
            if index["indexState"] == "Finished":
                return index
            time.sleep(1)
        raise AssertionError(f"index {index_name} of collection {collection_name} not ready after {timeout}s")

    def _describe_index_state(self, collection_name, index_name):
        index = self._wait_index_ready(collection_name, index_name)
        assert index["failReason"] == "", index
        index_params = [(param["key"], param["value"]) for param in index["indexParams"]]
        index_param_keys = [key for key, _ in index_params]
        assert len(index_param_keys) == len(set(index_param_keys)), index
        return {
            "fieldName": index["fieldName"],
            "indexName": index["indexName"],
            "metricType": index["metricType"],
            "indexType": index["indexType"],
            "indexState": index["indexState"],
            "indexParams": sorted(index_params),
        }

    def _assert_collection_state(self, collection_name, *, expected_fields, expected_indexes):
        desc = self.collection_client.collection_describe(collection_name)
        assert desc["code"] == 0, desc
        raw_fields = desc["data"]["fields"]
        field_names = [field["name"] for field in raw_fields]
        field_ids = [int(field["id"]) for field in raw_fields]
        assert field_names == list(expected_fields), desc
        assert len(field_names) == len(set(field_names)), desc
        assert all(field_id > 0 for field_id in field_ids), desc
        assert len(field_ids) == len(set(field_ids)), desc
        fields = {field["name"]: field for field in raw_fields}

        raw_properties = desc["data"].get("properties", [])
        property_keys = [prop["key"] for prop in raw_properties]
        assert len(property_keys) == len(set(property_keys)), desc
        properties = {prop["key"]: prop["value"] for prop in raw_properties}
        assert "max_field_id" in properties, desc
        assert int(properties["max_field_id"]) >= max(field_ids), desc

        raw_functions = desc["data"].get("functions", [])
        function_names = [function["name"] for function in raw_functions]
        assert len(function_names) == len(set(function_names)), desc
        functions = {function["name"]: function for function in raw_functions}
        assert set(fields) == set(expected_fields), desc
        assert functions == {}, desc

        indexes = self.index_client.index_list(collection_name=collection_name)
        assert indexes["code"] == 0, indexes
        assert len(indexes["data"]) == len(set(indexes["data"])), indexes
        assert set(indexes["data"]) == set(expected_indexes), indexes
        index_metadata = {
            index_name: self._describe_index_state(collection_name, index_name) for index_name in indexes["data"]
        }
        return {
            "fields": fields,
            "functions": functions,
            "indexes": index_metadata,
            "properties": properties,
        }

    @pytest.mark.parametrize(
        "field_name,field_id,expected_message",
        [
            (None, None, "exactly one of fieldName or fieldId is required"),
            ("tag", 101, "exactly one of fieldName or fieldId is required"),
            (None, 0, "fieldId must be greater than 0"),
            (None, -1, "fieldId must be greater than 0"),
        ],
        ids=["missing-identifier", "both-identifiers", "zero-field-id", "negative-field-id"],
    )
    def test_drop_field_identifier_parameter_validation(self, field_name, field_id, expected_message):
        """
        target: verify REST drop field requires one valid identifier form
        method: omit both identifiers, provide both, or provide a non-positive fieldId
        expected: REST rejects each request before attempting a schema mutation
        """
        collection_name = self._create_collection()
        before_state = self._assert_collection_state(
            collection_name,
            expected_fields=["id", "dense", "tag"],
            expected_indexes=["dense_idx"],
        )
        rsp = self.collection_client.drop_field(
            collection_name,
            field_name=field_name,
            field_id=field_id,
        )
        assert rsp["code"] == 1100, rsp
        assert expected_message in rsp["message"], rsp
        after_state = self._assert_collection_state(
            collection_name,
            expected_fields=["id", "dense", "tag"],
            expected_indexes=["dense_idx"],
        )
        assert after_state == before_state

    @pytest.mark.parametrize(
        "field_name,expected_message",
        [
            ("id", "cannot drop primary key field"),
            ("dense", "cannot drop the last vector field"),
            ("missing_field", "field not found"),
        ],
        ids=["primary-key", "last-vector", "unknown-field"],
    )
    def test_drop_field_rejects_protected_or_unknown_field(self, field_name, expected_message):
        """
        target: verify REST surfaces server-side field drop validation
        method: request a primary-key, last-vector, or unknown field drop by name
        expected: each request fails without removing any schema field
        """
        collection_name = self._create_collection()

        before_state = self._assert_collection_state(
            collection_name,
            expected_fields=["id", "dense", "tag"],
            expected_indexes=["dense_idx"],
        )

        rsp = self.collection_client.drop_field(collection_name, field_name=field_name)
        assert rsp["code"] == 1100, rsp
        assert expected_message in rsp["message"], rsp

        after_state = self._assert_collection_state(
            collection_name,
            expected_fields=["id", "dense", "tag"],
            expected_indexes=["dense_idx"],
        )
        assert after_state == before_state

    def test_drop_field_second_request_is_rejected(self):
        """
        target: verify REST drop field reports a field that was already removed
        method: drop the highest-ID scalar field twice by name, then add a new field
        expected: the first request succeeds, the second is rejected, and the high-water field ID is preserved
        """
        collection_name = self._create_collection()

        before_state = self._assert_collection_state(
            collection_name,
            expected_fields=["id", "dense", "tag"],
            expected_indexes=["dense_idx"],
        )
        before_max_field_id = int(before_state["properties"]["max_field_id"])
        assert before_max_field_id == int(before_state["fields"]["tag"]["id"])

        first = self.collection_client.drop_field(collection_name, field_name="tag")
        assert first["code"] == 0, first
        after_first_state = self._assert_collection_state(
            collection_name,
            expected_fields=["id", "dense"],
            expected_indexes=["dense_idx"],
        )
        after_first_max_field_id = int(after_first_state["properties"]["max_field_id"])
        assert after_first_max_field_id == before_max_field_id
        assert after_first_max_field_id > max(int(field["id"]) for field in after_first_state["fields"].values())
        expected_after_first_state = {
            "fields": {name: field for name, field in before_state["fields"].items() if name != "tag"},
            "functions": before_state["functions"],
            "indexes": before_state["indexes"],
            "properties": before_state["properties"],
        }
        assert after_first_state == expected_after_first_state

        second = self.collection_client.drop_field(collection_name, field_name="tag")
        assert second["code"] == 1100, second
        assert "field not found" in second["message"], second
        after_second_state = self._assert_collection_state(
            collection_name,
            expected_fields=["id", "dense"],
            expected_indexes=["dense_idx"],
        )
        assert after_second_state == after_first_state

        rsp = self.collection_client.add_field(
            collection_name,
            {
                "fieldName": "restored_tag",
                "dataType": "VarChar",
                "nullable": True,
                "elementTypeParams": {"max_length": "64"},
            },
        )
        assert rsp["code"] == 0, rsp
        after_add_state = self._assert_collection_state(
            collection_name,
            expected_fields=["id", "dense", "restored_tag"],
            expected_indexes=["dense_idx"],
        )
        restored_tag_id = int(after_add_state["fields"]["restored_tag"]["id"])
        assert restored_tag_id > before_max_field_id
        assert int(after_add_state["properties"]["max_field_id"]) == restored_tag_id
