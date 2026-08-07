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
                        {"fieldName": "tag", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                        {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": "4"}},
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

    def _assert_collection_state(self, collection_name, *, expected_fields, expected_indexes):
        desc = self.collection_client.collection_describe(collection_name)
        assert desc["code"] == 0, desc
        actual_fields = sorted(field["name"] for field in desc["data"]["fields"])
        assert actual_fields == sorted(expected_fields), desc

        indexes = self.index_client.index_list(collection_name=collection_name)
        assert indexes["code"] == 0, indexes
        assert sorted(indexes["data"]) == sorted(expected_indexes), indexes

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
        rsp = self.collection_client.drop_field(
            "drop_field_parameter_validation",
            field_name=field_name,
            field_id=field_id,
        )
        assert rsp["code"] == 1100, rsp
        assert expected_message in rsp["message"], rsp

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

        self._assert_collection_state(
            collection_name,
            expected_fields=["id", "tag", "dense"],
            expected_indexes=["dense_idx"],
        )

        rsp = self.collection_client.drop_field(collection_name, field_name=field_name)
        assert rsp["code"] == 1100, rsp
        assert expected_message in rsp["message"], rsp

        self._assert_collection_state(
            collection_name,
            expected_fields=["id", "tag", "dense"],
            expected_indexes=["dense_idx"],
        )

    def test_drop_field_second_request_is_rejected(self):
        """
        target: verify REST drop field reports a field that was already removed
        method: drop the same scalar field twice by name
        expected: the first request succeeds and the second reports field not found
        """
        collection_name = self._create_collection()

        self._assert_collection_state(
            collection_name,
            expected_fields=["id", "tag", "dense"],
            expected_indexes=["dense_idx"],
        )

        first = self.collection_client.drop_field(collection_name, field_name="tag")
        assert first["code"] == 0, first
        self._assert_collection_state(
            collection_name,
            expected_fields=["id", "dense"],
            expected_indexes=["dense_idx"],
        )

        second = self.collection_client.drop_field(collection_name, field_name="tag")
        assert second["code"] == 1100, second
        assert "field not found" in second["message"], second
        self._assert_collection_state(
            collection_name,
            expected_fields=["id", "dense"],
            expected_indexes=["dense_idx"],
        )
