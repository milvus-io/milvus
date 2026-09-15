import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name


class TestMembershipFilters(TestBase):
    """RESTful v2 protocol-negative coverage for membership expressions.

    The RESTful expression-template API accepts JSON-representable values but
    currently has no JSON mapping to TemplateValue.BytesVal. Positive blob
    coverage belongs to Go and PyMilvus until REST exposes a bytes representation.
    """

    def _create_collection(self, scenario):
        name = f"{self.__class__.__name__}_{scenario}_{gen_collection_name()}"
        rsp = self.collection_client.collection_create(
            {
                "collectionName": name,
                "schema": {
                    "autoId": False,
                    "enableDynamicField": False,
                    "fields": [
                        {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                        {"fieldName": "creator_id", "dataType": "Int64"},
                        {
                            "fieldName": "vector",
                            "dataType": "FloatVector",
                            "elementTypeParams": {"dim": "4"},
                        },
                    ],
                },
            }
        )
        assert rsp["code"] == 0, rsp
        return name

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        ("expression", "param_key"),
        [
            ("membership_match(creator_id, {bf}, type=bloom)", "bf"),
            ("membership_match(creator_id, {rb}, type=roaring)", "rb"),
        ],
    )
    def test_restful_membership_blob_parameter_rejected(self, expression, param_key):
        """
        target: verify JSON strings cannot masquerade as membership bytes and missing params fail at parsing
        method: 1. send a string template value; 2. omit the required expression parameter
        expected: 1. a bytes type error is returned; 2. the missing placeholder is named
        """
        name = self._create_collection("membership_param")
        rsp = self.vector_client.vector_query(
            {
                "collectionName": name,
                "filter": expression,
                "exprParams": {param_key: "not-a-bytes-blob"},
                "outputFields": ["id"],
            }
        )
        assert rsp["code"] != 0, rsp
        assert "bytes" in rsp.get("message", "").lower(), rsp

        rsp = self.vector_client.vector_query({"collectionName": name, "filter": expression, "outputFields": ["id"]})
        assert rsp["code"] != 0, rsp
        assert f"expression template variable name {{{param_key}}} is not found" in rsp.get("message", ""), rsp

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        ("expression", "param_key"),
        [("bloom_match(creator_id, {bf})", "bf"), ("roaring_match(creator_id, {rb})", "rb")],
    )
    def test_restful_membership_predecessor_names_rejected(self, expression, param_key):
        """
        target: verify RESTful v2 exposes only the unified membership_match name
        method: 1. submit each predecessor expression with a placeholder value
        expected: 1. parsing fails and directs the caller to membership_match
        """
        name = self._create_collection("predecessor_name")
        rsp = self.vector_client.vector_query(
            {
                "collectionName": name,
                "filter": expression,
                "exprParams": {param_key: "not-a-bytes-blob"},
                "outputFields": ["id"],
            }
        )
        assert rsp["code"] != 0, rsp
        message = rsp.get("message", "")
        assert "is not supported" in message, rsp
        assert "membership_match" in message, rsp
