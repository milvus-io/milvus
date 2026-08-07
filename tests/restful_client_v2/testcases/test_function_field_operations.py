import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name

FUNCTION_NAME = "bm25_fn"
OUTPUT_FIELD_NAME = "sparse"
INDEX_NAME = "sparse_idx"
BM25_FUNCTION_TYPE = 1


def _function_field_payload(collection_name):
    return {
        "collectionName": collection_name,
        "function": {
            "name": FUNCTION_NAME,
            "type": "BM25",
            "inputFieldNames": ["text"],
            "outputFieldNames": [OUTPUT_FIELD_NAME],
            "params": {},
        },
        "outputField": {
            "fieldName": OUTPUT_FIELD_NAME,
            "dataType": "SparseFloatVector",
        },
        "indexParams": {
            "fieldName": OUTPUT_FIELD_NAME,
            "indexName": INDEX_NAME,
            "metricType": "BM25",
            "indexType": "SPARSE_INVERTED_INDEX",
            "params": {},
        },
    }


@pytest.mark.tags(CaseLabel.L1)
class TestCollectionFunctionField(TestBase):
    """REST v2 contract tests for atomic function-field schema operations."""

    def _create_base_collection(self):
        collection_name = gen_collection_name(prefix=self.__class__.__name__)
        rsp = self.collection_client.collection_create(
            {
                "collectionName": collection_name,
                "schema": {
                    "autoId": False,
                    "enableDynamicField": False,
                    "fields": [
                        {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                        {
                            "fieldName": "text",
                            "dataType": "VarChar",
                            "elementTypeParams": {
                                "max_length": "1024",
                                "enable_analyzer": True,
                                "analyzer_params": {"tokenizer": "standard"},
                            },
                        },
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

    def _add_bm25_function_field(self, collection_name):
        rsp = self.collection_client.add_function_field(_function_field_payload(collection_name))
        assert rsp["code"] == 0, rsp

    def _assert_base_schema_state(self, collection_name):
        desc = self.collection_client.collection_describe(collection_name)
        assert desc["code"] == 0, desc
        assert {field["name"] for field in desc["data"]["fields"]} == {"id", "text", "dense"}
        assert desc["data"].get("functions", []) == []

        indexes = self.index_client.index_list(collection_name=collection_name)
        assert indexes["code"] == 0, indexes
        assert indexes["data"] == ["dense_idx"]

    def _assert_bm25_function_schema_state(self, collection_name):
        desc = self.collection_client.collection_describe(collection_name)
        assert desc["code"] == 0, desc
        fields = {field["name"]: field for field in desc["data"]["fields"]}
        assert set(fields) == {"id", "text", "dense", OUTPUT_FIELD_NAME}
        assert fields[OUTPUT_FIELD_NAME]["type"] == "SparseFloatVector"

        functions = {function["name"]: function for function in desc["data"].get("functions", [])}
        assert set(functions) == {FUNCTION_NAME}
        assert functions[FUNCTION_NAME]["type"] == BM25_FUNCTION_TYPE
        assert functions[FUNCTION_NAME]["inputFieldNames"] == ["text"]
        assert functions[FUNCTION_NAME]["outputFieldNames"] == [OUTPUT_FIELD_NAME]

        indexes = self.index_client.index_list(collection_name=collection_name)
        assert indexes["code"] == 0, indexes
        assert set(indexes["data"]) == {"dense_idx", INDEX_NAME}

        index_desc = self.index_client.index_describe(collection_name=collection_name, index_name=INDEX_NAME)
        assert index_desc["code"] == 0, index_desc
        assert len(index_desc["data"]) == 1, index_desc
        index = index_desc["data"][0]
        assert index["fieldName"] == OUTPUT_FIELD_NAME
        assert index["indexName"] == INDEX_NAME
        assert index["metricType"] == "BM25"
        assert index["indexType"] == "SPARSE_INVERTED_INDEX"

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_function_field_creates_function_output_field_and_index(self):
        """
        target: verify REST add_function_field atomically creates all bound schema objects
        method: add a BM25 function, sparse output field, and sparse index in one request
        expected: describe endpoints expose the exact function, field, and index metadata
        """
        collection_name = self._create_base_collection()

        rsp = self.collection_client.add_function_field(_function_field_payload(collection_name))
        assert rsp["code"] == 0, rsp

        desc = self.collection_client.collection_describe(collection_name)
        assert desc["code"] == 0, desc
        fields = {field["name"]: field for field in desc["data"]["fields"]}
        functions = {function["name"]: function for function in desc["data"].get("functions", [])}
        assert fields[OUTPUT_FIELD_NAME]["type"] == "SparseFloatVector"
        assert functions[FUNCTION_NAME]["type"] == BM25_FUNCTION_TYPE
        assert functions[FUNCTION_NAME]["inputFieldNames"] == ["text"]
        assert functions[FUNCTION_NAME]["outputFieldNames"] == [OUTPUT_FIELD_NAME]

        index_desc = self.index_client.index_describe(collection_name=collection_name, index_name=INDEX_NAME)
        assert index_desc["code"] == 0, index_desc
        assert len(index_desc["data"]) == 1, index_desc
        index = index_desc["data"][0]
        assert index["fieldName"] == OUTPUT_FIELD_NAME
        assert index["indexName"] == INDEX_NAME
        assert index["metricType"] == "BM25"
        assert index["indexType"] == "SPARSE_INVERTED_INDEX"

    @pytest.mark.parametrize(
        "function_output,index_field,expected_message",
        [
            (["other_sparse"], OUTPUT_FIELD_NAME, "function output field"),
            ([OUTPUT_FIELD_NAME], "other_sparse", "must match outputField.fieldName"),
        ],
        ids=["function-output-mismatch", "index-field-mismatch"],
    )
    def test_add_function_field_rejects_inconsistent_field_names(
        self,
        function_output,
        index_field,
        expected_message,
    ):
        """
        target: verify REST validates the three field-name bindings in add_function_field
        method: mismatch either function output or index target from the output field
        expected: request fails as invalid input without changing the collection schema
        """
        collection_name = self._create_base_collection()
        payload = _function_field_payload(collection_name)
        payload["function"]["outputFieldNames"] = function_output
        payload["indexParams"]["fieldName"] = index_field

        rsp = self.collection_client.add_function_field(payload)
        assert rsp["code"] == 1100, rsp
        assert expected_message in rsp["message"], rsp

        self._assert_base_schema_state(collection_name)

    @pytest.mark.parametrize(
        "missing_key",
        ["function", "outputField", "indexParams"],
    )
    def test_add_function_field_requires_atomic_request_parts(self, missing_key):
        """
        target: verify all atomic add_function_field request parts are required
        method: omit the function, outputField, or indexParams object
        expected: REST binding rejects each incomplete request
        """
        collection_name = self._create_base_collection()
        payload = _function_field_payload(collection_name)
        del payload[missing_key]

        rsp = self.collection_client.add_function_field(payload)
        assert rsp["code"] == 1802, rsp
        assert "required" in rsp["message"], rsp
        self._assert_base_schema_state(collection_name)

    def test_add_function_field_rejects_unsupported_function_type(self):
        """
        target: verify add_function_field accepts only function types supported by schema backfill
        method: send a TextEmbedding function with a newly-defined dense output field and index
        expected: REST returns invalid parameter instead of creating partial schema objects
        """
        collection_name = self._create_base_collection()
        payload = _function_field_payload(collection_name)
        payload["function"].update(
            {
                "type": "TextEmbedding",
                "outputFieldNames": ["embedding"],
            }
        )
        payload["outputField"] = {
            "fieldName": "embedding",
            "dataType": "FloatVector",
            "elementTypeParams": {"dim": "4"},
        }
        payload["indexParams"] = {
            "fieldName": "embedding",
            "indexName": "embedding_idx",
            "metricType": "L2",
            "indexType": "HNSW",
            "params": {"M": 8, "efConstruction": 64},
        }

        rsp = self.collection_client.add_function_field(payload)
        assert rsp["code"] == 1100, rsp
        assert "only BM25 and MinHash functions are supported" in rsp["message"], rsp
        self._assert_base_schema_state(collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_function_field_removes_function_output_field_and_index(self):
        """
        target: verify REST drop_function_field performs the documented cascade
        method: add a BM25 function field and then drop it by function name
        expected: function, sparse output field, and bound index all disappear
        """
        collection_name = self._create_base_collection()
        self._add_bm25_function_field(collection_name)

        rsp = self.collection_client.drop_function_field(
            {"collectionName": collection_name, "functionName": FUNCTION_NAME}
        )
        assert rsp["code"] == 0, rsp

        self._assert_base_schema_state(collection_name)

    @pytest.mark.parametrize(
        "function_name,expected_code,expected_message,seed_function",
        [
            ("", 1802, "required", False),
            ("missing_fn", 1100, "function not found", True),
        ],
        ids=["empty-name", "unknown-function"],
    )
    def test_drop_function_field_rejects_invalid_function_name(
        self,
        function_name,
        expected_code,
        expected_message,
        seed_function,
    ):
        """
        target: verify drop_function_field validates the functionName parameter
        method: send an empty name or seed BM25 and send a different, absent function name
        expected: REST returns the expected error without changing any field, function, or index
        """
        collection_name = self._create_base_collection()
        if seed_function:
            self._add_bm25_function_field(collection_name)
            self._assert_bm25_function_schema_state(collection_name)

        rsp = self.collection_client.drop_function_field(
            {"collectionName": collection_name, "functionName": function_name}
        )
        assert rsp["code"] == expected_code, rsp
        assert expected_message in rsp["message"], rsp

        if seed_function:
            self._assert_bm25_function_schema_state(collection_name)
        else:
            self._assert_base_schema_state(collection_name)

    def test_drop_function_field_second_request_is_rejected(self):
        """
        target: verify drop_function_field is not silently idempotent
        method: drop the same function field twice
        expected: the second request reports that the function no longer exists
        """
        collection_name = self._create_base_collection()
        self._add_bm25_function_field(collection_name)
        payload = {"collectionName": collection_name, "functionName": FUNCTION_NAME}

        first = self.collection_client.drop_function_field(payload)
        assert first["code"] == 0, first
        second = self.collection_client.drop_function_field(payload)
        assert second["code"] == 1100, second
        assert "function not found" in second["message"], second

    def test_drop_field_rejects_function_input_until_function_field_is_dropped(self):
        """
        target: verify REST drop field honors function input dependencies
        method: drop the BM25 input field before and after dropping its function field
        expected: the dependency blocks the first drop; the same field is removable after the cascade
        """
        collection_name = self._create_base_collection()
        self._add_bm25_function_field(collection_name)
        self._assert_bm25_function_schema_state(collection_name)

        blocked = self.collection_client.drop_field(collection_name, field_name="text")
        assert blocked["code"] == 1100, blocked
        assert "referenced by function" in blocked["message"], blocked
        self._assert_bm25_function_schema_state(collection_name)

        dropped = self.collection_client.drop_function_field(
            {"collectionName": collection_name, "functionName": FUNCTION_NAME}
        )
        assert dropped["code"] == 0, dropped
        self._assert_base_schema_state(collection_name)

        unblocked = self.collection_client.drop_field(collection_name, field_name="text")
        assert unblocked["code"] == 0, unblocked

        desc = self.collection_client.collection_describe(collection_name)
        assert desc["code"] == 0, desc
        assert {field["name"] for field in desc["data"]["fields"]} == {"id", "dense"}
        assert desc["data"].get("functions", []) == []

        indexes = self.index_client.index_list(collection_name=collection_name)
        assert indexes["code"] == 0, indexes
        assert indexes["data"] == ["dense_idx"]
