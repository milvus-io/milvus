import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name

FUNCTION_NAME = "bm25_fn"
OUTPUT_FIELD_NAME = "sparse"
INDEX_NAME = "sparse_idx"
KEEP_FUNCTION_NAME = "bm25_keep_fn"
KEEP_OUTPUT_FIELD_NAME = "sparse_keep"
KEEP_INDEX_NAME = "sparse_keep_idx"
BM25_FUNCTION_TYPE = 1


def _function_field_payload(
    collection_name,
    *,
    function_name=FUNCTION_NAME,
    output_field_name=OUTPUT_FIELD_NAME,
    index_name=INDEX_NAME,
):
    return {
        "collectionName": collection_name,
        "function": {
            "name": function_name,
            "type": "BM25",
            "inputFieldNames": ["text"],
            "outputFieldNames": [output_field_name],
            "params": {},
        },
        "outputField": {
            "fieldName": output_field_name,
            "dataType": "SparseFloatVector",
        },
        "indexParams": {
            "fieldName": output_field_name,
            "indexName": index_name,
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

    def _add_bm25_function_field(
        self,
        collection_name,
        *,
        function_name=FUNCTION_NAME,
        output_field_name=OUTPUT_FIELD_NAME,
        index_name=INDEX_NAME,
    ):
        rsp = self.collection_client.add_function_field(
            _function_field_payload(
                collection_name,
                function_name=function_name,
                output_field_name=output_field_name,
                index_name=index_name,
            )
        )
        assert rsp["code"] == 0, rsp

    def _describe_index_state(self, collection_name, index_name):
        rsp = self.index_client.index_describe(collection_name=collection_name, index_name=index_name)
        assert rsp["code"] == 0, rsp
        assert len(rsp["data"]) == 1, rsp
        index = rsp["data"][0]
        assert "fail" not in index["indexState"].lower(), rsp
        assert index["failReason"] == "", rsp
        index_params = [(param["key"], param["value"]) for param in index["indexParams"]]
        index_param_keys = [key for key, _ in index_params]
        assert len(index_param_keys) == len(set(index_param_keys)), index
        return {
            "fieldName": index["fieldName"],
            "indexName": index["indexName"],
            "metricType": index["metricType"],
            "indexType": index["indexType"],
            "indexParams": sorted(index_params),
        }

    def _assert_base_schema_state(self, collection_name):
        desc = self.collection_client.collection_describe(collection_name)
        assert desc["code"] == 0, desc
        raw_fields = desc["data"]["fields"]
        field_names = [field["name"] for field in raw_fields]
        assert field_names == ["id", "text", "dense"]
        assert len(field_names) == len(set(field_names)), desc
        fields = {field["name"]: field for field in raw_fields}

        raw_functions = desc["data"].get("functions", [])
        function_names = [function["name"] for function in raw_functions]
        assert len(function_names) == len(set(function_names)), desc
        functions = {function["name"]: function for function in raw_functions}
        assert set(fields) == {"id", "text", "dense"}
        assert functions == {}

        indexes = self.index_client.index_list(collection_name=collection_name)
        assert indexes["code"] == 0, indexes
        assert len(indexes["data"]) == len(set(indexes["data"])), indexes
        assert set(indexes["data"]) == {"dense_idx"}

        dense_index = self._describe_index_state(collection_name, "dense_idx")
        assert dense_index["fieldName"] == "dense"
        assert dense_index["indexName"] == "dense_idx"
        assert dense_index["metricType"] == "L2"
        assert dense_index["indexType"] == "AUTOINDEX"

        return {
            "fields": fields,
            "functions": functions,
            "indexes": {"dense_idx": dense_index},
        }

    def _assert_bm25_function_schema_state(
        self,
        collection_name,
        expected_bindings=None,
        expected_index_types=None,
    ):
        if expected_bindings is None:
            expected_bindings = [(FUNCTION_NAME, OUTPUT_FIELD_NAME, INDEX_NAME)]
        if expected_index_types is None:
            expected_index_types = {}

        desc = self.collection_client.collection_describe(collection_name)
        assert desc["code"] == 0, desc
        raw_fields = desc["data"]["fields"]
        field_names = [field["name"] for field in raw_fields]
        expected_output_fields_in_order = [output_field_name for _, output_field_name, _ in expected_bindings]
        assert field_names == ["id", "text", "dense", *expected_output_fields_in_order]
        assert len(field_names) == len(set(field_names)), desc
        fields = {field["name"]: field for field in raw_fields}
        expected_output_fields = {output_field_name for _, output_field_name, _ in expected_bindings}
        assert set(fields) == {"id", "text", "dense"} | expected_output_fields

        raw_functions = desc["data"].get("functions", [])
        function_names = [function["name"] for function in raw_functions]
        expected_function_names_in_order = [function_name for function_name, _, _ in expected_bindings]
        assert function_names == expected_function_names_in_order
        assert len(function_names) == len(set(function_names)), desc
        functions = {function["name"]: function for function in raw_functions}
        assert set(functions) == {function_name for function_name, _, _ in expected_bindings}

        indexes = self.index_client.index_list(collection_name=collection_name)
        assert indexes["code"] == 0, indexes
        assert len(indexes["data"]) == len(set(indexes["data"])), indexes
        expected_index_names = {index_name for _, _, index_name in expected_bindings}
        assert set(indexes["data"]) == {"dense_idx"} | expected_index_names

        dense_index = self._describe_index_state(collection_name, "dense_idx")
        assert dense_index["fieldName"] == "dense"
        assert dense_index["indexName"] == "dense_idx"
        assert dense_index["metricType"] == "L2"
        assert dense_index["indexType"] == "AUTOINDEX"
        index_metadata = {"dense_idx": dense_index}
        for function_name, output_field_name, index_name in expected_bindings:
            output_field = {key: value for key, value in fields[output_field_name].items() if key != "id"}
            assert output_field == {
                "autoId": False,
                "clusteringKey": False,
                "description": "",
                "isFunctionOutput": True,
                "name": output_field_name,
                "nullable": False,
                "partitionKey": False,
                "primaryKey": False,
                "type": "SparseFloatVector",
            }

            function = {key: value for key, value in functions[function_name].items() if key != "id"}
            assert function == {
                "description": "",
                "inputFieldNames": ["text"],
                "name": function_name,
                "outputFieldNames": [output_field_name],
                "params": None,
                "type": BM25_FUNCTION_TYPE,
            }

            index = self._describe_index_state(collection_name, index_name)
            expected_index_type = expected_index_types.get(index_name, "SPARSE_INVERTED_INDEX")
            assert index["fieldName"] == output_field_name
            assert index["indexName"] == index_name
            assert index["metricType"] == "BM25"
            assert index["indexType"] == expected_index_type
            assert index["indexParams"] == sorted(
                [
                    ("index_type", expected_index_type),
                    ("metric_type", "BM25"),
                ]
            )
            index_metadata[index_name] = index

        return {
            "fields": fields,
            "functions": functions,
            "indexes": index_metadata,
        }

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_function_field_creates_function_output_field_and_index(self):
        """
        target: verify REST add_function_field atomically creates all bound schema objects
        method: add a BM25 function, sparse output field, and sparse index in one request
        expected: describe endpoints expose the exact function, field, and index metadata
        """
        collection_name = self._create_base_collection()
        before_state = self._assert_base_schema_state(collection_name)

        rsp = self.collection_client.add_function_field(_function_field_payload(collection_name))
        assert rsp["code"] == 0, rsp

        after_state = self._assert_bm25_function_schema_state(collection_name)
        assert {name: after_state["fields"][name] for name in before_state["fields"]} == before_state["fields"]
        assert after_state["indexes"]["dense_idx"] == before_state["indexes"]["dense_idx"]

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
        before_state = self._assert_base_schema_state(collection_name)
        payload = _function_field_payload(collection_name)
        payload["function"]["outputFieldNames"] = function_output
        payload["indexParams"]["fieldName"] = index_field

        rsp = self.collection_client.add_function_field(payload)
        assert rsp["code"] == 1100, rsp
        assert expected_message in rsp["message"], rsp

        after_state = self._assert_base_schema_state(collection_name)
        assert after_state == before_state

    @pytest.mark.parametrize(
        "missing_key",
        ["function", "outputField"],
    )
    def test_add_function_field_requires_atomic_request_parts(self, missing_key):
        """
        target: verify the function and output field parts of add_function_field are required
        method: omit either the function or outputField object
        expected: REST binding rejects each incomplete request
        """
        collection_name = self._create_base_collection()
        before_state = self._assert_base_schema_state(collection_name)
        payload = _function_field_payload(collection_name)
        del payload[missing_key]

        rsp = self.collection_client.add_function_field(payload)
        assert rsp["code"] == 1802, rsp
        assert "required" in rsp["message"], rsp
        after_state = self._assert_base_schema_state(collection_name)
        assert after_state == before_state

    def test_add_function_field_without_index_params_uses_autoindex(self):
        """
        target: verify REST add_function_field supplies a bound AutoIndex when indexParams is omitted
        method: add a BM25 function and sparse output field without an indexParams object
        expected: the request succeeds and describe endpoints expose a same-name AUTOINDEX with BM25 metric
        """
        collection_name = self._create_base_collection()
        before_state = self._assert_base_schema_state(collection_name)
        payload = _function_field_payload(collection_name)
        del payload["indexParams"]

        rsp = self.collection_client.add_function_field(payload)
        assert rsp["code"] == 0, rsp

        after_state = self._assert_bm25_function_schema_state(
            collection_name,
            expected_bindings=[(FUNCTION_NAME, OUTPUT_FIELD_NAME, OUTPUT_FIELD_NAME)],
            expected_index_types={OUTPUT_FIELD_NAME: "AUTOINDEX"},
        )
        assert {name: after_state["fields"][name] for name in before_state["fields"]} == before_state["fields"]
        assert after_state["indexes"]["dense_idx"] == before_state["indexes"]["dense_idx"]

    def test_add_function_field_rejects_unsupported_function_type(self):
        """
        target: verify add_function_field accepts only function types supported by schema backfill
        method: send a TextEmbedding function with a newly-defined dense output field and index
        expected: REST returns invalid parameter instead of creating partial schema objects
        """
        collection_name = self._create_base_collection()
        before_state = self._assert_base_schema_state(collection_name)
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
        after_state = self._assert_base_schema_state(collection_name)
        assert after_state == before_state

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_function_field_removes_function_output_field_and_index(self):
        """
        target: verify REST drop_function_field cascades only through the named function binding
        method: add two BM25 bindings, drop one by name, and compare the surviving field/function/index metadata
        expected: only the selected function, output field, and index disappear; the other binding is unchanged
        """
        collection_name = self._create_base_collection()
        self._add_bm25_function_field(collection_name)
        self._add_bm25_function_field(
            collection_name,
            function_name=KEEP_FUNCTION_NAME,
            output_field_name=KEEP_OUTPUT_FIELD_NAME,
            index_name=KEEP_INDEX_NAME,
        )
        before_state = self._assert_bm25_function_schema_state(
            collection_name,
            expected_bindings=[
                (FUNCTION_NAME, OUTPUT_FIELD_NAME, INDEX_NAME),
                (KEEP_FUNCTION_NAME, KEEP_OUTPUT_FIELD_NAME, KEEP_INDEX_NAME),
            ],
        )

        rsp = self.collection_client.drop_function_field(
            {"collectionName": collection_name, "functionName": FUNCTION_NAME}
        )
        assert rsp["code"] == 0, rsp

        after_state = self._assert_bm25_function_schema_state(
            collection_name,
            expected_bindings=[(KEEP_FUNCTION_NAME, KEEP_OUTPUT_FIELD_NAME, KEEP_INDEX_NAME)],
        )
        expected_after_state = {
            "fields": {name: field for name, field in before_state["fields"].items() if name != OUTPUT_FIELD_NAME},
            "functions": {
                name: function for name, function in before_state["functions"].items() if name != FUNCTION_NAME
            },
            "indexes": {name: index for name, index in before_state["indexes"].items() if name != INDEX_NAME},
        }
        assert after_state == expected_after_state

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
            before_state = self._assert_bm25_function_schema_state(collection_name)
        else:
            before_state = self._assert_base_schema_state(collection_name)

        rsp = self.collection_client.drop_function_field(
            {"collectionName": collection_name, "functionName": function_name}
        )
        assert rsp["code"] == expected_code, rsp
        assert expected_message in rsp["message"], rsp

        if seed_function:
            after_state = self._assert_bm25_function_schema_state(collection_name)
        else:
            after_state = self._assert_base_schema_state(collection_name)
        assert after_state == before_state

    def test_drop_function_field_second_request_is_rejected(self):
        """
        target: verify repeated drop_function_field is rejected and remains atomic
        method: drop once and assert base state, then drop again and assert rejection plus the same base state
        expected: the first drop removes all bound objects and the rejected second drop changes nothing
        """
        collection_name = self._create_base_collection()
        base_state = self._assert_base_schema_state(collection_name)
        self._add_bm25_function_field(collection_name)
        payload = {"collectionName": collection_name, "functionName": FUNCTION_NAME}

        first = self.collection_client.drop_function_field(payload)
        assert first["code"] == 0, first
        after_first_state = self._assert_base_schema_state(collection_name)
        assert after_first_state == base_state

        second = self.collection_client.drop_function_field(payload)
        assert second["code"] == 1100, second
        assert "function not found" in second["message"], second
        after_second_state = self._assert_base_schema_state(collection_name)
        assert after_second_state == after_first_state

    def test_drop_field_rejects_function_input_until_function_field_is_dropped(self):
        """
        target: verify REST drop field honors function input dependencies
        method: drop the BM25 input field before and after dropping its function field
        expected: the dependency blocks the first drop; the same field is removable after the cascade
        """
        collection_name = self._create_base_collection()
        self._add_bm25_function_field(collection_name)
        before_blocked_state = self._assert_bm25_function_schema_state(collection_name)

        blocked = self.collection_client.drop_field(collection_name, field_name="text")
        assert blocked["code"] == 1100, blocked
        assert "referenced by function" in blocked["message"], blocked
        after_blocked_state = self._assert_bm25_function_schema_state(collection_name)
        assert after_blocked_state == before_blocked_state

        dropped = self.collection_client.drop_function_field(
            {"collectionName": collection_name, "functionName": FUNCTION_NAME}
        )
        assert dropped["code"] == 0, dropped
        before_unblocked_state = self._assert_base_schema_state(collection_name)

        unblocked = self.collection_client.drop_field(collection_name, field_name="text")
        assert unblocked["code"] == 0, unblocked

        desc = self.collection_client.collection_describe(collection_name)
        assert desc["code"] == 0, desc
        fields = {field["name"]: field for field in desc["data"]["fields"]}
        functions = {function["name"]: function for function in desc["data"].get("functions", [])}
        assert set(fields) == {"id", "dense"}
        assert functions == {}

        indexes = self.index_client.index_list(collection_name=collection_name)
        assert indexes["code"] == 0, indexes
        assert set(indexes["data"]) == {"dense_idx"}
        after_unblocked_state = {
            "fields": fields,
            "functions": functions,
            "indexes": {"dense_idx": self._describe_index_state(collection_name, "dense_idx")},
        }
        expected_after_unblocked_state = {
            "fields": {name: field for name, field in before_unblocked_state["fields"].items() if name != "text"},
            "functions": before_unblocked_state["functions"],
            "indexes": before_unblocked_state["indexes"],
        }
        assert after_unblocked_state == expected_after_unblocked_state
