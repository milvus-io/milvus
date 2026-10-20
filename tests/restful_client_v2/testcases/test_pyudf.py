import copy
import io
import json
import time
import zipfile
from contextlib import contextmanager

import pytest
from api.milvus import FileResourceClient, VectorClient
from base.testbase import TestBase
from pymilvus import Collection
from utils.constant import CaseLabel
from utils.utils import gen_collection_name, gen_unique_str


class TestPyUDF(TestBase):
    """REST Search coverage with real FileResource wheels and deterministic results."""

    @staticmethod
    def _wheel_bytes(package_name):
        module = """import json
import pyarrow as pa

class UDF:
    def transform_query(self, params, columns):
        mode = params.get("mode", "valid")
        if mode == "exception":
            raise RuntimeError("controlled REST PyUDF failure")
        if mode == "wrong_rows":
            return [pa.array([1.0] * (len(columns[0]) - 1), type=pa.float32())]
        if mode == "binary":
            return [pa.array([b"x"] * len(columns[0]), type=pa.binary())]
        if mode == "null_score":
            return [pa.array([None] * len(columns[0]), type=pa.float32())]
        if mode == "nullable_input":
            assert columns[1].type == pa.int64()
            values = [value.as_py() if value.is_valid else -1 for value in columns[1]]
            return [pa.array([float(value) * 2 for value in values], type=pa.float32())]
        if mode == "typed_probe":
            arrow_types = {"bool": pa.bool_(), "int64": pa.int64(),
                           "double": pa.float64(), "varchar": pa.string()}
            assert columns[1].type == arrow_types[params["input_type"]]
            assert columns[2].type == pa.int64()
            expected_values = json.loads(params["expected_json"])
            values = []
            for projected, scalar in zip(columns[1], columns[2]):
                ts = scalar.as_py()
                expected = expected_values[ts // 10 - 1]
                actual = projected.as_py()
                assert actual == expected, (ts, actual, expected)
                assert projected.is_valid == (expected is not None)
                values.append(float(ts + (100 if projected.is_valid else 0)))
            return [pa.array(values, type=pa.float32())]
        values = [float(value.as_py()) * 2 for value in columns[1]]
        return [pa.array(values, type=pa.float32())]

def create_udf(context):
    return UDF()
"""
        sink = io.BytesIO()
        with zipfile.ZipFile(sink, "w", compression=zipfile.ZIP_STORED) as wheel:
            wheel.writestr(f"{package_name}/__init__.py", module)
            wheel.writestr(
                f"{package_name}-0.0.1.dist-info/entry_points.txt",
                f"[milvus.pyudf]\nmain = {package_name}:create_udf\n",
            )
            wheel.writestr(
                f"{package_name}-0.0.1.dist-info/METADATA",
                f"Metadata-Version: 2.1\nName: {package_name}\nVersion: 0.0.1\n",
            )
        return sink.getvalue()

    def _create_collection(self, json_values=None, dynamic=False):
        self.name = gen_collection_name("rest_pyudf")
        payload = {
            "collectionName": self.name,
            "schema": {
                "autoId": False,
                "enableDynamicField": dynamic,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "ts", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": "2"}},
                ],
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector", "indexType": "FLAT", "metricType": "L2"}],
        }
        if json_values is not None and not dynamic:
            payload["schema"]["fields"].append({"fieldName": "payload", "dataType": "JSON"})
        response = self.collection_client.collection_create(payload)
        assert response["code"] == 0, response
        rows = [
            {"id": 1, "ts": 10, "vector": [0.0, 0.0]},
            {"id": 2, "ts": 20, "vector": [0.01, 0.0]},
            {"id": 3, "ts": 30, "vector": [0.02, 0.0]},
        ]
        if json_values is not None:
            assert len(json_values) == len(rows)
            for row, value in zip(rows, json_values):
                row["boost" if dynamic else "payload"] = value if dynamic else {"boost": value}
        response = self.vector_client.vector_insert(
            {
                "collectionName": self.name,
                "data": rows,
            }
        )
        assert response["code"] == 0, response
        assert response["data"]["insertCount"] == 3
        # Match the neighboring REST suite: SDK is used for flush, never for Search.
        Collection(self.name).flush()
        response = self.collection_client.collection_load(collection_name=self.name)
        assert response["code"] == 0, response
        self.wait_load_completed(self.name, timeout=30)

    @contextmanager
    def _pyudf_environment(self, json_values=None, dynamic=False):
        self._create_collection(json_values=json_values, dynamic=dynamic)
        resource = gen_unique_str("rest_pyudf_resource")
        package = gen_unique_str("rest_pyudf_package").lower()
        path = f"pyudf/{resource}.whl"
        content = self._wheel_bytes(package)
        uploaded = False
        registration_attempted = False
        try:
            self.storage_client.client.put_object(
                self.storage_client.bucket_name,
                path,
                io.BytesIO(content),
                len(content),
                content_type="application/octet-stream",
            )
            uploaded = True
            registration_attempted = True
            response = self.file_resource_client.file_resource_add({"name": resource, "path": path})
            assert response["code"] == 0, response
            yield resource
        finally:
            removed = not registration_attempted
            if registration_attempted:
                deadline = time.monotonic() + 30
                while True:
                    response = self.file_resource_client.file_resource_remove({"name": resource})
                    message = response.get("message", "")
                    if response["code"] == 0 or ("not found" in message and resource in message):
                        removed = True
                        break
                    if "is still in use" not in message or time.monotonic() >= deadline:
                        raise AssertionError(f"PyUDF resource cleanup failed; wheel retained: {response}")
                    time.sleep(0.2)
            if uploaded and removed:
                self.storage_client.client.remove_object(self.storage_client.bucket_name, path)

    def _search_payload(self, resource):
        return {
            "collectionName": self.name,
            "data": [[0.0, 0.0]],
            "annsField": "vector",
            "limit": 3,
            "outputFields": ["ts"],
            "searchParams": {"metricType": "L2"},
            "consistencyLevel": "Strong",
            "functionChains": [
                {
                    "stage": "FunctionChainStageL2Rerank",
                    "ops": [
                        {
                            "op": "map",
                            "outputs": ["$score"],
                            "expr": {
                                "name": "py_udf",
                                "args": [{"column": "$score"}, {"column": "ts"}],
                                "params": {"resource_name": resource},
                            },
                        },
                        {"op": "sort", "inputs": ["$score"], "params": {"column": "$score", "desc": True}},
                    ],
                }
            ],
        }

    def _search_once(self, payload, client=None):
        # vector_search replays empty responses; inspect the first REST result here.
        client = self.vector_client if client is None else client
        return client.post(
            f"{self.endpoint}/v2/vectordb/entities/search",
            headers=client.update_headers(),
            data=payload,
        ).json()

    def _search_after_resource_sync(self, payload):
        deadline = time.monotonic() + 30
        while True:
            response = self._search_once(payload)
            message = response.get("message", "")
            transient = "file resource snapshot is not ready" in message or (
                "file resource" in message and "not found" in message
            )
            if not transient or response.get("code") not in (2, 1100) or time.monotonic() >= deadline:
                return response
            time.sleep(0.2)

    @staticmethod
    def _assert_ranked_response(response):
        assert response["code"] == 0, response
        assert len(response["data"]) == 3, response
        assert [hit["id"] for hit in response["data"]] == [3, 2, 1]
        assert [hit["distance"] for hit in response["data"]] == pytest.approx([60, 40, 20], rel=1e-5, abs=1e-5)
        assert [hit["ts"] for hit in response["data"]] == [30, 20, 10]

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_pyudf_reranks_results(self):
        """
        target: rerank REST Search using a synchronized Python wheel
        method: register a wheel through REST and explicitly map/sort three known rows
        expected: REST returns exact IDs, scores and scalar alignment rather than only a nonempty response
        """
        with self._pyudf_environment() as resource:
            payload = self._search_payload(resource)
            self._assert_ranked_response(self._search_after_resource_sync(payload))

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "failure, code, message",
        [
            pytest.param("missing_parameter", 1100, "resource_name", id="missing_parameter"),
            pytest.param("blank_resource", 1100, "resource_name", id="blank_resource"),
            pytest.param("resource_type", 1100, "must be a string", id="resource_type"),
            pytest.param("params_type", 1100, "udf_params", id="params_type"),
            pytest.param("unknown_parameter", 1100, "unknown parameter", id="unknown_parameter"),
            pytest.param("missing_resource", 1100, "not found", id="missing_resource"),
            pytest.param("bad_stage", 1100, "stage", id="bad_stage"),
            pytest.param("readonly_output", 1100, "system output", id="readonly_output"),
            pytest.param("exception", 2400, "controlled REST PyUDF failure", id="udf_exception"),
            pytest.param("wrong_rows", 2400, "rows, expected 3", id="wrong_rows"),
            pytest.param("binary", 2400, "unsupported type", id="binary_output"),
            pytest.param("null_score", 2400, "$score contains null", id="null_score"),
        ],
    )
    def test_search_pyudf_errors_and_recovery(self, failure, code, message):
        """
        target: expose REST PyUDF failures without partial success or poisoning later requests
        method: alter a warmed valid request, assert exact error classification, then rerun both controls
        expected: the negative response has its expected code/cause and ordinary/UDF Search still return known content
        """
        with self._pyudf_environment() as resource:
            payload = self._search_payload(resource)
            self._assert_ranked_response(self._search_after_resource_sync(payload))
            invalid = copy.deepcopy(payload)
            chain = invalid["functionChains"][0]
            operation = chain["ops"][0]
            params = operation["expr"]["params"]
            if failure == "missing_parameter":
                params.clear()
            elif failure == "blank_resource":
                params["resource_name"] = "  "
            elif failure == "resource_type":
                params["resource_name"] = 7
            elif failure == "params_type":
                params["udf_params"] = "bad"
            elif failure == "unknown_parameter":
                params["unexpected"] = True
            elif failure == "missing_resource":
                params["resource_name"] = gen_unique_str("unregistered_pyudf")
            elif failure == "bad_stage":
                chain["stage"] = "FunctionChainStageIngestion"
            elif failure == "readonly_output":
                operation["outputs"] = ["$id"]
            else:
                params["udf_params"] = {"mode": failure}
            # Do not replay an error response through the empty-result polling wrapper.
            response = self._search_once(invalid)
            assert response["code"] == code, response
            assert message in response["message"], response
            assert not response.get("data"), response
            plain = copy.deepcopy(payload)
            plain.pop("functionChains")
            response = self._search_once(plain)
            assert response["code"] == 0, response
            assert [hit["id"] for hit in response["data"]] == [1, 2, 3]
            assert [hit["ts"] for hit in response["data"]] == [10, 20, 30]
            self._assert_ranked_response(self._search_once(payload))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("hint_case", ["int64", "double", "missing", "wrong_count", "unknown_type"])
    def test_search_pyudf_typed_json_projection_and_hint_validation(self, hint_case):
        """target: typed JSON input; method: REST operator type hints; expected: exact projected scores or rejection."""
        values = [30.5, 10.5, 20.5] if hint_case == "double" else [30, 10, 20]
        with self._pyudf_environment(json_values=values) as resource:
            payload = self._search_payload(resource)
            self._assert_ranked_response(self._search_after_resource_sync(payload))
            projected = copy.deepcopy(payload)
            operation = projected["functionChains"][0]["ops"][0]
            operation["expr"]["args"][1]["column"] = 'payload["boost"]'
            if hint_case != "missing":
                hints = [0, 11 if hint_case == "double" else 5]
                if hint_case == "wrong_count":
                    hints = [5]
                elif hint_case == "unknown_type":
                    hints = [0, 999]
                operation["params"] = {"$input_data_types": hints}
            response = self._search_once(projected)
            if hint_case in ("int64", "double"):
                assert response["code"] == 0, response
                assert [hit["id"] for hit in response["data"]] == [1, 3, 2]
                assert [hit["distance"] for hit in response["data"]] == pytest.approx(
                    [values[i] * 2 for i in (0, 2, 1)]
                )
                assert [hit["ts"] for hit in response["data"]] == [10, 30, 20]
                assert all("payload" not in hit for hit in response["data"])
            else:
                message = {
                    "missing": "explicit data_type",
                    "wrong_count": "does not match input count",
                    "unknown_type": "unknown data type value",
                }[hint_case]
                assert response["code"] == 1100, response
                assert message in response["message"], response
                assert not response.get("data"), response
            self._assert_ranked_response(self._search_once(payload))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "values,path",
        [
            pytest.param([30, None, 20], 'payload["boost"]', id="json_null"),
            pytest.param([30, "10", 20], 'payload["boost"]', id="numeric_string_is_null"),
            pytest.param([30, 9223372036854775808, 20], 'payload["boost"]', id="int64_overflow_is_null"),
            pytest.param(
                [{"nested": 30}, {}, {"nested": 20}],
                'payload["boost"]["nested"]',
                id="nested_missing_key",
            ),
            pytest.param(
                [{"nested": 30}, {"nested": None}, {"nested": 20}],
                'payload["boost"]["nested"]',
                id="nested_null",
            ),
            pytest.param([[30], [], [20]], 'payload["boost"][0]', id="array_index_out_of_range"),
        ],
    )
    def test_search_pyudf_typed_json_null_paths_preserve_rows(self, values, path):
        """target: nullable JSON projection; method: typed path plus null-aware UDF; expected: exact rows and recovery."""
        with self._pyudf_environment(json_values=values) as resource:
            payload = self._search_payload(resource)
            self._assert_ranked_response(self._search_after_resource_sync(payload))
            projected = copy.deepcopy(payload)
            operation = projected["functionChains"][0]["ops"][0]
            operation["expr"]["args"][1]["column"] = path
            operation["expr"]["params"]["udf_params"] = {"mode": "nullable_input"}
            operation["params"] = {"$input_data_types": [0, 5]}
            response = self._search_once(projected)
            assert response["code"] == 0, response
            assert len(response["data"]) == 3, response
            assert [hit["id"] for hit in response["data"]] == [1, 3, 2]
            assert [hit["distance"] for hit in response["data"]] == pytest.approx([60, 40, -2])
            assert [hit["ts"] for hit in response["data"]] == [10, 30, 20]
            assert all("payload" not in hit for hit in response["data"])
            plain = copy.deepcopy(payload)
            plain.pop("functionChains")
            response = self._search_once(plain)
            assert response["code"] == 0, response
            assert [hit["id"] for hit in response["data"]] == [1, 2, 3]
            assert [hit["distance"] for hit in response["data"]] == pytest.approx([0, 0.0001, 0.0004])
            assert [hit["ts"] for hit in response["data"]] == [10, 20, 30]
            self._assert_ranked_response(self._search_once(payload))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("source", ["json", "dynamic"])
    @pytest.mark.parametrize("shape", ["mixed", "all_null"])
    @pytest.mark.parametrize(
        "input_type,hint,values,expected",
        [
            pytest.param("bool", 1, [True, False, "true"], [True, False, None], id="bool"),
            pytest.param(
                "int64",
                5,
                [-9223372036854775808, 9223372036854775807, 9223372036854775808],
                [-9223372036854775808, 9223372036854775807, None],
                id="int64",
            ),
            pytest.param("double", 11, [1.25, 2, "3.5"], [1.25, 2.0, None], id="double"),
            pytest.param("varchar", 21, ['汉🙂"\\', "", 42], ['汉🙂"\\', "", None], id="varchar"),
        ],
    )
    def test_search_pyudf_json_and_dynamic_types_preserve_values(
        self, source, shape, input_type, hint, values, expected
    ):
        """target: typed paths; method: assert real Arrow values in UDF; expected: exact types, NULLs and recovery."""
        if shape == "all_null":
            values, expected = [None, {}, []], [None, None, None]
        with self._pyudf_environment(json_values=values, dynamic=source == "dynamic") as resource:
            payload = self._search_payload(resource)
            self._assert_ranked_response(self._search_after_resource_sync(payload))
            projected = copy.deepcopy(payload)
            operation = projected["functionChains"][0]["ops"][0]
            operation["expr"]["args"] = [
                {"column": "$score"},
                {"column": '$meta["boost"]' if source == "dynamic" else 'payload["boost"]'},
                {"column": "ts"},
            ]
            operation["expr"]["params"]["udf_params"] = {
                "mode": "typed_probe",
                "input_type": input_type,
                # Keep NULL and exact integer bounds out of the generic parameter codec.
                "expected_json": json.dumps(expected, ensure_ascii=False),
            }
            operation["params"] = {"$input_data_types": [0, hint, 5]}
            response = self._search_once(projected)
            assert response["code"] == 0, response
            assert len(response["data"]) == 3, response
            expected_scores = {
                i + 1: (i + 1) * 10 + (100 if value is not None else 0) for i, value in enumerate(expected)
            }
            expected_ids = sorted(expected_scores, key=lambda pk: -expected_scores[pk])
            assert [hit["id"] for hit in response["data"]] == expected_ids
            assert [hit["distance"] for hit in response["data"]] == pytest.approx(
                [expected_scores[pk] for pk in expected_ids]
            )
            assert [hit["ts"] for hit in response["data"]] == [pk * 10 for pk in expected_ids]
            assert all(not {"payload", "$meta", "boost"}.intersection(hit) for hit in response["data"])
            plain = copy.deepcopy(payload)
            plain.pop("functionChains")
            response = self._search_once(plain)
            assert response["code"] == 0, response
            assert [hit["id"] for hit in response["data"]] == [1, 2, 3]
            assert [hit["distance"] for hit in response["data"]] == pytest.approx([0, 0.0001, 0.0004])
            self._assert_ranked_response(self._search_once(payload))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "failure,message",
        [
            pytest.param("json_root", "complete JSON root input is not supported", id="json_root"),
            pytest.param("unsupported_hint", "unsupported JSON path data type hint", id="unsupported_hint"),
            pytest.param("conflicting_hints", "conflicting data type hints", id="conflicting_hints"),
            pytest.param("scalar_hint", "incompatible with schema field type", id="scalar_hint"),
            pytest.param("system_hint", "system input does not accept data type hint", id="system_hint"),
            pytest.param("bare_dynamic", "must use explicit $meta[...] syntax", id="bare_dynamic"),
            pytest.param("null_parameter", "function param value is nil", id="null_parameter"),
        ],
    )
    def test_search_pyudf_invalid_input_contracts_reject_and_recover(self, failure, message):
        """target: invalid input plans; method: REST rejection plus recovery; expected: code1100 without data."""
        with self._pyudf_environment(json_values=[30, 20, 10], dynamic=failure == "bare_dynamic") as resource:
            payload = self._search_payload(resource)
            self._assert_ranked_response(self._search_after_resource_sync(payload))
            invalid = copy.deepcopy(payload)
            operation = invalid["functionChains"][0]["ops"][0]
            path, hints = 'payload["boost"]', [0, 5]
            if failure == "json_root":
                path = "payload"
            elif failure == "unsupported_hint":
                hints = [0, 10]
            elif failure == "scalar_hint":
                path, hints = "ts", [0, 21]
            elif failure == "system_hint":
                path, hints = "ts", [10, 5]
            elif failure == "bare_dynamic":
                path = "boost"
            operation["expr"]["args"][1]["column"] = path
            if failure == "conflicting_hints":
                operation["expr"]["args"].append({"column": path})
                hints = [0, 5, 11]
            operation["params"] = {"$input_data_types": hints}
            if failure == "null_parameter":
                operation["expr"]["params"]["udf_params"] = {"mode": "valid", "value": None}
            response = self._search_once(invalid)
            assert response["code"] == 1100, response
            assert message in response["message"], response
            assert not response.get("data"), response
            plain = copy.deepcopy(payload)
            plain.pop("functionChains")
            response = self._search_once(plain)
            assert response["code"] == 0, response
            assert [hit["id"] for hit in response["data"]] == [1, 2, 3]
            assert [hit["distance"] for hit in response["data"]] == pytest.approx([0, 0.0001, 0.0004])
            self._assert_ranked_response(self._search_once(payload))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("source", ["json", "dynamic"])
    def test_search_pyudf_json_integer_underflow_insert_rejects_and_recovers(self, source):
        """target: JSON storage lower bound; method: reject insert then search; expected: code1804 and intact UDF."""
        with self._pyudf_environment(json_values=[30, 20, 10], dynamic=source == "dynamic") as resource:
            payload = self._search_payload(resource)
            self._assert_ranked_response(self._search_after_resource_sync(payload))
            invalid = {"id": 4, "ts": 40, "vector": [0.03, 0.0]}
            if source == "dynamic":
                invalid["boost"] = -9223372036854775809
            else:
                invalid["payload"] = {"boost": -9223372036854775809}
            response = self.vector_client.vector_insert({"collectionName": self.name, "data": [invalid]})
            assert response["code"] == 1804, response
            assert "exceeds the 64-bit range supported by the JSON engine" in response["message"], response
            assert not response.get("data"), response
            plain = copy.deepcopy(payload)
            plain.pop("functionChains")
            plain["limit"] = 10
            response = self._search_once(plain)
            assert response["code"] == 0, response
            assert [hit["id"] for hit in response["data"]] == [1, 2, 3]
            assert [hit["ts"] for hit in response["data"]] == [10, 20, 30]
            assert [hit["distance"] for hit in response["data"]] == pytest.approx([0, 0.0001, 0.0004])
            self._assert_ranked_response(self._search_once(payload))

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("operation", ["search", "add", "remove", "list"])
    def test_pyudf_unauthenticated_calls_are_rejected(self, operation):
        """
        preconditions: function.pyUDF.enabled=true, common.fileResource.mode.proxy=sync,
        MinIO wheel storage, common.security.authorizationEnabled=true, and a valid root --token.
        target: reject unauthenticated use and management without altering an existing UDF
        method: send an invalid token against a warmed resource, then inspect it as the owner
        expected: REST code 1800, no partial data, and the authorized resource/result remain intact
        """
        with self._pyudf_environment() as resource:
            payload = self._search_payload(resource)
            self._assert_ranked_response(self._search_after_resource_sync(payload))
            invalid = FileResourceClient(self.endpoint, self.invalid_api_key)
            if operation == "search":
                response = self._search_once(payload, client=VectorClient(self.endpoint, self.invalid_api_key))
            elif operation == "add":
                response = invalid.file_resource_add({"name": resource, "path": f"pyudf/{resource}.whl"})
            elif operation == "remove":
                response = invalid.file_resource_remove({"name": resource})
            else:
                response = invalid.file_resource_list()
            assert response["code"] == 1800, response
            assert not response.get("data"), response
            listed = self.file_resource_client.file_resource_list()
            assert listed["code"] == 0, listed
            assert any(item["name"] == resource for item in listed["data"]), listed
            self._assert_ranked_response(self._search_once(payload))
