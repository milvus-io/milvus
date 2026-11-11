"""PyUDF search and resource lifecycles with independent score oracles."""

import hashlib
import io
import ipaddress
import json
import math
import os
import shutil
import socket
import subprocess
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import pyarrow.lib
import pytest
import requests
from base.client_v2_pyudf_base import PyUDFClientBase
from common import common_func as cf
from common.common_type import CaseLabel
from common.minio_comm import copy_files_to_minio
from common.pyudf_helpers import assert_pyudf_deadline
from pymilvus import (
    AnnSearchRequest,
    DataType,
    Function,
    FunctionChain,
    FunctionChainStage,
    FunctionType,
    MilvusException,
)
from pymilvus.bulk_writer import bulk_import, get_import_progress
from pymilvus.client.prepare import Prepare
from pymilvus.function_chain import col, fn
from pymilvus.function_chain.chain import FunctionChainExpr
from pymilvus.grpc_gen import common_pb2

MODULE = """import math
import os
import threading
import pyarrow as pa

class UDF:
    def __init__(self, context):
        self.context = context
        self.calls = 0
        self.late_calls = 0
        self.lock = threading.Lock()
    def transform_query(self, params, columns):
        with self.lock:
            self.calls += 1
            call = self.calls
        self.context.logger.warning("PYUDF_R2_CALL resource=%s pid=%s call=%s mode=%s rows=%s",
            self.context.resource_name, os.getpid(), call, params.get("mode", "normal"), len(columns[0]))
        invalid_values = {"null": None, "nan": float("nan"), "positive_inf": float("inf"), "negative_inf": -float("inf"), "overflow": 1e40}
        mode = params.get("mode")
        if mode == "five_columns":
            assert len(columns) == 5
            assert all(value.type == pa.int64() for value in columns)
            assert columns[0] is columns[2]
            assert columns[1] is columns[4]
            assert columns[1] is not columns[3]
            for a, b, repeated_a, c, repeated_b in zip(*(value.to_pylist() for value in columns)):
                assert a == repeated_a and b == repeated_b == c == 10 * a
            return [pa.array([210 * value.as_py() for value in columns[0]], type=pa.float32())]
        if mode == "later_batch_error":
            self.late_calls += 1
            if self.late_calls == 2:
                raise ValueError("qa_synthetic_later_batch_error")
        if mode == "return_none":
            return None
        if mode == "return_scalar":
            return 42
        if mode == "zero_outputs":
            return []
        if mode in invalid_values:
            return [pa.array([invalid_values[mode]] * len(columns[0]), type=pa.float64())]
        if mode == "repair":
            for value in columns[0].to_pylist():
                invalid = params["invalid"]
                valid = value is None if invalid == "null" else math.isnan(value) if invalid == "nan" else value == -float("inf") if invalid == "negative_inf" else value == float("inf")
                if not valid:
                    raise AssertionError("invalid intermediate value was lost")
        if params.get("mode") == "count":
            values = [call] * len(columns[0])
        elif params.get("mode") == "identity":
            values = columns[0].to_pylist()
        elif params.get("mode") == "fusion":
            values = [100 * score.as_py() + 2 * ts.as_py() for score, ts in zip(columns[0], columns[1])]
        else:
            values = [v.as_py() * 2 for v in columns[-1]]
        return [pa.array(values, type=pa.float32())]
def create_udf(context):
    return UDF(context)
"""

DRAIN_MODULE = """import os
import threading
import time
import pyarrow as pa

class UDF:
    def __init__(self, context):
        self.context = context
        self.lock = threading.Lock()
        self.active = 0
        self.finished = 0
    def transform_query(self, params, columns):
        mode = params.get("mode", "normal")
        if mode == "slow":
            with self.lock:
                self.active += 1
            self.context.logger.warning("PYUDF_R2_DRAIN start resource=%s pid=%s", self.context.resource_name, os.getpid())
            try:
                time.sleep(3)
            finally:
                with self.lock:
                    self.active -= 1
                    self.finished += 1
                self.context.logger.warning("PYUDF_R2_DRAIN end resource=%s pid=%s", self.context.resource_name, os.getpid())
        if mode == "state":
            with self.lock:
                value = self.active * 100 + self.finished
            result = [value] * len(columns[0])
        elif mode == "pid":
            result = [os.getpid()] * len(columns[0])
        else:
            result = [v.as_py() * 2 for v in columns[-1]]
        return [pa.array(result, type=pa.float32())]
def create_udf(context):
    return UDF(context)
"""

SCALAR_FIELDS = ["ts", "flag", "i8", "i16", "i32", "i64", "f32", "f64", "text", "optional"]


def _schema_row(number, string_pk=False, primary=None):
    primary = number if primary is None else primary
    return {
        "id": f"id_{primary:05d}" if string_pk else primary,
        "ts": number,
        "flag": number % 2 == 0,
        "i8": number % 127,
        "i16": number % 32767,
        "i32": number * 3,
        "i64": number * 5,
        "f32": number * 0.25,
        "f64": number * 0.125,
        "text": f"row_{number}",
        "optional": None if number % 3 == 0 else number,
        "payload": {"boost": number * 7},
        "dynamic_boost": number * 11,
        "array": [number, number + 1],
        "vector": [float(number % 17), float(number % 19)],
    }


SCHEMA_MODULE = """import os
import pyarrow as pa
class UDF:
    def __init__(self, context):
        self.context = context
    def transform_query(self, params, columns):
        if params.get("mode") != "schema":
            return [pa.array([v.as_py() * 2 for v in columns[-1]], type=pa.float32())]
        types = [pa.int64(), pa.bool_(), pa.int8(), pa.int16(), pa.int32(), pa.int64(),
                 pa.float32(), pa.float64(), pa.string(), pa.int64()]
        if [c.type for c in columns[:10]] != types:
            raise AssertionError("scalar Arrow types changed")
        for values in zip(*(c.to_pylist() for c in columns)):
            n = values[0]
            expected = (n, n % 2 == 0, n % 127, n % 32767, n * 3, n * 5,
                        n / 4, n / 8, "row_" + str(n), None if n % 3 == 0 else n)
            if values[:10] != expected:
                raise AssertionError(("scalar alignment", values[:10], expected))
            if len(values) == 11 and values[10] != (99 if n >= 20000 else None):
                raise AssertionError("added field old/new row mismatch")
        self.context.logger.warning("PYUDF_R2_SCHEMA resource=%s pid=%s rows=%s columns=%s",
            self.context.resource_name, os.getpid(), len(columns[0]), len(columns))
        return [pa.array([v.as_py() * 2 for v in columns[0]], type=pa.float32())]
def create_udf(context):
    return UDF(context)
"""


def _vector_data(kind, count=10000):
    if kind == DataType.BINARY_VECTOR:
        vectors = [index.to_bytes(2, "little") for index in range(count)]
        return vectors, (1234).to_bytes(2, "little"), "BIN_FLAT", {"dim": 16}
    if kind == DataType.SPARSE_FLOAT_VECTOR:
        vectors = [{index % 97: float(index % 5 + 1), 100 + index // 97: 0.5} for index in range(count)]
        return vectors, {3: 2.0, 104: 1.0}, "SPARSE_INVERTED_INDEX", {}
    # Exact binary fractions survive FP16/BF16; int8 gets exact integer values.
    raw = np.array([[index % 97 + 1, index // 97 + 1] + [1] * 14 for index in range(count)], dtype=np.float32)
    if kind != DataType.INT8_VECTOR:
        raw /= 128
    query = raw[1234].copy()

    def encode(value):
        if kind == DataType.FLOAT_VECTOR:
            return value.tolist()
        if kind == DataType.FLOAT16_VECTOR:
            return value.astype(np.float16).tobytes()
        if kind == DataType.BFLOAT16_VECTOR:
            return (value.view(np.uint32) >> 16).astype(np.uint16).tobytes()
        return value.astype(np.int8).tobytes()

    return [encode(value) for value in raw], encode(query), "HNSW", {"dim": 16}


def _metric_score(kind, metric, vector, query):
    if kind == DataType.BINARY_VECTOR:
        first, second = int.from_bytes(vector, "little"), int.from_bytes(query, "little")
        if metric == "HAMMING":
            return (first ^ second).bit_count()
        union = (first | second).bit_count()
        return 1 - (first & second).bit_count() / union if union else 0
    if kind == DataType.SPARSE_FLOAT_VECTOR:
        return sum(value * query.get(key, 0) for key, value in vector.items())

    def decode(value):
        if kind == DataType.FLOAT_VECTOR:
            return np.asarray(value, dtype=np.float64)
        if kind == DataType.BFLOAT16_VECTOR:
            return (np.frombuffer(value, dtype=np.uint16).astype(np.uint32) << 16).view(np.float32).astype(np.float64)
        return np.frombuffer(value, dtype=np.float16 if kind == DataType.FLOAT16_VECTOR else np.int8).astype(np.float64)

    first, second = decode(vector), decode(query)
    if metric == "L2":
        return float(np.sum((first - second) ** 2))
    dot = float(np.dot(first, second))
    return dot if metric == "IP" else dot / math.sqrt(float(np.dot(first, first) * np.dot(second, second)))


LEGACY_SDK_PROBE = """import json
import sys

import pymilvus


def main():
    config = json.load(sys.stdin)
    assert pymilvus.__version__ == "2.6.6", pymilvus.__version__
    assert not hasattr(pymilvus, "FunctionChain")
    assert not hasattr(pymilvus, "FunctionChainStage")
    client = pymilvus.MilvusClient(uri=config["uri"], token=config["credential"])
    try:
        version = client.get_server_version(detail=True)
        # SDK2.6.6 ignores detail=True and exposes only the version string.
        # The current SDK independently binds this same endpoint to its commit.
        assert isinstance(version, str) and version == config["server_version"], version
        result = client.search(
            config["collection"],
            data=[[0.0, 0.0]],
            anns_field="vector",
            limit=3,
            output_fields=["ts"],
            search_params={"metric_type": "L2"},
            timeout=30,
        )
        assert [hit["id"] for hit in result[0]] == [1, 2, 3], result
        assert [hit["entity"]["ts"] for hit in result[0]] == [10, 20, 30], result
        expected = [0, 0.0001, 0.0004]
        for hit, distance in zip(result[0], expected, strict=True):
            assert abs(hit["distance"] - distance) <= 1e-7, result
        print(
            json.dumps(
                {
                    "sdk": pymilvus.__version__,
                    "server": version,
                    "ordinary_search": "PASS",
                    "function_chain_api": "ABSENT_LOCAL_SDK",
                    "server_function_chain_request_attempted": False,
                    "detailed_server_version_api": "ABSENT_LOCAL_SDK",
                    "hits": [
                        [{"id": hit["id"], "distance": hit["distance"], "ts": hit["entity"]["ts"]} for hit in hits]
                        for hits in result
                    ],
                }
            )
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()
"""


INDEX_TUPLES = (
    [
        pytest.param(index, mmap, None, None, False, id=f"{index}-mmap-{mmap}")
        for index in ("FLAT", "HNSW", "IVF_FLAT", "AUTOINDEX")
        for mmap in (None, True, False)
    ]
    + [
        pytest.param(
            "AUTOINDEX",
            (None, True, False)[(kind_id + policy_id) % 3],
            kind,
            policy,
            False,
            id=f"warmup-{kind}-{policy}",
        )
        for kind_id, kind in enumerate(("scalarField", "scalarIndex", "vectorField", "vectorIndex"))
        for policy_id, policy in enumerate((None, "sync", "async", "disable"))
    ]
    + [
        pytest.param("AUTOINDEX", mmap, "scalarField", "sync", True, id=f"priority-mmap-{mmap}")
        for mmap in (True, False)
    ]
)


class TestMilvusClientPyUDFSearchLifecycle(PyUDFClientBase):
    """Search, indexing and snapshot lifecycles with owned resources."""

    skip_global_role_cleanup = True
    dim, vector_field, scalar_field = 2, "vector", "ts"

    @pytest.mark.tags(CaseLabel.L1)
    def test_pyudf_equal_valued_distinct_input_columns(self, tmp_path, minio_host, minio_bucket, record_property):
        """Preserve [A,B,A,C,B] identity while B and C have equal typed values."""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, MODULE) as (client, name, resource):
            self.release_collection(client, name)
            self.add_collection_field(client, name, "same_ts", DataType.INT64, nullable=True)
            rows = [{"id": pk, "ts": pk * 10, "same_ts": pk * 10, "vector": [(pk - 1) * 0.01, 0.0]} for pk in (1, 2, 3)]
            self.upsert(client, name, rows)
            self.flush(client, name)
            self.load_collection(client, name)
            chain = (
                FunctionChain(FunctionChainStage.L2_RERANK)
                .map("$score", self._expr(resource, "id", "ts", "id", "same_ts", "ts", mode="five_columns"))
                .sort(col("$score"), desc=True, tie_break_col=col("$id"))
            )
            result = self._search_with_synced_resource(client, name, chain, data=[[0.0, 0.0], [0.1, 0.1]])
            assert len(result) == 2
            for hits in result:
                assert [hit["id"] for hit in hits] == [3, 2, 1]
                assert [hit["distance"] for hit in hits] == [630, 420, 210]
            record_property("input_identity", "[A,B,A,C,B]; B/C int64 equal-valued but distinct; NQ2")
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("origin", ["module_import", "later_query_batch"])
    def test_pyudf_import_and_later_batch_exception_origin(
        self, tmp_path, minio_host, minio_bucket, origin, record_property
    ):
        """Reject import and later-query user failures atomically, then recover."""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, MODULE) as (client, name, resource):
            self._assert_pyudf_recovery(client, name, resource)
            if origin == "module_import":
                module = 'raise RuntimeError("qa_synthetic_module_import_error")\n' + MODULE
                with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, module) as invalid:
                    bad_client, bad_name, bad_resource = invalid
                    with pytest.raises(MilvusException) as captured:
                        self._search_with_synced_resource(bad_client, bad_name, self._pyudf_chain(bad_resource))
                    assert captured.value.code == 2400
                    assert "cannot import PyUDF entry module" in str(captured.value)
                    assert "qa_synthetic_module_import_error" in str(captured.value)
            else:
                with pytest.raises(MilvusException) as captured:
                    self._search_with_synced_resource(
                        client,
                        name,
                        self._pyudf_chain(resource, {"mode": "later_batch_error"}),
                        data=[[0.0, 0.0], [0.1, 0.1]],
                    )
                assert captured.value.code == 2400
                assert "query=1: UDF execution failed" in str(captured.value)
                assert "qa_synthetic_later_batch_error" in str(captured.value)
            assert "UDF_FAILED" in str(captured.value)
            record_property("exception_origin", origin)
            record_property("atomic_response", "Search raised; no partial Search result returned")
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L3, "pyudf")
    def test_pyudf_actual_pod_worker_network_boundary(self, request, record_property):
        """Probe a recorded target Pod IP from the external E2E runner Pod."""
        endpoint = request.config.getoption("--pyudf-management-endpoint")
        target = urlparse(endpoint).hostname
        ipaddress.ip_address(target)
        observations = {}
        for port in (19530, 19090):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                probe.settimeout(3)
                observations[str(port)] = probe.connect_ex((target, port))
        assert observations["19530"] == 0, observations
        assert observations["19090"] != 0, observations
        record_property(
            "pod_peer_probe",
            json.dumps(
                {
                    "target_pod_ip": target,
                    "runner_hostname": socket.gethostname(),
                    "runner_ip": socket.gethostbyname(socket.gethostname()),
                    "tcp_connect_codes": observations,
                    "identity_binding": "Join target IP and runner hostname with coordinator-recorded Pod UIDs",
                }
            ),
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("mode", ["return_none", "return_scalar", "zero_outputs"])
    def test_pyudf_missing_output_contracts(self, tmp_path, minio_host, minio_bucket, mode):
        """Reject missing or non-sequence outputs at their actual Search boundary."""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, MODULE) as environment:
            client, collection, resource = environment
            self._assert_pyudf_recovery(client, collection, resource)
            # Zero-column IPC decodes to a nil Go slice, rejected by the
            # expression validator before MapOp checks its output count.
            message = (
                "py_udf: client returned nil outputs"
                if mode == "zero_outputs"
                else "transform_query must return a sequence of pyarrow.Array"
            )
            self._assert_pyudf_error(
                client, collection, self._pyudf_chain(resource, {"mode": mode}), message, code=2400
            )
            self._assert_pyudf_recovery(client, collection, resource)

    @staticmethod
    def _expr(resource, *inputs, **params):
        return FunctionChainExpr(
            "py_udf",
            args=tuple(col(value) for value in inputs),
            params={"resource_name": resource, "udf_params": params},
        )

    @staticmethod
    def _limit(request, axis, record_property):
        key = "quotaAndLimits.limits.nq" if axis == "nq" else "quotaAndLimits.limits.topK"
        endpoint = request.config.getoption("--pyudf-management-endpoint")
        assert endpoint
        response = requests.get(endpoint.rstrip("/") + "/management/config/get", params={"keys": key}, timeout=10)
        response.raise_for_status()
        entry = response.json()["configs"][0]
        assert entry["key"] == key, entry
        # Management returns configured sources, not absent ParamItem defaults.
        # quota_param.go at the fixed baseline specifies 16384 for both axes;
        # actual boundary requests independently check that effective default.
        if "value" not in entry:
            assert entry["error"] == f"{key}: key not found", entry
            record_property("limit_source", json.dumps({"key": key, "override": "absent", "source_default": 16384}))
            return 16384
        record_property("limit_source", json.dumps(entry))
        return int(entry["value"])

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "kind,metric",
        [
            *[
                (kind, metric)
                for kind in (
                    DataType.FLOAT_VECTOR,
                    DataType.FLOAT16_VECTOR,
                    DataType.BFLOAT16_VECTOR,
                    DataType.INT8_VECTOR,
                )
                for metric in ("L2", "IP", "COSINE")
            ],
            (DataType.BINARY_VECTOR, "HAMMING"),
            (DataType.BINARY_VECTOR, "JACCARD"),
            (DataType.SPARSE_FLOAT_VECTOR, "IP"),
        ],
    )
    def test_pyudf_d2_vector_index_matrix(
        self, tmp_path, minio_host, minio_bucket, request, kind, metric, record_property
    ):
        """target: M2/M3; method: real indexed D2 and decoded-vector oracle; expected: metric and scalar alignment through two reloads."""
        vectors, query, requested_index, dimensions = _vector_data(kind)
        expected_autoindex = request.config.getoption("--pyudf-expected-autoindex")
        if expected_autoindex:
            actual = json.loads(self._pyudf_management_config(request, "autoIndex.params.build"))
            assert actual["index_type"] == expected_autoindex, actual
            requested_index = "AUTOINDEX"
        # The cap profile trades ordinary floating-vector score precision for
        # efficiency. Keep exact PyUDF arithmetic and other profiles unchanged.
        score_tolerance = {"rel": 1e-5, "abs": 1e-6}
        if expected_autoindex == "DISKANN" and kind in (
            DataType.FLOAT_VECTOR,
            DataType.FLOAT16_VECTOR,
            DataType.BFLOAT16_VECTOR,
        ):
            score_tolerance = {"rel": 1e-3, "abs": 1e-5}
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, MODULE) as (client, control, resource):
            self._assert_pyudf_recovery(client, control, resource)
            with ExitStack() as cleanup:
                name = cf.gen_unique_str("pyudf_r2_vectors")
                schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
                schema.add_field("id", DataType.INT64, is_primary=True)
                schema.add_field("ts", DataType.INT64)
                schema.add_field("vector", kind, **dimensions)
                self.create_collection(client, name, schema=schema, consistency_level="Strong")
                cleanup.callback(self.drop_collection, client, name)
                rows = [{"id": i, "ts": i * 3 + 7, "vector": value} for i, value in enumerate(vectors)]
                self.insert(client, name, rows[:5000])
                self.insert(client, name, rows[5000:])
                self.flush(client, name)
                indexes = self.prepare_index_params(client)[0]
                indexes.add_index(
                    "vector",
                    index_type=requested_index,
                    metric_type=metric,
                    params={"M": 16, "efConstruction": 128} if requested_index == "HNSW" else {},
                )
                self.create_index(client, name, indexes)
                info = client.describe_index(name, "vector")
                assert info["state"] == "Finished" and info["indexed_rows"] == 10000, info
                metric_failures = []
                for reload_id in range(3):
                    start = time.time()
                    self.load_collection(client, name)
                    segments = [
                        {"id": item.segment_id, "rows": item.num_rows, "index": item.index_id}
                        for item in client.list_loaded_segments(name)
                    ]
                    assert sum(item["rows"] for item in segments) == 10000, segments
                    for temperature in ("cold", "hot"):
                        options = {
                            "data": [query],
                            "anns_field": "vector",
                            "limit": 64,
                            "output_fields": ["ts"],
                            "search_params": {"metric_type": metric, "params": {"ef": 128}},
                            "timeout": 30,
                        }
                        baseline = client.search(name, **options)[0]
                        assert len(baseline) == 64
                        mismatches = []
                        for hit in baseline:
                            exact = _metric_score(kind, metric, vectors[hit["id"]], query)
                            if hit["distance"] != pytest.approx(exact, **score_tolerance):
                                mismatches.append({"id": hit["id"], "actual": hit["distance"], "exact": exact})
                        metric_failures.extend(mismatches)
                        result = client.search(name, function_chains=self._pyudf_chain(resource), **options)[0]
                        expected = sorted((hit["id"] for hit in baseline), reverse=True)
                        assert [hit["id"] for hit in result] == expected
                        assert [hit["distance"] for hit in result] == [(pk * 3 + 7) * 2 for pk in expected]
                        assert [self._hit_field(hit, "ts") for hit in result] == [pk * 3 + 7 for pk in expected]
                        record_property(
                            f"vector_{reload_id}_{temperature}",
                            json.dumps(
                                {
                                    "collection": name,
                                    "resource": resource,
                                    "kind": kind.name,
                                    "metric": metric,
                                    "requested_index": requested_index,
                                    "index": info,
                                    "segments": segments,
                                    "start_unix": start,
                                    "end_unix": time.time(),
                                    "candidates": [hit["id"] for hit in baseline],
                                    "metric_mismatches": mismatches,
                                    "score_tolerance": score_tolerance,
                                    "pyudf_field_alignment": "PASS",
                                }
                            ),
                        )
                    self.release_collection(client, name)
                self.load_collection(client, name)
                all_rows = client.query(name, filter="", output_fields=["id", "ts"], limit=11000)
                assert len(all_rows) == 10000 and {item["id"]: item["ts"] for item in all_rows} == {
                    i: i * 3 + 7 for i in range(10000)
                }
            self._assert_pyudf_recovery(client, control, resource)
            # Enforce the profile's ordinary-score tolerance after independently
            # validating exact PyUDF arithmetic and alignment across every reload.
            assert not metric_failures, metric_failures

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid", ["null", "nan", "positive_inf", "negative_inf", "overflow"])
    @pytest.mark.parametrize("flow", ["final", "ascending", "group", "range_offset"])
    def test_pyudf_sdk_nonfinite_query_combinations(
        self, tmp_path, minio_host, minio_bucket, invalid, flow, record_property
    ):
        """target: Q01 SDK; method: nonfinite output then query-specific repair; expected: exact errors or aligned results."""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, MODULE) as (client, name, resource):
            self._assert_pyudf_recovery(client, name, resource)
            chain = FunctionChain(FunctionChainStage.L2_RERANK).map(
                "$score", self._expr(resource, "$score", "ts", mode=invalid)
            )
            options = {}
            expected = [3, 2, 1]
            if flow != "final":
                chain.map("$score", self._expr(resource, "$score", "ts", mode="repair", invalid=invalid))
            chain.sort(col("$score"), desc=flow != "ascending", tie_break_col=col("$id"))
            if flow == "ascending":
                options["filter"] = "id >= 2"
                expected = [2, 3]
            elif flow == "group":
                options["group_by_field"] = "ts"
            elif flow == "range_offset":
                options.update(
                    data=[[0.009, 0.0]],
                    search_params={
                        "metric_type": "L2",
                        "params": {"radius": 0.00015, "range_filter": 0.0},
                    },
                    offset=1,
                    limit=2,
                )
                expected = [3, 1]
            if flow == "final":
                self._assert_pyudf_error(client, name, chain, "$score", code=2400)
            else:
                result = self._search_with_synced_resource(client, name, chain, **options)[0]
                assert [hit["id"] for hit in result] == expected
                assert [hit["distance"] for hit in result] == [pk * 20 for pk in expected]
                assert [self._hit_field(hit, "ts") for hit in result] == [pk * 10 for pk in expected]
                record_property("nonfinite_combination", json.dumps({"invalid": invalid, "flow": flow, "hits": result}))
            self._assert_pyudf_recovery(client, name, resource)
            assert resource in {item.name for item in client.list_file_resources()}

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index,mmap,kind,policy,priority", INDEX_TUPLES)
    def test_pyudf_index_mmap_warmup_paths(
        self, tmp_path, minio_host, minio_bucket, record_property, index, mmap, kind, policy, priority
    ):
        """Verify configured loading and reloading, each with cold/hot searches and retained vectors."""
        rows = [{"id": i, "ts": i * 3 + 7, "vector": [i % 97 / 97, i // 97 / 103]} for i in range(10000)]
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, MODULE, rows=rows, index_type=index) as (
            client,
            name,
            resource,
        ):
            record_property("warmup_server_identity", json.dumps(client.get_server_version(detail=True)))
            self.release_collection(client, name)
            scalars = self.prepare_index_params(client)[0]
            scalars.add_index("ts", index_type="INVERTED")
            self.create_index(client, name, scalars)
            properties = {}
            if mmap is not None:
                properties["mmap.enabled"] = str(mmap).lower()
                for field in ("vector", "ts"):
                    self.alter_index_properties(client, name, field, properties={"mmap.enabled": str(mmap).lower()})
            if kind and policy:
                properties["warmup." + kind] = policy
            if priority:
                properties.update(
                    {"warmup.scalarIndex": "sync", "warmup.vectorField": "disable", "warmup.vectorIndex": "disable"}
                )
                self.alter_collection_field(client, name, "ts", field_params={"warmup": "disable"})
                self.alter_collection_field(client, name, "vector", field_params={"warmup": "sync"})
                self.alter_index_properties(client, name, "ts", properties={"warmup": "async"})
                self.alter_index_properties(client, name, "vector", properties={"warmup": "sync"})
            if properties:
                self.alter_collection_properties(client, name, properties=properties)
            described = client.describe_collection(name)
            for key, value in properties.items():
                assert str(described["properties"][key]).lower() == str(value).lower(), described
            if kind and policy is None:
                assert "warmup." + kind not in described["properties"]
            if priority:
                assert cf.get_field_warmup(described, "ts") == "disable"
                assert cf.get_field_warmup(described, "vector") == "sync"
            # The vector index was created on an empty collection. Finished
            # state alone does not prove that later flushed rows are indexed.
            deadline = time.monotonic() + 120
            readiness = []
            while True:
                indices = {field: client.describe_index(name, field) for field in ("vector", "ts")}
                readiness.append({"unix": time.time(), "indices": indices})
                record_property("index_readiness", json.dumps(readiness[-1]))
                assert all(info["state"] != "Failed" for info in indices.values()), indices
                if all(info["state"] == "Finished" and info["indexed_rows"] == 10000 for info in indices.values()):
                    break
                assert time.monotonic() < deadline, readiness
                time.sleep(1)
            for field, info in indices.items():
                assert info["state"] == "Finished" and info["indexed_rows"] == 10000, (field, info)
                if mmap is not None:
                    assert str(info["mmap.enabled"]).lower() == str(mmap).lower(), info
            if priority:
                assert cf.get_index_warmup(indices["ts"]) == "async"
                assert cf.get_index_warmup(indices["vector"]) == "sync"
            observations = []
            for reload_id in range(2):
                started = time.time()
                self.load_collection(client, name)
                load_completed = time.time()
                segments = [
                    {
                        "segment": segment.segment_id,
                        "rows": segment.num_rows,
                        "index": segment.index_id,
                        "state": segment.state,
                    }
                    for segment in client.list_loaded_segments(name)
                ]
                assert sum(segment["rows"] for segment in segments) == 10000, segments
                first_query = time.time()
                for temperature in ("cold", "hot"):
                    for query in ([0.13, 0.41], [0.7, 0.2], [0.99, 0.99]):
                        options = {
                            "data": [query],
                            "anns_field": "vector",
                            "limit": 64,
                            # Request raw vectors so disabled field warmup
                            # also exercises demand reads.
                            "output_fields": ["ts", "vector"],
                            "search_params": {"metric_type": "L2", "params": {"ef": 128, "nprobe": 128}},
                            "filter": "ts >= 7",
                            "timeout": 30,
                        }
                        plain = client.search(name, **options)[0]
                        result = client.search(name, function_chains=self._pyudf_chain(resource), **options)[0]
                        expected = sorted((hit["id"] for hit in plain), reverse=True)
                        assert len(expected) == 64 and [hit["id"] for hit in result] == expected
                        assert [hit["distance"] for hit in result] == [(pk * 3 + 7) * 2 for pk in expected]
                        assert [self._hit_field(hit, "ts") for hit in result] == [pk * 3 + 7 for pk in expected]
                        for hits in (plain, result):
                            for hit in hits:
                                assert self._hit_field(hit, "vector") == pytest.approx(
                                    rows[hit["id"]]["vector"], abs=1e-7
                                )
                        observations.append(
                            {"reload": reload_id, "temperature": temperature, "query": query, "candidate_ids": expected}
                        )
                record_property(
                    f"index_reload_{reload_id}",
                    json.dumps(
                        {
                            "collection": name,
                            "resource": resource,
                            "started_unix": started,
                            "load_completed_unix": load_completed,
                            "first_query_unix": first_query,
                            "finished_unix": time.time(),
                            "segments": segments,
                        }
                    ),
                )
                if reload_id == 0:
                    self.release_collection(client, name)
            stored = client.query(name, filter="", output_fields=["id", "ts", "vector"], limit=11000)
            assert len(stored) == 10000
            for item in stored:
                original = rows[item["id"]]
                assert item["ts"] == original["ts"]
                assert item["vector"] == pytest.approx(original["vector"], abs=1e-7)
            record_property(
                "index_tuple",
                json.dumps(
                    {
                        "collection": name,
                        "resource": resource,
                        "requested_index": index,
                        "mmap": mmap,
                        "kind": kind,
                        "policy": policy,
                        "priority": priority,
                        "schema": described,
                        "indices": indices,
                        "observations": observations,
                    }
                ),
            )

    @pytest.mark.tags(CaseLabel.L3, "pyudf")
    @pytest.mark.parametrize("kind", ["scalarField", "vectorField"])
    @pytest.mark.parametrize("policy", [None, "sync", "async", "disable"])
    def test_pyudf_field_data_warmup_observation(
        self, tmp_path, minio_host, minio_bucket, request, kind, policy, record_property
    ):
        """target: Q06 raw fields; method: exclusive pre-query load observations; expected: exact UDF and raw values."""
        # This opt-in package needs an exclusive instance with startup config
        # retaining raw columns, including columns whose index contains data.
        assert self._pyudf_management_config(request, "queryNode.preferFieldDataWhenIndexHasRawData") == "true"
        endpoint = request.config.getoption("--pyudf-management-endpoint").rstrip("/")

        def metrics():
            response = requests.get(endpoint + "/metrics", timeout=10)
            response.raise_for_status()
            return [
                line
                for line in response.text.splitlines()
                if not line.startswith("#") and any(word in line.lower() for word in ("cache", "prefetch", "warmup"))
            ]

        rows = [{"id": i, "ts": i * 3 + 7, "vector": [i % 97 / 128, i // 97 / 128]} for i in range(10000)]
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, MODULE, rows=rows, index_type="HNSW") as (
            client,
            name,
            resource,
        ):
            warm = self._search_with_synced_resource(client, name, self._pyudf_chain(resource), limit=3)[0]
            assert len(warm) == 3
            assert all(hit["distance"] == (hit["id"] * 3 + 7) * 2 for hit in warm)
            self.release_collection(client, name)
            properties = {
                "warmup.scalarIndex": "disable",
                "warmup.vectorIndex": "disable",
                "warmup." + ("vectorField" if kind == "scalarField" else "scalarField"): "disable",
            }
            if policy is not None:
                properties["warmup." + kind] = policy
            self.alter_collection_properties(client, name, properties=properties)
            description = client.describe_collection(name)
            assert all(str(description["properties"][key]).lower() == value for key, value in properties.items())
            for reload_id in range(3):
                before_metrics = metrics()
                started = time.time()
                self.load_collection(client, name)
                loaded = time.time()
                after_load_metrics = metrics()
                time.sleep(0.5)
                settled_metrics = metrics()
                first_query = time.time()
                record_property(
                    f"field_warmup_load_{reload_id}",
                    json.dumps(
                        {
                            "collection": name,
                            "kind": kind,
                            "policy": policy,
                            "load_started_unix": started,
                            "load_completed_unix": loaded,
                            "first_query_unix": first_query,
                            "before_metrics": before_metrics,
                            "loaded_metrics": after_load_metrics,
                            "settled_metrics": settled_metrics,
                        }
                    ),
                )
                options = dict(
                    data=[[0.1, 0.1]],
                    anns_field="vector",
                    limit=64,
                    output_fields=["ts", "vector"],
                    search_params={"metric_type": "L2", "params": {"ef": 128}},
                )
                baseline = client.search(name, **options)[0]
                result = client.search(name, function_chains=self._pyudf_chain(resource), **options)[0]
                expected = sorted((hit["id"] for hit in baseline), reverse=True)
                assert len(expected) == len(set(expected)) == 64
                assert [hit["id"] for hit in result] == expected
                assert [hit["distance"] for hit in result] == [(pk * 3 + 7) * 2 for pk in expected]
                for hit in result:
                    assert self._hit_field(hit, "ts") == rows[hit["id"]]["ts"]
                    assert self._hit_field(hit, "vector") == pytest.approx(rows[hit["id"]]["vector"], abs=1e-7)
                record_property(
                    f"field_warmup_{reload_id}",
                    json.dumps(
                        {
                            "collection": name,
                            "resource": resource,
                            "kind": kind,
                            "policy": policy,
                            "properties": properties,
                            "load_started_unix": started,
                            "load_completed_unix": loaded,
                            "first_query_unix": first_query,
                            "segments": [
                                {"id": segment.segment_id, "rows": segment.num_rows}
                                for segment in client.list_loaded_segments(name)
                            ],
                            "external": "PASS",
                            "internal": "Requires translator-key logs and pre-query metric interpretation",
                        }
                    ),
                )
                self.release_collection(client, name)

    @pytest.mark.tags(CaseLabel.L3, "pyudf")
    def test_pyudf_large_topk_collection_support(self, tmp_path, minio_host, minio_bucket, record_property):
        """target: large_topk interaction; method: 16385 real candidates; expected: ordinary and PyUDF requests preserve every row."""
        count = 16385
        rows = [{"id": i, "ts": i, "vector": [float(i), 0.0]} for i in range(1, count + 1)]
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None, rows=rows) as (
            client,
            name,
            resource,
        ):
            self.release_collection(client, name)
            self.drop_index(client, name, "vector")
            self.alter_collection_properties(client, name, properties={"query_mode": "large_topk"})
            indexes = self.prepare_index_params(client)[0]
            indexes.add_index("vector", index_type="FLAT", metric_type="L2")
            self.create_index(client, name, indexes)
            self.load_collection(client, name)
            assert client.describe_collection(name)["properties"]["query_mode"] == "large_topk"
            for limit in (100, count):
                plain = client.search(
                    name,
                    data=[[0.0, 0.0]],
                    anns_field="vector",
                    limit=limit,
                    search_params={"metric_type": "L2"},
                    output_fields=["ts"],
                    timeout=30,
                )[0]
                assert [hit["id"] for hit in plain] == list(range(1, limit + 1))
                result = self._search_with_synced_resource(client, name, self._pyudf_chain(resource), limit=limit)[0]
                expected = list(range(limit, 0, -1))
                assert [hit["id"] for hit in result] == expected
                assert [hit["distance"] for hit in result] == [pk * 2 for pk in expected]
                assert [self._hit_field(hit, "ts") for hit in result] == expected
                record_property(
                    f"large_topk_{limit}", json.dumps({"collection": name, "resource": resource, "hits": len(result)})
                )

    @pytest.mark.tags(CaseLabel.L3, "pyudf")
    @pytest.mark.parametrize("axis", ["topk", "nq"])
    @pytest.mark.parametrize("position", ["one", "below", "limit"])
    def test_pyudf_positive_capacity_boundaries(
        self, tmp_path, minio_host, minio_bucket, request, record_property, axis, position
    ):
        """target: Q02; method: one maximum axis at a time; expected: every query, score and candidate remains aligned."""
        maximum = self._limit(request, axis, record_property)
        count = {"one": 1, "below": maximum - 1, "limit": maximum}[position]
        rows = [{"id": i, "ts": i, "vector": [float(i), 0.0]} for i in range(1, count + 1)] if axis == "topk" else None
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, None, rows=rows) as (
            client,
            name,
            resource,
        ):
            queries = [[0.0, 0.0]] if axis == "topk" else [[(index % 3) * 0.01, 0.0] for index in range(count)]
            started = time.monotonic()
            result = client.search(
                name,
                data=queries,
                anns_field="vector",
                limit=count if axis == "topk" else 1,
                output_fields=["ts"],
                search_params={"metric_type": "L2"},
                function_chains=self._pyudf_chain(resource),
                timeout=90,
            )
            if axis == "topk":
                expected = list(range(count, 0, -1))
                assert len(result) == 1
                assert [hit["id"] for hit in result[0]] == expected
                assert [hit["distance"] for hit in result[0]] == [item * 2 for item in expected]
                assert [self._hit_field(hit, "ts") for hit in result[0]] == expected
            else:
                assert len(result) == count
                for index, hits in enumerate(result):
                    pk = index % 3 + 1
                    assert len(hits) == 1 and hits[0]["id"] == pk, (index, hits)
                    assert hits[0]["distance"] == pk * 20 and self._hit_field(hits[0], "ts") == pk * 10
                self._assert_pyudf_recovery(client, name, resource)
            record_property(
                "capacity",
                json.dumps(
                    {
                        "axis": axis,
                        "position": position,
                        "effective_limit": maximum,
                        "requested": count,
                        "actual_queries": len(result),
                        "actual_hits": sum(map(len, result)),
                        "elapsed_seconds": time.monotonic() - started,
                        "resource": resource,
                        "collection": name,
                    }
                ),
            )

    @pytest.mark.tags(CaseLabel.L3, "pyudf")
    def test_pyudf_pinned_old_sdk_capability_boundary(
        self, tmp_path, minio_host, minio_bucket, request, record_property
    ):
        """target: Q13; method: isolated published SDK2.6.6; expected: ordinary requests work and DSL absence is local."""
        uv = shutil.which("uv")
        assert uv, "The approved Python environment requires uv"
        env_path = tmp_path / "old_sdk"
        created = subprocess.run(
            [uv, "venv", "--python", sys.executable, str(env_path)], capture_output=True, text=True, timeout=60
        )
        assert created.returncode == 0, created.stderr
        interpreter = env_path / "bin/python"
        installed = subprocess.run(
            [
                uv,
                "pip",
                "install",
                "--python",
                str(interpreter),
                "--index-url",
                "https://pypi.org/simple/",
                "pymilvus==2.6.6",
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )
        assert installed.returncode == 0, installed.stderr
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, MODULE) as (client, name, resource):
            self._assert_pyudf_recovery(client, name, resource)
            server = client.get_server_version(detail=True)
            record_property("server_identity_from_current_sdk", json.dumps(server))
            expected_commit = request.config.getoption("--pyudf-expected-commit")
            if expected_commit:
                assert server["git_commit"].startswith(expected_commit), server
            config = {
                "collection": name,
                "server_version": server["version"],
                "uri": cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}",
                "credential": cf.param_info.param_token or f"{cf.param_info.param_user}:{cf.param_info.param_password}",
            }
            environment = dict(
                os.environ, VIRTUAL_ENV=str(env_path), PATH=str(env_path / "bin") + os.pathsep + os.environ["PATH"]
            )
            process = subprocess.run(
                [str(interpreter), "-c", LEGACY_SDK_PROBE],
                input=json.dumps(config),
                env=environment,
                capture_output=True,
                text=True,
                timeout=60,
            )
            assert process.returncode == 0, (process.stdout, process.stderr)
            record_property("old_sdk", process.stdout.strip())
            self._assert_pyudf_recovery(client, name, resource)

    @pytest.mark.tags(CaseLabel.L3, "pyudf")
    def test_pyudf_wheel_switch_after_actual_timeout_drain(
        self, tmp_path, minio_host, minio_bucket, request, record_property
    ):
        """target: Q16; method: observe actual completion before package switch; expected: distinct versions without mixing."""
        assert self._pyudf_management_config(request, "function.pyUDF.server.workerCount") == "1"
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, DRAIN_MODULE) as (client, name, old):
            self._assert_pyudf_recovery(client, name, old)
            old_wheel = next(tmp_path.glob("*.whl"))
            old_hash = hashlib.sha256(old_wheel.read_bytes()).hexdigest()

            def invoke(mode, timeout=10):
                chain = FunctionChain(FunctionChainStage.L2_RERANK).map("$score", self._expr(old, "ts", mode=mode))
                return client.search(
                    name,
                    data=[[0.0, 0.0]],
                    anns_field="vector",
                    limit=3,
                    output_fields=["ts"],
                    search_params={"metric_type": "L2"},
                    function_chains=chain,
                    timeout=timeout,
                )

            old_pid = int(invoke("pid")[0][0]["distance"])
            start = time.monotonic()
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(invoke, "slow", 0.5)
                while invoke("state")[0][0]["distance"] != 100:
                    assert time.monotonic() - start < 0.45, "Slow invocation did not start before deadline"
                    time.sleep(0.01)
                assert_pyudf_deadline(lambda: pending.result(timeout=2))
                assert invoke("state")[0][0]["distance"] == 100, "Timeout must precede actual completion"
                # No more work is submitted to this package until its real end.
                while invoke("state")[0][0]["distance"] != 1:
                    assert time.monotonic() - start < 10, "Timed-out invocation did not drain"
                    time.sleep(0.05)
            drained = time.monotonic() - start
            assert drained >= 3
            new_module = DRAIN_MODULE.replace("v.as_py() * 2", "v.as_py() * 3")
            with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, new_module) as (
                new_client,
                new_name,
                new,
            ):
                new_wheel = next(path for path in tmp_path.glob("*.whl") if path != old_wheel)
                new_hash = hashlib.sha256(new_wheel.read_bytes()).hexdigest()
                assert new_hash != old_hash and new_wheel.name != old_wheel.name and new != old
                paths = {item.name: item.path for item in client.list_file_resources()}
                assert paths[new] != paths[old]
                new_pid_chain = FunctionChain(FunctionChainStage.L2_RERANK).map(
                    "$score", self._expr(new, "ts", mode="pid")
                )
                new_pid = int(self._search_with_synced_resource(new_client, new_name, new_pid_chain)[0][0]["distance"])
                assert new_pid == old_pid, "This switch intentionally retains the worker cache"
                for _ in range(3):
                    result = self._search_with_synced_resource(new_client, new_name, self._pyudf_chain(new))
                    assert [hit["id"] for hit in result[0]] == [3, 2, 1]
                    assert [hit["distance"] for hit in result[0]] == [90, 60, 30]
                    self._assert_pyudf_recovery(client, name, old)
                client.remove_file_resource(old)
                self._wait_for_removed_pyudf(client, name, self._pyudf_chain(old), old)
                assert old not in {item.name for item in client.list_file_resources()}
                result = self._search_with_synced_resource(new_client, new_name, self._pyudf_chain(new))
                assert [hit["distance"] for hit in result[0]] == [90, 60, 30]
                plain = new_client.search(
                    new_name, data=[[0.0, 0.0]], anns_field="vector", limit=3, search_params={"metric_type": "L2"}
                )
                assert [hit["id"] for hit in plain[0]] == [1, 2, 3]
                record_property(
                    "wheel_switch",
                    json.dumps(
                        {
                            "old": old,
                            "new": new,
                            "old_sha256": old_hash,
                            "new_sha256": new_hash,
                            "old_path": paths[old],
                            "new_path": paths[new],
                            "old_pid": old_pid,
                            "new_pid": new_pid,
                            "actual_drain_seconds": drained,
                            "old_calls_at_switch": 1,
                        }
                    ),
                )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("string_pk", [False, True], ids=["int64", "varchar"])
    @pytest.mark.parametrize("ingress", ["insert", "bulk_import"])
    def test_pyudf_comprehensive_schema_maintenance(
        self, tmp_path, minio_host, minio_bucket, string_pk, ingress, record_property
    ):
        """target: D2 lifecycle; method: full field ledger and runtime scalar checks; expected: exact rows through maintenance."""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, SCHEMA_MODULE) as (
            client,
            control,
            resource,
        ):
            self._assert_pyudf_recovery(client, control, resource)
            with ExitStack() as cleanup:
                name = cf.gen_unique_str("pyudf_r2_schema")
                schema = self.create_schema(client, auto_id=False, enable_dynamic_field=True)[0]
                schema.add_field(
                    "id",
                    DataType.VARCHAR if string_pk else DataType.INT64,
                    is_primary=True,
                    **({"max_length": 64} if string_pk else {}),
                )
                for field, kind in zip(
                    SCALAR_FIELDS,
                    [
                        DataType.INT64,
                        DataType.BOOL,
                        DataType.INT8,
                        DataType.INT16,
                        DataType.INT32,
                        DataType.INT64,
                        DataType.FLOAT,
                        DataType.DOUBLE,
                        DataType.VARCHAR,
                        DataType.INT64,
                    ],
                    strict=True,
                ):
                    schema.add_field(
                        field,
                        kind,
                        **({"max_length": 64} if kind == DataType.VARCHAR else {}),
                        **({"nullable": True} if field == "optional" else {}),
                    )
                schema.add_field("payload", DataType.JSON)
                schema.add_field("array", DataType.ARRAY, element_type=DataType.INT64, max_capacity=2)
                schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)
                indexes = self.prepare_index_params(client)[0]
                indexes.add_index("vector", index_type="FLAT", metric_type="L2")
                self.create_collection(
                    client, name, schema=schema, index_params=indexes, consistency_level="Strong", num_shards=2
                )
                cleanup.callback(self.drop_collection, client, name)
                self.alter_collection_properties(
                    client, name, properties={"collection.autocompaction.enabled": "false"}
                )
                rows = [_schema_row(i, string_pk) for i in range(10000)]
                ledger = {item["id"]: item for item in rows}
                if ingress == "insert":
                    self.insert(client, name, rows[:5000])
                    self.insert(client, name, rows[5000:])
                else:
                    path = f"pyudf_r2/{name}.json"
                    source = tmp_path / path
                    source.parent.mkdir(parents=True, exist_ok=True)
                    source.write_text(json.dumps(rows))
                    # Reuse the suite's storage configuration and retain the
                    # exact owned import input for post-run ledger inspection.
                    previous = Path.cwd()
                    try:
                        copy_files_to_minio(f"{minio_host}:9000", str(tmp_path), [path], minio_bucket)
                    finally:
                        os.chdir(previous)
                    record_property(
                        "retained_import_input",
                        json.dumps(
                            {
                                "bucket": minio_bucket,
                                "object": path,
                                "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                            }
                        ),
                    )
                    uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
                    response = bulk_import(
                        url=uri, collection_name=name, files=[[path]], api_key=cf.param_info.param_token
                    ).json()
                    assert response["code"] == 0, response
                    job = response["data"]["jobId"]
                    deadline = time.monotonic() + 300
                    while True:
                        state = get_import_progress(url=uri, job_id=job, api_key=cf.param_info.param_token).json()
                        assert state["code"] == 0, state
                        if state["data"]["state"] == "Completed":
                            break
                        assert state["data"]["state"] != "Failed" and time.monotonic() < deadline, state
                        time.sleep(1)
                    record_property("import_result", json.dumps(state))
                    assert state["data"]["importedRows"] == state["data"]["totalRows"] == len(rows), state
                    # Import completion commits segments; refresh the already
                    # loaded collection before asserting the loaded row set.
                    client.refresh_load(name, timeout=180)

                def verify(stage, added=False):
                    fields = [*SCALAR_FIELDS, *(["added"] if added else [])]
                    chain = (
                        FunctionChain(FunctionChainStage.L2_RERANK)
                        .map("$score", self._expr(resource, *fields, mode="schema"))
                        .sort(col("$score"), desc=True, tie_break_col=col("$id"))
                    )
                    stored = self.query(client, name, filter="", output_fields=["*"], limit=11000)[0]
                    assert len(stored) == len(ledger), (stage, len(stored), len(ledger))
                    assert {item["id"]: item for item in stored} == ledger, stage
                    result = self._search_with_synced_resource(client, name, chain, limit=len(ledger))[0]
                    expected = sorted(ledger, key=lambda pk: (-ledger[pk]["ts"], pk))
                    assert [hit["id"] for hit in result] == expected, stage
                    assert [hit["distance"] for hit in result] == [ledger[pk]["ts"] * 2 for pk in expected], stage
                    assert [self._hit_field(hit, "ts") for hit in result] == [ledger[pk]["ts"] for pk in expected]
                    segments = [
                        {"id": s.segment_id, "rows": s.num_rows, "state": s.state, "index": s.index_id}
                        for s in client.list_loaded_segments(name)
                    ]
                    record_property(
                        "schema_" + stage, json.dumps({"collection": name, "rows": len(ledger), "segments": segments})
                    )

                verify("initial")
                self.flush(client, name)
                self.release_collection(client, name)
                self.load_collection(client, name)
                verify("sealed_reload")
                self.add_collection_field(client, name, "added", DataType.INT64, nullable=True)
                for item in ledger.values():
                    item["added"] = None
                replacement = _schema_row(20000, string_pk, primary=9000)
                replacement["added"] = 99
                self.upsert(client, name, [replacement])
                ledger[replacement["id"]] = replacement
                new = _schema_row(20001, string_pk)
                new["added"] = 99
                self.insert(client, name, [new])
                ledger[new["id"]] = new
                deleted = [_schema_row(i, string_pk)["id"] for i in range(2500)]
                self.delete(client, name, ids=deleted)
                for key in deleted:
                    ledger.pop(key)
                verify("mutated_mixed", added=True)
                self._assert_pyudf_error(
                    client,
                    name,
                    FunctionChain(FunctionChainStage.L2_RERANK).map("$score", self._expr(resource, "array")),
                    "unsupported field type Array",
                )
                verify("array_rejection_recovery", added=True)
                self.flush(client, name)
                job = self.compact(client, name)[0]
                deadline = time.monotonic() + 180
                while True:
                    state = self.get_compaction_state(client, job)[0]
                    if state == "Completed":
                        break
                    assert state != "Failed" and time.monotonic() < deadline, state
                    time.sleep(0.5)
                plans = client.get_compaction_plans(job)
                assert plans.plans, "No-op compaction does not establish maintenance coverage"
                record_property("compaction_plans", str(plans))
                self.release_collection(client, name)
                self.load_collection(client, name)
                verify("compacted_reload", added=True)
            self._assert_pyudf_recovery(client, control, resource)

    @pytest.mark.tags(CaseLabel.L3, "pyudf")
    @pytest.mark.parametrize(
        "invalid", ["duplicate_stage", "invalid_stage", "topk_zero", "topk_over", "nq_zero", "nq_over"]
    )
    def test_pyudf_static_rejection_never_invokes_worker(
        self, tmp_path, minio_host, minio_bucket, request, record_property, invalid
    ):
        """target: admission; method: raw public RPC and worker counter; expected: rejection and zero calls."""
        # One worker is required for an exact process-local invocation counter.
        assert self._pyudf_management_config(request, "function.pyUDF.server.workerCount") == "1"
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, MODULE) as (client, name, resource):
            self._assert_pyudf_recovery(client, name, resource)
            counter = FunctionChain(FunctionChainStage.L2_RERANK).map(
                "$score", self._expr(resource, "ts", mode="count")
            )
            before = self._search_with_synced_resource(client, name, counter)[0][0]["distance"]
            limit = self._limit(request, "nq" if invalid.startswith("nq") else "topk", record_property)
            wire = Prepare.search_requests_with_expr(
                name,
                "vector",
                {"metric_type": "L2"},
                1,
                data=[[0.0, 0.0]],
                output_fields=["ts"],
                function_chains=self._pyudf_chain(resource),
                use_default_consistency=False,
                consistency_level=0,
            )
            expected = "stage"
            if invalid == "duplicate_stage":
                wire.function_chains.add().CopyFrom(wire.function_chains[0])
                expected = "appears more than once"
            elif invalid == "invalid_stage":
                wire.function_chains[0].stage = 999
            elif invalid.startswith("topk"):
                next(item for item in wire.search_params if item.key == "topk").value = str(
                    0 if invalid == "topk_zero" else limit + 1
                )
                expected = f"range [1, {limit}]"
            else:
                wire.nq = 0 if invalid == "nq_zero" else limit + 1
                if invalid == "nq_zero":
                    # getNq infers legacy nq=0 from the placeholder values.
                    placeholders = common_pb2.PlaceholderGroup.FromString(wire.placeholder_group)
                    for placeholder in placeholders.placeholders:
                        del placeholder.values[:]
                    wire.placeholder_group = placeholders.SerializeToString()
                expected = "nq"
            # The authenticated SDK channel preserves its metadata interceptor;
            # raw Search bypasses SDK parameter validation and retries.
            response = client._get_connection()._stub.Search(wire, timeout=30)
            assert response.status.code == 1100, response.status
            assert expected in response.status.reason.lower(), response.status
            assert not response.results.ids.int_id.data and not response.results.scores
            after = self._search_with_synced_resource(client, name, counter)[0][0]["distance"]
            assert after == before + 1, (before, after, response.status)
            record_property(
                "admission",
                json.dumps(
                    {"case": invalid, "limit": limit, "before": before, "after": after, "status": str(response.status)}
                ),
            )
            self._assert_pyudf_recovery(client, name, resource)
            assert resource in {item.name for item in client.list_file_resources()}

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "packaging",
        [
            "source",
            "zip64",
            "preinstalled_native",
            "missing_dependency",
            "namespace",
            "native_extension",
            "invalid_metadata",
            "missing_factory",
            "noncallable_factory",
        ],
    )
    def test_pyudf_wheel_packaging_boundary(self, tmp_path, minio_host, minio_bucket, packaging, record_property):
        """target: cold loader; method: isolated wheel variants; expected: exact output or classified loader failure."""
        package = cf.gen_unique_str("pyudf_r2_package").lower()
        module = MODULE
        entry = f"{package}:create_udf"
        files = {}
        error = None
        if packaging == "preinstalled_native":
            module = (
                "import pyarrow as pa\nimport pyarrow.compute as pc\nassert pc.sum(pa.array([1, 2])).as_py() == 3\n"
                + module
            )
        elif packaging == "missing_dependency":
            module = "import pyudf_r2_deliberately_absent_dependency\n" + module
            error = "cannot import PyUDF entry module"
        elif packaging == "namespace":
            files[f"{package}/module.py"] = module
            entry = f"{package}.module:create_udf"
            error = "cannot import PyUDF entry module"
        elif packaging == "native_extension":
            # A native module inside a ZIP cannot be imported by zipimport.
            files[f"{package}/lib.so"] = Path(pyarrow.lib.__file__).read_bytes()
            module = "from . import lib\n" + MODULE
            error = "cannot import PyUDF entry module"
        elif packaging == "missing_factory":
            entry = f"{package}:absent_factory"
            error = "cannot resolve PyUDF entry factory"
        elif packaging == "noncallable_factory":
            module += "\ncreate_udf = 17\n"
            error = "is not callable"
        if packaging != "namespace":
            files[f"{package}/__init__.py"] = module
        files[f"{package}-0.0.1.dist-info/METADATA"] = f"Metadata-Version: 2.1\nName: {package}\nVersion: 0.0.1\n"
        files[f"{package}-0.0.1.dist-info/entry_points.txt"] = (
            "[broken\n" if packaging == "invalid_metadata" else f"[milvus.pyudf]\nmain = {entry}\n"
        )
        if packaging == "invalid_metadata":
            error = "invalid wheel metadata"
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as wheel:
            for path, data in files.items():
                wheel.writestr(path, data)
            if packaging == "zip64":
                # Exceed the central-directory count, requiring real ZIP64 EOCD.
                for index in range(65536):
                    wheel.writestr(f"padding/{index:05d}", b"")
        if packaging == "zip64":
            assert b"PK\x06\x06" in output.getvalue()
            error = "cannot import PyUDF entry module"
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, MODULE) as (owner, control, good):
            self._assert_pyudf_recovery(owner, control, good)
            with self._pyudf_test_environment(
                tmp_path, minio_host, minio_bucket, None, wheel_bytes=output.getvalue()
            ) as (client, name, resource):
                if error:
                    self._assert_pyudf_error(client, name, self._pyudf_chain(resource), error, code=2400)
                    plain = self.search(
                        client,
                        name,
                        data=[[0.0, 0.0]],
                        anns_field="vector",
                        limit=3,
                        search_params={"metric_type": "L2"},
                    )[0]
                    assert [hit["id"] for hit in plain[0]] == [1, 2, 3]
                else:
                    self._assert_pyudf_recovery(client, name, resource)
                assert resource in {item.name for item in owner.list_file_resources()}
                record_property(
                    "packaging", json.dumps({"kind": packaging, "resource": resource, "expected_error": error})
                )
            self._assert_pyudf_recovery(owner, control, good)

    @pytest.mark.tags(CaseLabel.L3, "pyudf")
    def test_pyudf_search_only_cannot_manage_resources(self, tmp_path, minio_host, minio_bucket):
        """target: reverse RBAC; method: Search-only actor attempts add/list/remove; expected: owner state unchanged."""
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, MODULE) as (root, name, resource):
            self._assert_pyudf_recovery(root, name, resource)
            original = {item.name: item.path for item in root.list_file_resources()}
            candidate = cf.gen_unique_str("pyudf_r2_denied")
            with ExitStack() as cleanup:
                cleanup.callback(self._remove_file_resource, root, candidate)
                with self._pyudf_security_actor(root) as (actor, _, grant, revoke):
                    grant("DescribeCollection", name, "default")
                    grant("Search", name, "default")

                    def operation():
                        return self._search_with_synced_resource(actor, name, self._pyudf_chain(resource))

                    allowed = self._wait_for_pyudf_permission(operation)
                    assert [hit["distance"] for hit in allowed[0]] == [60, 40, 20]
                    for denied in (
                        actor.list_file_resources,
                        lambda: actor.remove_file_resource(resource),
                        lambda: actor.add_file_resource(candidate, original[resource]),
                    ):
                        self._wait_for_pyudf_permission(denied, denied=True)
                        actual = {item.name: item.path for item in root.list_file_resources()}
                        assert actual.get(resource) == original[resource] and candidate not in actual
                    revoke("Search", name, "default")
                    self._wait_for_pyudf_permission(operation, denied=True)
                    grant("Search", name, "default")
                    allowed = self._wait_for_pyudf_permission(operation)
                    assert [hit["id"] for hit in allowed[0]] == [3, 2, 1]
                    assert [hit["distance"] for hit in allowed[0]] == [60, 40, 20]
                self._assert_pyudf_recovery(root, name, resource)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric", ["L2", "IP", "COSINE"])
    @pytest.mark.parametrize("sort", [False, True], ids=["map_only", "explicit_sort"])
    @pytest.mark.parametrize("mode", ["rewrite", "preserve", "rewrite_group", "preserve_group"])
    def test_pyudf_cross_stage_score_sign_and_rank(
        self, tmp_path, minio_host, minio_bucket, metric, sort, mode, record_property
    ):
        """target: #55212; method: positive stage arithmetic and real identity UDF; expected: positive scores and exact ranks."""
        rows = [{"id": i, "ts": i * 10, "vector": [float(i), 1.0]} for i in (1, 2, 3)]
        with self._pyudf_test_environment(
            tmp_path, minio_host, minio_bucket, MODULE, rows=rows, metric_type=metric
        ) as (
            client,
            name,
            resource,
        ):
            plain = self.search(
                client,
                name,
                data=[[1.0, 1.0]],
                anns_field="vector",
                limit=3,
                search_params={"metric_type": metric},
                output_fields=["ts"],
            )[0]
            expected_raw = {
                i: float((i - 1) ** 2)
                if metric == "L2"
                else float(i + 1)
                if metric == "IP"
                else (i + 1) / math.sqrt(2 * (i * i + 1))
                for i in (1, 2, 3)
            }
            for hit in plain[0]:
                assert hit["distance"] == pytest.approx(expected_raw[hit["id"]], rel=1e-5, abs=1e-6)
            l0 = FunctionChain(FunctionChainStage.L0_RERANK).map(
                "$score", fn.num_combine(col("ts"), col("ts"), mode="sum")
            )
            l1 = FunctionChain(FunctionChainStage.L1_RERANK).map(
                "$score", fn.num_combine(col("$score"), col("ts"), mode="sum")
            )
            l2 = FunctionChain(FunctionChainStage.L2_RERANK).map(
                "$score", self._expr(resource, "$score", mode="identity")
            )
            preserve = mode.startswith("preserve")
            if preserve:
                # Exercise each stage without writing $score at any stage.
                l0 = FunctionChain(FunctionChainStage.L0_RERANK).map(
                    "l0_value", fn.num_combine(col("ts"), col("ts"), mode="sum")
                )
                l1 = FunctionChain(FunctionChainStage.L1_RERANK).map(
                    "l1_value", fn.num_combine(col("ts"), col("ts"), mode="sum")
                )
                l2 = FunctionChain(FunctionChainStage.L2_RERANK).map("pyudf_value", self._expr(resource, "ts"))
            if sort:
                l2.sort(col("$score"), desc=True, tie_break_col=col("$id"))
            stages = [l0, l1, l2]
            # All six permutations share immutable rows, index and UDF behavior.
            for stage_order in ((0, 1, 2), (2, 0, 1), (1, 2, 0), (0, 2, 1), (1, 0, 2), (2, 1, 0)):
                result = self._search_with_synced_resource(
                    client,
                    name,
                    [stages[i] for i in stage_order],
                    data=[[1.0, 1.0]],
                    search_params={"metric_type": metric},
                    **({"group_by_field": "ts", "group_size": 1} if mode.endswith("group") else {}),
                )
                record_property(
                    f"cross_stage_observed_{stage_order}",
                    json.dumps({"metric": metric, "stages": stage_order, "sort": sort, "mode": mode, "hits": result}),
                )
                # Preserve positive business-score expectations even on the known failing baseline.
                expected_scores = expected_raw if preserve else {1: 30.0, 2: 60.0, 3: 90.0}
                assert {hit["id"]: hit["distance"] for hit in result[0]} == pytest.approx(
                    expected_scores, rel=1e-5, abs=1e-6
                )
                if sort:
                    assert [hit["id"] for hit in result[0]] == sorted(
                        expected_scores, key=lambda pk: (-expected_scores[pk], pk)
                    )
                elif preserve:
                    assert [hit["id"] for hit in result[0]] == [hit["id"] for hit in plain[0]]
                for hit in result[0]:
                    assert self._hit_field(hit, "ts") == hit["id"] * 10

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("merge", ["rrf", "weighted"])
    @pytest.mark.parametrize("rewrite", [False, True], ids=["read_only", "rewrite"])
    def test_pyudf_indexed_hybrid_fusion_read_and_rewrite(
        self, tmp_path, minio_host, minio_bucket, request, merge, rewrite, record_property
    ):
        """target: Q03 profile mapping; method: indexed dense/BM25 independent fusion; expected: exact read-only or rewritten scores."""
        expected_auto = request.config.getoption("--pyudf-expected-autoindex")
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, MODULE) as (client, control, resource):
            self._assert_pyudf_recovery(client, control, resource)
            with ExitStack() as cleanup:
                name = cf.gen_unique_str("pyudf_r2_fusion")
                started = time.time()
                schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
                schema.add_field("id", DataType.INT64, is_primary=True)
                schema.add_field("ts", DataType.INT64)
                schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)
                schema.add_field(
                    "text",
                    DataType.VARCHAR,
                    max_length=128,
                    enable_analyzer=True,
                    enable_match=True,
                    analyzer_params={"tokenizer": "standard"},
                )
                schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
                schema.add_function(
                    Function(
                        name="bm25",
                        function_type=FunctionType.BM25,
                        input_field_names=["text"],
                        output_field_names=["sparse"],
                    )
                )
                self.create_collection(client, name, schema=schema, consistency_level="Strong")
                cleanup.callback(self.drop_collection, client, name)
                rows = [
                    {"id": pk, "ts": pk * 10, "vector": vector, "text": text}
                    for pk, vector, text in [
                        (1, [0.0, 0.0], "milvus vector database"),
                        (2, [0.01, 0.0], "milvus search"),
                        (3, [0.02, 0.0], "postgres relational"),
                        (4, [1.0, 0.0], "milvus relational"),
                    ]
                ]
                rows.extend(
                    {"id": i, "ts": i * 10, "vector": [float(i), 1.0], "text": "filler"} for i in range(5, 10001)
                )
                self.insert(client, name, rows[:5000])
                self.insert(client, name, rows[5000:])
                self.flush(client, name)
                indexes = self.prepare_index_params(client)[0]
                indexes.add_index(
                    "vector",
                    index_type="AUTOINDEX" if expected_auto else "HNSW",
                    metric_type="L2",
                    params={} if expected_auto else {"M": 16, "efConstruction": 128},
                )
                indexes.add_index(
                    "sparse", index_type="AUTOINDEX" if expected_auto else "SPARSE_INVERTED_INDEX", metric_type="BM25"
                )
                self.create_index(client, name, indexes)
                index_details = {field: client.describe_index(name, field) for field in ("vector", "sparse")}
                assert all(
                    value["indexed_rows"] == 10000 and value["state"] == "Finished" for value in index_details.values()
                ), index_details
                self.load_collection(client, name)
                # Single/batch queries are read-only and reuse the same built indexes.
                for nq in (1, 2):
                    dense, texts = [[0.0, 0.0], [0.02, 0.0]][:nq], ["milvus", "relational"][:nq]
                    dense_hits = client.search(
                        name,
                        data=dense,
                        anns_field="vector",
                        search_params={"metric_type": "L2", "params": {"ef": 128}},
                        limit=2,
                    )
                    sparse_hits = client.search(
                        name, data=texts, anns_field="sparse", search_params={"metric_type": "BM25"}, limit=3
                    )
                    expected = []
                    for query_id, (first, second) in enumerate(zip(dense_hits, sparse_hits, strict=True)):
                        # ANN recall is measured independently from fusion and UDF
                        # correctness; use the actual branch candidates as input.
                        assert len(first) == len({hit["id"] for hit in first}) == 2
                        assert all(1 <= hit["id"] <= 10000 for hit in first)
                        record_property(
                            f"ann_recall_{query_id}",
                            json.dumps({"exact_ids": [1, 2] if query_id == 0 else [3, 2], "actual": first}),
                        )
                        assert {hit["id"] for hit in second} == ({1, 2, 4} if query_id == 0 else {3, 4})
                        scores = {}
                        for branch, weight, dense_branch in ((first, 0.3, True), (second, 0.7, False)):
                            for rank, hit in enumerate(branch, start=1):
                                normalized = 2 * math.atan(hit["distance"]) / math.pi
                                contribution = (
                                    1 / (60 + rank)
                                    if merge == "rrf"
                                    else weight * (1 - normalized if dense_branch else normalized)
                                )
                                scores[hit["id"]] = scores.get(hit["id"], 0) + contribution
                        expected.append(
                            {pk: 100 * score + pk * 20 if rewrite else score for pk, score in scores.items()}
                        )
                    chain = FunctionChain(FunctionChainStage.L2_RERANK)
                    chain.merge("rrf", k=60) if merge == "rrf" else chain.merge(
                        "weighted", weights=[0.3, 0.7], norm_score=True
                    )
                    chain.map(
                        "$score" if rewrite else "read_fused",
                        self._expr(resource, "$score", "ts", mode="fusion" if rewrite else "identity"),
                    )
                    chain.sort(col("$score"), desc=True, tie_break_col=col("$id"))
                    requests = [
                        AnnSearchRequest(
                            data=dense, anns_field="vector", param={"metric_type": "L2", "params": {"ef": 128}}, limit=2
                        ),
                        AnnSearchRequest(data=texts, anns_field="sparse", param={"metric_type": "BM25"}, limit=3),
                    ]
                    result = client.hybrid_search(
                        name, reqs=requests, ranker=None, limit=3, output_fields=["ts"], function_chains=chain
                    )
                    assert len(result) == nq
                    for scores, hits in zip(expected, result, strict=True):
                        ids = sorted(scores, key=lambda pk: (-scores[pk], pk))[:3]
                        assert [hit["id"] for hit in hits] == ids
                        assert [hit["distance"] for hit in hits] == pytest.approx(
                            [scores[pk] for pk in ids], rel=1e-5, abs=1e-6
                        )
                        assert [self._hit_field(hit, "ts") for hit in hits] == [pk * 10 for pk in ids]
                    record_property(
                        f"indexed_fusion_nq{nq}",
                        json.dumps(
                            {
                                "collection": name,
                                "resource": resource,
                                "merge": merge,
                                "nq": nq,
                                "rewrite": rewrite,
                                "expected_autoindex": expected_auto,
                                "indices": index_details,
                                "segments": [
                                    {"id": segment.segment_id, "rows": segment.num_rows, "index": segment.index_id}
                                    for segment in client.list_loaded_segments(name)
                                ],
                                "start_unix": started,
                                "end_unix": time.time(),
                                "hits": result,
                            }
                        ),
                    )
            self._assert_pyudf_recovery(client, control, resource)

    @pytest.mark.tags(CaseLabel.L3, "pyudf")
    @pytest.mark.parametrize("warm_resource", [False, True], ids=["cold-resource", "warm-resource"])
    def test_pyudf_same_cluster_snapshot_restore_reuses_registered_resource(
        self, tmp_path, minio_host, minio_bucket, warm_resource, record_property
    ):
        """Restore snapshot data with existing global resources, then verify missing-resource rejection."""
        from minio import Minio

        rows = [{"id": i, "ts": i * 3 + 7, "vector": [i / 1024, 0.0]} for i in range(1000)]
        module = MODULE.replace(
            "def create_udf(context):\n    return UDF(context)",
            'def create_udf(context):\n    context.logger.warning("PYUDF_SNAPSHOT_FACTORY resource=%s", '
            "context.resource_name)\n    return UDF(context)",
        )
        with self._pyudf_test_environment(tmp_path, minio_host, minio_bucket, module, rows=rows) as (
            client,
            source,
            resource,
        ):
            server = client.get_server_version(detail=True)
            registered = {item.name: item.path for item in client.list_file_resources()}
            wheel_path = registered[resource]
            store = Minio(f"{minio_host}:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)

            def wheel_hash():
                response = store.get_object(minio_bucket, wheel_path)
                try:
                    return hashlib.sha256(response.read()).hexdigest()
                finally:
                    response.close()
                    response.release_conn()

            original_hash = wheel_hash()
            expected = {row["id"]: dict(row) for row in rows}
            updates = [{**expected[i], "ts": 10000 + i} for i in range(10, 30)]
            additions = [{"id": i, "ts": i * 3 + 7, "vector": [i / 1024, 0.0]} for i in range(1000, 1010)]
            self.upsert(client, source, updates)
            self.delete(client, source, filter="id < 10")
            self.insert(client, source, additions)
            self.flush(client, source)
            expected.update({row["id"]: row for row in updates + additions})
            for i in range(10):
                del expected[i]
            chain = self._pyudf_chain(resource)

            def audit_data(name, wanted):
                observed = client.query(name, filter="", output_fields=["id", "ts", "vector"], limit=2000)
                assert len(observed) == len(wanted) == 1000
                assert {row["id"] for row in observed} == wanted.keys()
                for row in observed:
                    assert row["ts"] == wanted[row["id"]]["ts"]
                    assert row["vector"] == pytest.approx(wanted[row["id"]]["vector"], abs=1e-7)

            def audit_udf(name):
                result = self._search_with_synced_resource(client, name, chain, filter="id in [10, 11, 12]")[0]
                assert [hit["id"] for hit in result] == [12, 11, 10]
                assert [hit["distance"] for hit in result] == [20024, 20022, 20020]
                assert [self._hit_field(hit, "ts") for hit in result] == [10012, 10011, 10010]

            audit_data(source, expected)
            if warm_resource:
                audit_udf(source)
            record_property(
                "snapshot_setup",
                json.dumps({"server": server, "source": source, "resource": resource, "warm": warm_resource}),
            )
            snapshot = cf.gen_unique_str("pyudf_same_cluster_snapshot")
            with ExitStack() as cleanup:
                self.create_snapshot(client, snapshot, source, timeout=180)
                cleanup.callback(self.drop_snapshot, client, snapshot, source)
                info, _ = self.describe_snapshot(client, snapshot, source)
                assert info.name == snapshot
                # Mutations after the snapshot must remain absent in both restores.
                changed = {**expected[10], "ts": 99999}
                later = {"id": 1010, "ts": 123, "vector": [1010 / 1024, 0.0]}
                self.upsert(client, source, [changed])
                self.delete(client, source, filter="id == 11")
                self.insert(client, source, [later])
                self.flush(client, source)
                current = {**expected, 10: changed, 1010: later}
                del current[11]
                audit_data(source, current)

                def restore(suffix):
                    name = cf.gen_unique_str("pyudf_same_cluster_" + suffix)
                    cleanup.callback(self.drop_collection, client, name)
                    job, _ = self.restore_snapshot(client, snapshot, source, name, timeout=180)
                    record_property("restore_submitted", json.dumps({"target": name, "job": job}))
                    deadline = time.monotonic() + 180
                    while True:
                        state, _ = self.get_restore_snapshot_state(client, job)
                        assert state.state != "RestoreSnapshotFailed", state
                        if state.state == "RestoreSnapshotCompleted":
                            break
                        assert time.monotonic() < deadline, state
                        time.sleep(1)
                    self.load_collection(client, name)
                    audit_data(name, expected)
                    record_property("restore_completed", json.dumps({"target": name, "rows": 1000, "job": job}))
                    return name

                restored = restore("registered")
                assert {item.name: item.path for item in client.list_file_resources()} == registered
                assert wheel_hash() == original_hash
                audit_udf(restored)
                self.release_collection(client, restored)
                self.load_collection(client, restored)
                audit_udf(restored)
                record_property("restore_with_existing_resource", "PASS without uploading or registering any resource")

                self._remove_file_resource(client, resource)
                self._wait_for_removed_pyudf(client, restored, chain, resource)
                missing = restore("unregistered")
                assert resource not in {item.name for item in client.list_file_resources()}
                assert wheel_hash() == original_hash
                self._wait_for_removed_pyudf(client, missing, chain, resource)
                record_property("restore_without_registration", "Data PASS; PyUDF rejected; registration not recreated")

                # Removed registrations do not unload worker packages. Recover
                # using a distinct package rather than unsupported hot replacement.
                package = cf.gen_unique_str("pyudf_snapshot_recovery").lower()
                recovery_wheel = self._build_wheel(tmp_path, package, module=module).read_bytes()
                recovery_path = f"pyudf/{resource}-recovery.whl"
                store.put_object(minio_bucket, recovery_path, io.BytesIO(recovery_wheel), len(recovery_wheel))
                cleanup.callback(store.remove_object, minio_bucket, recovery_path)
                cleanup.callback(self._remove_file_resource, client, resource)
                self.add_file_resource(client, resource, recovery_path)
                audit_udf(missing)
                audit_udf(restored)
                audit_data(source, current)
                assert wheel_hash() == original_hash
                assert {item.name: item.path for item in client.list_file_resources()} == {
                    **registered,
                    resource: recovery_path,
                }
                assert client.get_server_version(detail=True)["git_commit"] == server["git_commit"]
                record_property(
                    "same_cluster_snapshot_complete",
                    json.dumps(
                        {
                            "snapshot": snapshot,
                            "source": source,
                            "targets": [restored, missing],
                            "resource": resource,
                            "wheel_sha256": original_hash,
                            "warm_resource": warm_resource,
                            "rows_per_restore": 1000,
                            "snapshot_cut": "PASS",
                            "automatic_resource_recreation": False,
                            "explicit_reregistration_recovery": "PASS",
                            "recovery_package": package,
                            "recovery_wheel_sha256": hashlib.sha256(recovery_wheel).hexdigest(),
                        }
                    ),
                )
