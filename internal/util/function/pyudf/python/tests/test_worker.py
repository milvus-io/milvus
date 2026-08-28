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

"""P2 tests use real unary gRPC calls, real wheels and controlled failures."""

from __future__ import annotations

import importlib
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import grpc
import pyarrow as pa
from grpc_health.v1 import health_pb2, health_pb2_grpc

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from milvus_pyudf_runtime import PyUDFLoader, loader  # noqa: E402
from milvus_pyudf_runtime.config import MAX_PARAM_NESTING_DEPTH, ServerConfig  # noqa: E402
from milvus_pyudf_runtime.errors import RequestInactive, WorkerError  # noqa: E402
from milvus_pyudf_runtime.proto import pyudf_pb2 as wire  # noqa: E402
from milvus_pyudf_runtime.proto import pyudf_pb2_grpc  # noqa: E402
from milvus_pyudf_runtime.worker import WorkerServer, _params_object  # noqa: E402
from test_milvus_pyudf_runtime import WheelFixture  # noqa: E402


def encode(batches, compression=None):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batches[0].schema, options=pa.ipc.IpcWriteOptions(compression=compression)) as writer:
        for batch in batches:
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def decode(payload):
    with pa.ipc.open_stream(payload) as reader:
        return list(reader)


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.fixtures = WheelFixture(self.directory)
        self.packages = []
        self.release_events = []
        self.module_locks = dict(PyUDFLoader._module_locks)
        self.paths = set(PyUDFLoader._wheel_paths)
        self.claims = dict(PyUDFLoader._package_claims)
        self.config = ServerConfig("127.0.0.1:19090", 1, 4, 100, 1 << 20, 30000)
        self.worker = WorkerServer(self.config)
        self.server, self.executor = self.worker.server, self.worker.executor
        port = self.worker.start("127.0.0.1:0")
        self.channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        grpc.channel_ready_future(self.channel).result(timeout=3)
        self.client = pyudf_pb2_grpc.PyUDFWorkerStub(self.channel)

    def tearDown(self):
        for event in self.release_events:
            event.set()
        self.server.stop(0).wait(timeout=3)
        self.executor.shutdown(wait=True)
        self.channel.close()
        for package in self.packages:
            for name in list(sys.modules):
                if name == package or name.startswith(package + "."):
                    sys.modules.pop(name, None)
        for path in PyUDFLoader._wheel_paths - self.paths:
            while path in sys.path:
                sys.path.remove(path)
        PyUDFLoader._module_locks.clear()
        PyUDFLoader._module_locks.update(self.module_locks)
        PyUDFLoader._wheel_paths.clear()
        PyUDFLoader._wheel_paths.update(self.paths)
        PyUDFLoader._package_claims.clear()
        PyUDFLoader._package_claims.update(self.claims)
        self.temp.cleanup()

    def wheel(self, source=None):
        package = "worker_test_" + uuid.uuid4().hex
        self.packages.append(package)
        source = (
            source
            or "loads=0\nclass UDF:\n def transform_query(self, params, columns): return columns\ndef factory(context):\n global loads\n loads+=1\n return UDF()\n"
        )
        return self.fixtures.make(package=package, module=source), package

    def request(self, path, batches=None):
        if batches is None:
            batches = [pa.record_batch([pa.array([1, 2])], names=["c0"])]
        return wire.ExecuteRequest(resource_name="test", udf_path=str(path), stage="L2_rerank", inputs=encode(batches))

    def execute(self, request):
        return self.client.Execute(request, timeout=3)

    def assert_code(self, response, code):
        self.assertEqual(response.WhichOneof("result"), "error")
        self.assertEqual(response.error.code, code, response.error.message)
        self.assertFalse(response.outputs)
        self.assertTrue(response.error.message)

    def test_health_interface_reports_this_worker(self):
        health = health_pb2_grpc.HealthStub(self.channel)
        for name in ("", "milvus.proto.pyudf.PyUDFWorker"):
            response = health.Check(health_pb2.HealthCheckRequest(service=name), timeout=3)
            self.assertEqual(response.status, health_pb2.HealthCheckResponse.SERVING)
        path, _ = self.wheel()
        self.assertEqual(self.execute(self.request(path)).WhichOneof("result"), "outputs")

    def test_real_wheel_reuse_multiple_queries_nulls_and_positions(self):
        path, package = self.wheel()
        batches = [
            pa.record_batch([pa.array([], type=pa.int64()), pa.array([], type=pa.int64())], names=["c0", "c1"]),
            pa.record_batch([pa.array([None, 4, 9]).slice(1), pa.array([None, 4])], names=["c0", "c1"]),
        ]
        request = self.request(path, batches)
        # Removed field 1 is an unknown protobuf field, not a version gate.
        request.MergeFromString(b"\x08\x02")
        for _ in range(2):
            response = self.execute(request)
            self.assertEqual(response.WhichOneof("result"), "outputs", response)
            result = decode(response.outputs)
            self.assertEqual([b.num_rows for b in result], [0, 2])
            self.assertEqual(result[1].column(0).to_pylist(), [4, 9])
            self.assertEqual(result[1].column(1).to_pylist(), [None, 4])
        self.assertEqual(importlib.import_module(package).loads, 1)
        other, other_package = self.wheel()
        self.assertEqual(self.execute(self.request(other)).WhichOneof("result"), "outputs")
        self.assertEqual(importlib.import_module(other_package).loads, 1)

    def test_stage_is_passed_unchanged_to_udf_and_scopes_cache(self):
        path, package = self.wheel("""
import pyarrow as pa
stages=[]
class UDF:
 def __init__(self, context): self.stage=context.stage
 def transform_query(self,p,c): return [pa.array([self.stage]*len(c[0]))]
def factory(context):
 if context.stage=='reject': raise ValueError('stage rejected by UDF')
 stages.append(context.stage)
 return UDF(context)
""")
        request = self.request(path)
        stages = ["L2_rerank", "custom_stage", "", "  自定义阶段  "]
        for stage in stages * 2:
            request.stage = stage
            response = self.execute(request)
            self.assertEqual(response.WhichOneof("result"), "outputs", response)
            self.assertEqual(decode(response.outputs)[0].column(0).to_pylist(), [stage, stage])
        self.assertEqual(importlib.import_module(package).stages, stages)
        request.stage = "reject"
        self.assert_code(self.execute(request), wire.UDF_FAILED)

    def test_input_references_preserve_order_and_share_arrays(self):
        path, _ = self.wheel("""
class UDF:
 def transform_query(self, params, columns):
  assert len(columns) == 4
  assert columns[0] is columns[2] is columns[3]
  assert columns[0] is not columns[1]
  return columns[:2]
def factory(context): return UDF()
""")
        batches = [
            pa.record_batch([pa.array([], type=pa.int64()), pa.array([], type=pa.int64())], names=["c0", "c1"]),
            pa.record_batch([pa.array([99, 4, None]).slice(1), pa.array([8, 9])], names=["c0", "c1"]),
        ]
        request = self.request(path, batches)
        request.input_column_indices.extend([1, 0, 1, 1])
        response = self.execute(request)
        self.assertEqual(response.WhichOneof("result"), "outputs", response)
        result = decode(response.outputs)
        self.assertEqual([batch.num_rows for batch in result], [0, 2])
        self.assertEqual(result[1].column(0).to_pylist(), [8, 9])
        self.assertEqual(result[1].column(1).to_pylist(), [4, None])

    def test_invalid_input_reference_fails_before_loading_udf(self):
        path = self.directory / "not-loaded.whl"
        for indices in ([1], [0, 1], [2**32 - 1]):
            with self.subTest(indices=indices):
                request = self.request(path)
                request.input_column_indices.extend(indices)
                response = self.execute(request)
                self.assert_code(response, wire.INTERNAL)
                self.assertIn("input column reference is out of range", response.error.message)

    def test_recursive_params_are_immutable(self):
        path, _ = self.wheel("""
import pyarrow as pa
class UDF:
 def transform_query(self, params, columns):
  assert params['integer']==-(1<<63)+1 and params['bytes']==bytes([0,255])
  assert params['array']==(True, 0.5)
  try: params['object']['x']=2
  except TypeError: pass
  else: raise ValueError('mutable parameters')
  return columns
def factory(context): return UDF()
""")
        request = self.request(path)
        request.params.fields["integer"].int64_value = -(1 << 63) + 1
        request.params.fields["bytes"].bytes_value = b"\x00\xff"
        values = request.params.fields["array"].array_value.values
        values.add().bool_value = True
        values.add().double_value = 0.5
        request.params.fields["object"].object_value.fields["x"].int64_value = 1
        self.assertEqual(self.execute(request).WhichOneof("result"), "outputs")
        request.params.fields["unset"].SetInParent()
        self.assert_code(self.execute(request), wire.INVALID_ARGUMENT)

    def test_unset_nested_params_rejected_before_loading_and_worker_recovers(self):
        path, _ = self.wheel()
        for container in ("object", "array"):
            with self.subTest(container=container):
                request = self.request(path)
                nested = request.params.fields["nested"]
                if container == "object":
                    nested.object_value.fields["unset"].SetInParent()
                else:
                    nested.array_value.values.add()
                with patch.object(self.worker.service.loader, "load") as load:
                    with self.assertLogs("milvus.pyudf.worker", level="ERROR") as logs:
                        response = self.execute(request)
                    self.assert_code(response, wire.INVALID_ARGUMENT)
                    self.assertIn("unset function parameter", response.error.message)
                    self.assertEqual(len(logs.records), 1)
                    self.assertIn("unset function parameter", logs.output[0])
                    load.assert_not_called()
                self.assertEqual(self.execute(self.request(path)).WhichOneof("result"), "outputs")

    def test_parameter_depth_boundary(self):
        # Test application validation directly: protobuf may reject deep messages
        # before the gRPC handler can apply this runtime contract.
        for container in ("object", "array", "mixed"):
            for depth in (MAX_PARAM_NESTING_DEPTH, MAX_PARAM_NESTING_DEPTH + 1):
                with self.subTest(container=container, depth=depth):
                    params = wire.FunctionParamObject()
                    value = params.fields["nested"]
                    for level in range(1, depth):
                        if container == "object" or (container == "mixed" and level % 2):
                            value = value.object_value.fields["next"]
                        else:
                            value = value.array_value.values.add()
                    value.int64_value = 1
                    if depth == MAX_PARAM_NESTING_DEPTH:
                        self.assertIn("nested", _params_object(params))
                    else:
                        with self.assertRaises(WorkerError) as caught:
                            _params_object(params)
                        self.assertEqual(caught.exception.code, wire.INVALID_ARGUMENT)

    def test_execution_errors_and_worker_survives(self):
        for expression, code in [
            ("raise ValueError('boom')", wire.UDF_FAILED),
            ("raise FileNotFoundError('user I/O')", wire.UDF_FAILED),
            ("raise MemoryError('allocation')", wire.OUT_OF_MEMORY),
            ("raise SystemExit(9)", wire.INTERNAL),
        ]:
            with self.subTest(expression=expression):
                path, _ = self.wheel(
                    f"class UDF:\n def transform_query(self,p,c): {expression}\ndef factory(context): return UDF()\n"
                )
                with self.assertLogs("milvus.pyudf.worker", level="ERROR") as logs:
                    self.assert_code(self.execute(self.request(path)), code)
                self.assertEqual(len(logs.records), 1)
                self.assertIn("resource='test'", logs.output[0])
                self.assertIn("stage='L2_rerank'", logs.output[0])
                self.assertIn(f"code={code}", logs.output[0])
                self.assertIsNotNone(logs.records[0].exc_info)
        good, _ = self.wheel()
        self.assertEqual(self.execute(self.request(good)).WhichOneof("result"), "outputs")

    def test_inactive_request_is_logged(self):
        with (
            patch("milvus_pyudf_runtime.worker.decode_inputs", side_effect=RequestInactive()),
            self.assertLogs("milvus.pyudf.worker", level="WARNING") as logs,
            self.assertRaises(grpc.RpcError) as caught,
        ):
            self.execute(self.request(self.directory / "not-loaded.whl"))
        self.assertEqual(caught.exception.code(), grpc.StatusCode.CANCELLED)
        self.assertEqual(len(logs.records), 1)
        self.assertIn("cancelled or expired", logs.output[0])

    def test_contract_errors_and_empty_output(self):
        for expression in (
            "[pa.array([1,2], type=pa.decimal32(5,0))]",
            "[pa.array([1]), pa.array([1,2])]",
            "[[1,2]]",
            "pa.array([1,2])",
            "[pa.array([1,2],type=pa.uint8())]",
        ):
            with self.subTest(expression=expression):
                path, _ = self.wheel(
                    f"import pyarrow as pa\nclass UDF:\n def transform_query(self,p,c): return {expression}\ndef factory(context): return UDF()\n"
                )
                self.assert_code(self.execute(self.request(path)), wire.UDF_FAILED)
        path, _ = self.wheel(
            "class UDF:\n def transform_query(self,p,c): return []\ndef factory(context): return UDF()\n"
        )
        result = decode(self.execute(self.request(path)).outputs)
        self.assertEqual((result[0].num_rows, result[0].num_columns), (0, 0))
        path, _ = self.wheel("class UDF:\n def transform(self,p,c): return c\ndef factory(context): return UDF()\n")
        self.assert_code(self.execute(self.request(path)), wire.UDF_FAILED)

    def test_transform_query_can_change_row_count(self):
        path, _ = self.wheel(
            "import pyarrow as pa\nclass UDF:\n def transform_query(self,p,c): return [pa.array(range(p['rows']), type=pa.int64())]\ndef factory(context): return UDF()\n"
        )
        batches = [pa.record_batch([pa.array(values, type=pa.int64())], names=["c0"]) for values in ([], [1, 2])]
        for rows in (0, 1, 5):
            with self.subTest(rows=rows):
                request = self.request(path, batches)
                request.params.fields["rows"].int64_value = rows
                result = decode(self.execute(request).outputs)
                self.assertEqual([batch.num_rows for batch in result], [rows, rows])

    def test_cross_query_schema_mismatch_does_not_publish_partial_output(self):
        path, _ = self.wheel("""
import pyarrow as pa
class UDF:
 def transform_query(self,p,c):
  return [c[0] if c[0][0].as_py()==1 else pa.array(['x'])]
def factory(context): return UDF()
""")
        batches = [pa.record_batch([pa.array([n])], names=["c0"]) for n in (1, 2)]
        self.assert_code(self.execute(self.request(path, batches)), wire.UDF_FAILED)

    def test_loader_error_origins(self):
        request = self.request(self.directory / "missing.whl")
        self.assert_code(self.execute(request), wire.RESOURCE_NOT_FOUND)
        bad = self.directory / "bad.whl"
        bad.write_bytes(b"invalid")
        self.assert_code(self.execute(self.request(bad)), wire.UDF_FAILED)
        path, _ = self.wheel()
        for error, code in [
            (PermissionError(13, "denied"), wire.RESOURCE_PERMISSION_DENIED),
            (OSError(5, "disk"), wire.RESOURCE_IO_FAILED),
            (MemoryError("alloc"), wire.OUT_OF_MEMORY),
        ]:
            with self.subTest(code=code), patch.object(loader.zipfile, "ZipFile", side_effect=error):
                self.assert_code(self.execute(self.request(path)), code)
        # A failed load is not published: the same key works after I/O recovers.
        self.assertEqual(self.execute(self.request(path)).WhichOneof("result"), "outputs")
        for expression, code in [
            ("raise ValueError('factory')", wire.UDF_FAILED),
            ("raise MemoryError()", wire.OUT_OF_MEMORY),
        ]:
            path, _ = self.wheel(f"def factory(context): {expression}\n")
            self.assert_code(self.execute(self.request(path)), code)

    def test_ipc_rejected_before_factory(self):
        path, package = self.wheel()
        request = self.request(path)
        batch = pa.record_batch([pa.array([1, 2])], names=["c0"])
        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, batch.schema) as writer:
            writer.write_batch(batch)
        for data in (
            b"",
            b"garbage",
            sink.getvalue().to_pybytes(),
        ):
            with self.subTest(size=len(data)):
                broken = wire.ExecuteRequest()
                broken.CopyFrom(request)
                broken.inputs = data
                self.assert_code(self.execute(broken), wire.INTERNAL)
                self.assertNotIn(package, sys.modules)
        request.udf_path = "relative.whl"
        self.assert_code(self.execute(request), wire.INTERNAL)

    def test_arrow_stream_acceptance(self):
        path, _ = self.wheel()
        request = self.request(path)
        batch = pa.record_batch([pa.array([1, 2])], names=["source_name"])
        for data in (
            request.inputs[:-8],
            request.inputs + b"tail",
            request.inputs + request.inputs,
            encode([batch], compression="zstd"),
        ):
            with self.subTest(size=len(data)):
                request.inputs = data
                response = self.execute(request)
                self.assertEqual(response.WhichOneof("result"), "outputs")
                self.assertEqual(len(decode(response.outputs)), 1)

    def test_message_limit_for_request_and_output(self):
        path, _ = self.wheel()
        request = self.request(path)
        request.inputs = b"x" * (2 << 20)
        with self.assertRaises(grpc.RpcError) as caught:
            self.execute(request)
        self.assertEqual(caught.exception.code(), grpc.StatusCode.RESOURCE_EXHAUSTED)
        path, _ = self.wheel(
            "import pyarrow as pa\nclass UDF:\n def transform_query(self,p,c): return [pa.array(['x'*(1<<20)]*len(c[0]))]\ndef factory(context): return UDF()\n"
        )
        with self.assertRaises(grpc.RpcError) as caught:
            self.execute(self.request(path))
        self.assertEqual(caught.exception.code(), grpc.StatusCode.RESOURCE_EXHAUSTED)

    def test_cache_load_once_and_same_instance_runs_concurrently(self):
        path, package = self.wheel("""
import threading
loads=0
barrier=threading.Barrier(3)
class UDF:
 def transform_query(self,p,c):
  barrier.wait(timeout=2)
  return c
def factory(context):
 global loads
 loads+=1
 return UDF()
""")
        request = self.request(path)
        calls = [self.client.Execute.future(request, timeout=3) for _ in range(3)]
        for call in calls:
            self.assertEqual(call.result().WhichOneof("result"), "outputs")
        self.assertEqual(importlib.import_module(package).loads, 1)

    def test_grpc_queues_up_to_rpc_limit_and_skips_expired_requests(self):
        self.server.stop(0).wait()
        self.executor.shutdown(wait=True)
        self.channel.close()
        self.worker = WorkerServer(replace(self.config, grpc_concurrency=2, max_concurrent_rpcs=3))
        self.server, self.executor = self.worker.server, self.worker.executor
        port = self.worker.start("127.0.0.1:0")
        self.channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        grpc.channel_ready_future(self.channel).result(timeout=3)
        self.client = pyudf_pb2_grpc.PyUDFWorkerStub(self.channel)
        path, package = self.wheel("""
import threading
release=threading.Event()
lock=threading.Lock()
calls=0
class UDF:
 def transform_query(self,p,c):
  global calls
  with lock: calls+=1
  release.wait(timeout=5)
  return c
def factory(context): return UDF()
""")
        request = self.request(path)
        running = [self.client.Execute.future(request, timeout=5) for _ in range(2)]
        deadline = time.monotonic() + 3
        while package not in sys.modules or getattr(sys.modules[package], "calls", 0) != 2:
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.01)
        module = sys.modules[package]
        self.release_events.append(module.release)
        queued = self.client.Execute.future(request, timeout=0.5)
        while self.server._state.active_rpc_count != 3:
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.01)
        self.assertEqual(module.calls, 2)
        with self.assertRaises(grpc.RpcError) as caught:
            self.execute(request)
        self.assertEqual(caught.exception.code(), grpc.StatusCode.RESOURCE_EXHAUSTED)
        with self.assertRaises(grpc.RpcError) as caught:
            queued.result()
        self.assertEqual(caught.exception.code(), grpc.StatusCode.DEADLINE_EXCEEDED)
        module.release.set()
        for call in running:
            self.assertEqual(call.result().WhichOneof("result"), "outputs")
        while self.server._state.active_rpc_count:
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.01)
        self.assertEqual(module.calls, 2, "expired queued RPC must not execute the UDF")
        self.assertEqual(self.execute(request).WhichOneof("result"), "outputs")

    def test_cancelled_handler_retains_grpc_capacity_and_stops_later_queries(self):
        self.server.stop(0).wait()
        self.executor.shutdown(wait=True)
        self.channel.close()
        self.worker = WorkerServer(replace(self.config, grpc_concurrency=1, max_concurrent_rpcs=1))
        self.server, self.executor = self.worker.server, self.worker.executor
        port = self.worker.start("127.0.0.1:0")
        self.channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        grpc.channel_ready_future(self.channel).result(timeout=3)
        self.client = pyudf_pb2_grpc.PyUDFWorkerStub(self.channel)
        path, package = self.wheel("""
import threading
entered=threading.Event()
release=threading.Event()
calls=0
class UDF:
 def transform_query(self,p,c):
  global calls
  calls+=1
  entered.set()
  release.wait(timeout=5)
  return c
def factory(context): return UDF()
""")
        batches = [pa.record_batch([pa.array([1])], names=["c0"])] * 2
        request = self.request(path, batches)
        call = self.client.Execute.future(request, timeout=0.5)
        deadline = time.monotonic() + 3
        while package not in sys.modules or not hasattr(sys.modules[package], "entered"):
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.01)
        module = sys.modules[package]
        self.release_events.append(module.release)
        self.assertTrue(module.entered.wait(2))
        with self.assertRaises(grpc.RpcError) as caught:
            call.result()
        self.assertEqual(caught.exception.code(), grpc.StatusCode.DEADLINE_EXCEEDED)
        with self.assertRaises(grpc.RpcError) as caught:
            self.execute(request)
        self.assertEqual(caught.exception.code(), grpc.StatusCode.RESOURCE_EXHAUSTED)
        self.assertEqual(module.calls, 1)
        module.release.set()
        # Wait for real handler completion, not just transport cancellation.
        while self.server._state.active_rpc_count:
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.01)
        self.assertEqual(module.calls, 1)
        self.assertEqual(self.execute(request).WhichOneof("result"), "outputs")
        self.assertEqual(module.calls, 3)

    def test_blocked_factory_does_not_block_a_different_udf(self):
        path, package = self.wheel("""
import threading
entered=threading.Event()
release=threading.Event()
loads=0
class UDF:
 def transform_query(self,p,c): return c
def factory(context):
 global loads
 loads+=1
 entered.set()
 release.wait(timeout=5)
 return UDF()
""")
        request = self.request(path)
        first = self.client.Execute.future(request, timeout=3)
        deadline = time.monotonic() + 2
        while package not in sys.modules or not hasattr(sys.modules[package], "entered"):
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.01)
        module = sys.modules[package]
        self.release_events.append(module.release)
        self.assertTrue(module.entered.wait(1))
        waiting = self.client.Execute.future(request, timeout=0.15)
        other, _ = self.wheel()
        self.assertEqual(self.execute(self.request(other)).WhichOneof("result"), "outputs")
        with self.assertRaises(grpc.RpcError) as caught:
            waiting.result()
        self.assertEqual(caught.exception.code(), grpc.StatusCode.DEADLINE_EXCEEDED)
        self.assertEqual(module.loads, 1)
        module.release.set()
        self.assertEqual(first.result().WhichOneof("result"), "outputs")
        self.assertEqual(self.execute(request).WhichOneof("result"), "outputs")
        self.assertEqual(module.loads, 1)

    def test_cancelled_module_wait_releases_grpc_capacity(self):
        path, package = self.wheel("""
import threading
entered=threading.Event()
release=threading.Event()
loads=[]
entered.set()
release.wait(timeout=5)
class UDF:
 def transform_query(self,p,c): return c
def factory(context):
 loads.append(context.stage)
 return UDF()
""")
        first = self.client.Execute.future(self.request(path), timeout=5)
        deadline = time.monotonic() + 3
        while package not in sys.modules or not hasattr(sys.modules[package], "release"):
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.01)
        module = sys.modules[package]
        self.release_events.append(module.release)
        self.assertTrue(module.entered.wait(1))
        request = self.request(path)
        request.stage = "other-stage"
        waiting = self.client.Execute.future(request, timeout=0.2)
        with self.assertRaises(grpc.RpcError) as caught:
            waiting.result()
        self.assertEqual(caught.exception.code(), grpc.StatusCode.DEADLINE_EXCEEDED)
        # Transport completion alone is insufficient: the waiting handler must
        # finish while the module initializer remains blocked.
        while self.server._state.active_rpc_count != 1:
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.01)
        other, _ = self.wheel()
        self.assertEqual(self.execute(self.request(other)).WhichOneof("result"), "outputs")
        self.assertFalse(module.loads)
        module.release.set()
        self.assertEqual(first.result().WhichOneof("result"), "outputs")
        self.assertEqual(module.loads, ["L2_rerank"])
        self.assertEqual(self.execute(request).WhichOneof("result"), "outputs")
        self.assertEqual(module.loads, ["L2_rerank", "other-stage"])

    def test_dictionary_ipc_and_metadata_read_failure(self):
        path, _ = self.wheel(
            "class UDF:\n def transform_query(self,p,c): return [c[0].dictionary_decode()]\ndef factory(context): return UDF()\n"
        )
        batch = pa.record_batch([pa.array(["a", None, "b"]).dictionary_encode()], names=["c0"])
        result = self.execute(self.request(path, [batch]))
        self.assertEqual(decode(result.outputs)[0].column(0).to_pylist(), ["a", None, "b"])
        request = self.request(path, [batch])
        request.inputs = encode([batch], compression="zstd")
        self.assertEqual(decode(self.execute(request).outputs)[0].column(0).to_pylist(), ["a", None, "b"])
        path, _ = self.wheel()
        initial_open = loader.zipfile.ZipFile(path)
        with patch.object(
            loader.zipfile, "ZipFile", side_effect=[initial_open, PermissionError(13, "metadata denied")]
        ):
            self.assert_code(self.execute(self.request(path)), wire.RESOURCE_PERMISSION_DENIED)

    def test_invalid_parameter_depth_and_output_encoding_failure(self):
        path, _ = self.wheel()
        request = self.request(path)
        value = request.params.fields["nested"]
        for _ in range(65):
            value = value.object_value.fields["next"]
        value.int64_value = 1
        with self.assertRaises(WorkerError) as direct:
            _params_object(request.params)
        self.assertEqual(direct.exception.code, wire.INVALID_ARGUMENT)
        # Protobuf's own message-recursion limit can reject this before handler dispatch.
        with self.assertRaises(grpc.RpcError) as caught:
            self.execute(request)
        self.assertEqual(caught.exception.code(), grpc.StatusCode.INTERNAL)
        with patch("milvus_pyudf_runtime.worker.OutputIPC.finish", side_effect=RuntimeError("encoder failed")):
            self.assert_code(self.execute(self.request(path)), wire.INTERNAL)

    def test_signal_exits_worker_without_draining_udf(self):
        entered = self.directory / "entered"
        closed = self.directory / "closed"
        finalized = self.directory / "finally"
        path, _ = self.wheel(f"""
import time
from pathlib import Path
class UDF:
 def transform_query(self,p,c):
  Path({str(entered)!r}).touch()
  try: time.sleep(30)
  finally: Path({str(finalized)!r}).touch()
  return c
 def close(self): Path({str(closed)!r}).touch()
def factory(context): return UDF()
""")
        with socket.socket() as reserve:
            reserve.bind(("127.0.0.1", 0))
            port = reserve.getsockname()[1]
        runtime_root = Path(__file__).resolve().parents[1]
        code = """
import sys
sys.path.insert(0, sys.argv[1])
from milvus_pyudf_runtime.worker import WorkerServer
from milvus_pyudf_runtime.config import ServerConfig
config=ServerConfig(sys.argv[2],1,2,100,1048576,5000)
WorkerServer(config).run()
"""
        child = subprocess.Popen(
            [sys.executable, "-I", "-c", code, str(runtime_root), f"127.0.0.1:{port}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        try:
            grpc.channel_ready_future(channel).result(timeout=5)
            rpc = pyudf_pb2_grpc.PyUDFWorkerStub(channel).Execute.future(self.request(path), timeout=10)
            deadline = time.monotonic() + 5
            while not entered.exists():
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.01)
            child.terminate()
            self.assertEqual(child.wait(timeout=4), -signal.SIGTERM)
            self.assertFalse(closed.exists())
            self.assertFalse(finalized.exists())
            with self.assertRaises(grpc.RpcError):
                rpc.result()
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=3)
            channel.close()
            child.stderr.close()

    def test_runtime_failure_and_bounded_error_message(self):
        path, _ = self.wheel()
        with patch("milvus_pyudf_runtime.worker.decode_inputs", side_effect=RuntimeError("internal")):
            self.assert_code(self.execute(self.request(path)), wire.INTERNAL)
        path, _ = self.wheel(
            "class UDF:\n def transform_query(self,p,c): raise ValueError('错'*20000)\ndef factory(context): return UDF()\n"
        )
        response = self.execute(self.request(path))
        self.assert_code(response, wire.UDF_FAILED)
        self.assertLessEqual(len(response.error.message.encode()), 8192)


if __name__ == "__main__":
    unittest.main()
