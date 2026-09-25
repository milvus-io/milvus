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

"""P1 protocol/configuration tests; no worker execution service is implemented."""

from __future__ import annotations

import subprocess
import sys
import unittest
from dataclasses import replace
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from milvus_pyudf_runtime.config import ServerConfig, parse_startup_args  # noqa: E402
from milvus_pyudf_runtime.proto import pyudf_pb2, pyudf_pb2_grpc  # noqa: E402


def config():
    return ServerConfig("127.0.0.1:19090", 1, 10, 100, 64 << 20, 30000)


class WorkerProtocolTests(unittest.TestCase):
    def test_protocol_does_not_register_milvus_api_descriptors(self):
        # Different SDK versions may own these names in the default pool.
        # Check both import orders in fresh processes; the worker must neither
        # reserve those names nor depend on the SDK's versions of their types.
        code = """
import sys
from google.protobuf import descriptor_pb2, descriptor_pool
sys.path.insert(0, sys.argv[1])
pool = descriptor_pool.Default()
def register_sdk():
    for name in ('common', 'schema'):
        descriptor = descriptor_pb2.FileDescriptorProto(
            name=name + '.proto', package='milvus.proto.' + name, syntax='proto3')
        descriptor.message_type.add(name='SDKOnlyMessage')
        pool.AddSerializedFile(descriptor.SerializeToString())
def import_worker():
    from milvus_pyudf_runtime import WorkerServer
    from milvus_pyudf_runtime.proto import pyudf_pb2
    assert callable(WorkerServer)
    assert not pyudf_pb2.DESCRIPTOR.dependencies
    assert pyudf_pb2.FunctionParamObject.DESCRIPTOR.file is pyudf_pb2.DESCRIPTOR
if sys.argv[2] == 'sdk_first':
    register_sdk()
    import_worker()
else:
    import_worker()
    for name in ('common.proto', 'schema.proto'):
        try:
            pool.FindFileByName(name)
        except KeyError:
            pass
        else:
            raise AssertionError('runtime registered ' + name)
    register_sdk()
"""
        for order in ("sdk_first", "runtime_first"):
            with self.subTest(order=order):
                subprocess.run([sys.executable, "-I", "-c", code, str(_ROOT), order], check=True, timeout=15)

    def test_only_execute_rpc_and_field_contract(self):
        service = pyudf_pb2.DESCRIPTOR.services_by_name["PyUDFWorker"]
        self.assertEqual([m.name for m in service.methods], ["Execute"])
        self.assertFalse(service.methods[0].client_streaming)
        self.assertFalse(service.methods[0].server_streaming)
        self.assertTrue(callable(pyudf_pb2_grpc.PyUDFWorkerStub))
        request = pyudf_pb2.ExecuteRequest()
        self.assertEqual(
            {field.name: field.number for field in request.DESCRIPTOR.fields},
            {"resource_name": 2, "udf_path": 3, "stage": 4, "params": 5, "inputs": 6, "input_column_indices": 7},
        )
        request.params.fields["large"].int64_value = -(1 << 63)
        request.params.fields["bytes"].bytes_value = b"\x00\xff"
        request.params.fields["empty"].object_value.SetInParent()
        request.input_column_indices.extend([1, 0, 1])
        decoded = pyudf_pb2.ExecuteRequest.FromString(request.SerializeToString())
        self.assertEqual(decoded, request)
        self.assertEqual(decoded.params.fields["empty"].WhichOneof("value"), "object_value")
        response = pyudf_pb2.ExecuteResponse(outputs=b"ipc")
        self.assertEqual(response.WhichOneof("result"), "outputs")
        response.error.code = pyudf_pb2.UDF_FAILED
        self.assertEqual(response.WhichOneof("result"), "error")
        error = pyudf_pb2.ExecuteError(code=pyudf_pb2.RESOURCE_PERMISSION_DENIED, message="读取 UDF：权限不足")
        self.assertEqual(set(error.DESCRIPTOR.fields_by_name), {"code", "message"})
        self.assertEqual(pyudf_pb2.ExecuteError.FromString(error.SerializeToString()), error)

    def test_startup_args(self):
        args = [
            "--address",
            "127.0.0.1:19090",
            "--worker-count",
            "1",
            "--grpc-concurrency",
            "10",
            "--max-concurrent-rpcs",
            "100",
            "--max-message-bytes",
            "67108864",
            "--shutdown-timeout-ms",
            "30000",
        ]
        self.assertEqual(parse_startup_args(args), config())
        for field, value in (
            ("worker_count", 0),
            ("grpc_concurrency", True),
            ("max_concurrent_rpcs", 0),
            ("max_concurrent_rpcs", 1 << 31),
            ("max_message_bytes", 1),
            ("shutdown_timeout_ms", 4999),
            ("shutdown_timeout_ms", 60001),
            ("address", "localhost:19090"),
            ("address", "0.0.0.0:19090"),
            ("address", "[::1]:19090"),
            ("address", "127.0.0.1:019090"),
        ):
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                replace(config(), **{field: value}).validate()
        replace(config(), worker_count=100, grpc_concurrency=100).validate()
        for timeout in (5000, 60000):
            replace(config(), shutdown_timeout_ms=timeout).validate()

    def test_supervisor_imports_remain_lightweight(self):
        code = """
import sys
sys.path.insert(0, sys.argv[1])
import milvus_pyudf_runtime
from milvus_pyudf_runtime import config, supervisor  # noqa: E402
assert 'grpc' not in sys.modules
assert 'pyarrow' not in sys.modules
assert 'milvus_pyudf_runtime.loader' not in sys.modules
assert 'milvus_pyudf_runtime.instance' not in sys.modules
from milvus_pyudf_runtime import PyUDFInstance, PyUDFLoader, WorkerServer  # noqa: E402
assert callable(PyUDFInstance) and callable(PyUDFLoader) and callable(WorkerServer)
"""
        subprocess.run([sys.executable, "-I", "-c", code, str(_ROOT)], check=True)


if __name__ == "__main__":
    unittest.main()
