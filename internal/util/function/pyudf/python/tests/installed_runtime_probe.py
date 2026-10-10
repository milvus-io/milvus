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

"""Isolated installed-wheel probe; never add repository paths to sys.path."""

import importlib.metadata
import os
import signal
import socket
import subprocess
import sys
import zipfile
from pathlib import Path

import grpc
import milvus_pyudf_runtime
import pyarrow as pa
from milvus_pyudf_runtime.proto import pyudf_pb2 as wire
from milvus_pyudf_runtime.proto import pyudf_pb2_grpc


def main():
    source_root = Path(sys.argv[1]).resolve()
    installed = Path(milvus_pyudf_runtime.__file__).resolve()
    distribution = importlib.metadata.distribution("milvus-pyudf-runtime")
    expected = Path(distribution.locate_file("milvus_pyudf_runtime/__init__.py")).resolve()
    assert installed == expected and not installed.is_relative_to(source_root), installed
    assert not wire.DESCRIPTOR.dependencies
    assert not any(path.name in ("common_pb2.py", "schema_pb2.py") for path in distribution.files)
    assert all(not Path(path).resolve().is_relative_to(source_root) for path in sys.path)

    wheel = Path.cwd() / "installed_udf.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(
            "installed_udf/__init__.py",
            """
import pyarrow as pa
class UDF:
 def transform_query(self, params, columns):
  assert len(columns) == 2 and columns[0] is columns[1]
  values = [value.as_py() * params['factor'] for value in columns[0]]
  return [pa.array(values, type=pa.int64())]
def factory(context): return UDF()
""",
        )
        archive.writestr("installed_udf.dist-info/entry_points.txt", "[milvus.pyudf]\nmain=installed_udf:factory\n")

    with socket.socket() as reserve:
        reserve.bind(("127.0.0.1", 0))
        address = f"127.0.0.1:{reserve.getsockname()[1]}"
    command = [
        sys.executable,
        "-I",
        "-m",
        "milvus_pyudf_runtime.supervisor",
        "--address",
        address,
        "--worker-count",
        "1",
        "--grpc-concurrency",
        "2",
        "--max-concurrent-rpcs",
        "10",
        "--max-message-bytes",
        str(1 << 20),
        "--shutdown-timeout-ms",
        "5000",
    ]
    log_path = Path.cwd() / "supervisor.log"
    process = None
    try:
        with log_path.open("wb") as log:
            process = subprocess.Popen(command, stdout=log, stderr=log, start_new_session=True)
            try:
                with grpc.insecure_channel(address) as channel:
                    grpc.channel_ready_future(channel).result(timeout=10)
                    batches = [
                        pa.record_batch([pa.array(values, type=pa.int64())], names=["c0"]) for values in ([], [2, 3])
                    ]
                    sink = pa.BufferOutputStream()
                    with pa.ipc.new_stream(sink, batches[0].schema) as writer:
                        for batch in batches:
                            writer.write_batch(batch)
                    request = wire.ExecuteRequest(
                        resource_name="installed",
                        udf_path=str(wheel),
                        stage="L2_rerank",
                        inputs=sink.getvalue().to_pybytes(),
                        input_column_indices=[0, 0],
                    )
                    request.params.fields["factor"].int64_value = 3
                    response = pyudf_pb2_grpc.PyUDFWorkerStub(channel).Execute(request, timeout=5)
                    assert response.WhichOneof("result") == "outputs", response
                    with pa.ipc.open_stream(response.outputs) as reader:
                        outputs = list(reader)
                    assert [batch.num_rows for batch in outputs] == [0, 2], outputs
                    assert outputs[1].column(0).to_pylist() == [6, 9], outputs
                process.terminate()
                assert process.wait(timeout=5) == 0
            finally:
                # Clean up this probe's process group even if startup or an RPC fails.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=5)
    except BaseException:
        if log_path.exists():
            sys.stderr.write(log_path.read_text())
        raise


if __name__ == "__main__":
    main()
