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

"""Linux/macOS supervisor tests use real workers and verify process exit."""

import ctypes
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import zipfile
from pathlib import Path

import grpc
import pyarrow as pa
from grpc_health.v1 import health_pb2, health_pb2_grpc

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
from milvus_pyudf_runtime.proto import pyudf_pb2, pyudf_pb2_grpc  # noqa: E402

_HELPER = _ROOT.parent / "testdata/supervisor_probe.py"
_SERVICE = "milvus.proto.pyudf.PyUDFWorker"


@unittest.skipUnless(sys.platform in ("linux", "darwin"), "supervisor requires Linux or macOS")
class SupervisorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if sys.platform != "linux":
            return
        # Test-only adoption lets assertions reap workers after killing their
        # supervisor. Product Milvus does not change process-wide subreaping.
        cls.libc = ctypes.CDLL(None, use_errno=True)
        cls.was_subreaper = ctypes.c_int()
        if cls.libc.prctl(37, ctypes.byref(cls.was_subreaper), 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "PR_GET_CHILD_SUBREAPER")
        if cls.libc.prctl(36, 1, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")

    @classmethod
    def tearDownClass(cls):
        if sys.platform == "linux":
            cls.libc.prctl(36, cls.was_subreaper.value, 0, 0, 0)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pyudf-supervisor-")
        self.directory = Path(self.temp.name)
        self.pools = []
        self.channels = []
        self.last_health = None

    def tearDown(self):
        for channel in self.channels:
            channel.close()
        for pool in reversed(self.pools):
            process = pool["process"]
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=3)
            pids = self.workers(pool)
            parent_file = pool["directory"] / "supervisor.pid"
            if parent_file.exists():
                pids.append(int(parent_file.read_text()))
            deadline = time.monotonic() + 3
            while pids and time.monotonic() < deadline:
                for pid in list(pids):
                    try:
                        reaped, _ = os.waitpid(pid, os.WNOHANG)
                        if reaped:
                            pids.remove(pid)
                    except ChildProcessError:
                        pids.remove(pid)
                if pids:
                    time.sleep(0.01)
            pool["log"].close()
        self.temp.cleanup()

    def eventually(self, condition, timeout=5):
        deadline = time.monotonic() + timeout
        while not condition():
            if time.monotonic() >= deadline:
                logs = "\n".join((pool["directory"] / "process.log").read_text() for pool in self.pools)
                self.fail(f"condition did not become true; last health={self.last_health}\n" + logs)
            time.sleep(0.02)

    def start(self, mode="normal", count=2, address=None):
        directory = self.directory / str(len(self.pools))
        directory.mkdir()
        if address is None:
            with socket.socket() as reserve:
                reserve.bind(("127.0.0.1", 0))
                address = f"127.0.0.1:{reserve.getsockname()[1]}"
        output = (directory / "process.log").open("wb")
        args = [
            "--address",
            address,
            "--worker-count",
            str(count),
            "--grpc-concurrency",
            "4",
            "--max-concurrent-rpcs",
            "100",
            "--max-message-bytes",
            str(1 << 20),
            "--shutdown-timeout-ms",
            "5000",
        ]
        process = subprocess.Popen(
            [sys.executable, "-I", str(_HELPER), mode, str(directory), *args],
            stdout=output,
            stderr=output,
            start_new_session=True,
        )
        pool = {"process": process, "directory": directory, "address": address, "log": output}
        self.pools.append(pool)
        return pool

    @staticmethod
    def workers(pool):
        manifest = pool["directory"] / "workers.jsonl"
        if not manifest.exists():
            return []
        return [json.loads(line)["pid"] for line in manifest.read_text().splitlines() if line.endswith("}")]

    def health(self, pool):
        channel = grpc.insecure_channel(
            pool["address"],
            options=(
                ("grpc.use_local_subchannel_pool", 1),
                ("grpc.initial_reconnect_backoff_ms", 100),
                ("grpc.min_reconnect_backoff_ms", 100),
                ("grpc.max_reconnect_backoff_ms", 100),
            ),
        )
        self.channels.append(channel)
        grpc.channel_ready_future(channel).result(timeout=5)
        return health_pb2_grpc.HealthStub(channel)

    def status(self, client):
        try:
            value = client.Check(health_pb2.HealthCheckRequest(service=_SERVICE), timeout=1).status
            self.last_health = value
            return value
        except grpc.RpcError as exc:
            self.last_health = (exc.code(), exc.details())
            return None

    def ready(self, pool):
        client = self.health(pool)
        self.eventually(lambda: self.status(client) == health_pb2.HealthCheckResponse.SERVING)
        return client

    @staticmethod
    def alive(pid):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        return True

    def assert_gone(self, pids):
        self.eventually(lambda: all(not self.alive(pid) for pid in pids))

    def assert_orphan_exited(self, pid, expected_signal=None):
        if sys.platform != "linux":
            # macOS adopts orphans through launchd; this test cannot waitpid them.
            self.assert_gone([pid])
            return
        result = []

        def reaped():
            child, status = os.waitpid(pid, os.WNOHANG)
            if child:
                result.append(status)
            return bool(child)

        self.eventually(reaped)
        if expected_signal is None:
            self.assertEqual(result, [0])
        else:
            self.assertTrue(os.WIFSIGNALED(result[0]))
            self.assertEqual(os.WTERMSIG(result[0]), expected_signal)

    def stop(self, pool):
        pids = self.workers(pool)
        pool["process"].terminate()
        self.assertEqual(pool["process"].wait(timeout=10), 0)
        self.assert_gone(pids)
        self.assertFalse(list(pool["directory"].glob("atexit-*")))
        return pids

    def test_health_reports_local_worker_without_waiting_for_pool(self):
        pool = self.start("delayed")
        client = self.health(pool)
        self.eventually(lambda: self.status(client) == health_pb2.HealthCheckResponse.SERVING)
        self.assertEqual(len(self.workers(pool)), 2)
        (pool["directory"] / "release").touch()
        self.eventually(lambda: self.status(client) == health_pb2.HealthCheckResponse.SERVING)
        self.stop(pool)

    def test_worker_crash_is_reaped_before_replacement(self):
        pool = self.start()
        self.ready(pool)
        original = self.workers(pool)
        os.kill(original[0], signal.SIGKILL)
        self.eventually(lambda: len(self.workers(pool)) >= 3)
        self.assert_gone([original[0]])
        self.ready(pool)
        self.assertEqual(sum(self.alive(pid) for pid in self.workers(pool)), 2)
        self.stop(pool)

    def test_execute_recovers_after_worker_crash_and_reloads_udf(self):
        pool = self.start(count=1)
        self.ready(pool)
        original = self.workers(pool)[0]
        loads = pool["directory"] / "loads.jsonl"
        wheel = pool["directory"] / "recovery.whl"
        source = f"""
import os
import pyarrow as pa
class UDF:
 def transform_query(self, params, columns):
  if params.get('crash'): os._exit(23)
  values = [value.as_py() * 3 for value in columns[0]]
  return [pa.array(values, type=pa.int64()), pa.array([os.getpid()] * len(values), type=pa.int64())]
def factory(context):
 with open({str(loads)!r}, 'a') as output: output.write(str(os.getpid()) + '\\n')
 return UDF()
"""
        with zipfile.ZipFile(wheel, "w") as archive:
            archive.writestr("recovery/__init__.py", source)
            archive.writestr("recovery.dist-info/entry_points.txt", "[milvus.pyudf]\nmain=recovery:factory\n")
        batch = pa.record_batch([pa.array([1, 2], type=pa.int64())], names=["c0"])
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        request = pyudf_pb2.ExecuteRequest(
            resource_name="recovery", udf_path=str(wheel), stage="L2_rerank", inputs=sink.getvalue().to_pybytes()
        )

        def connect():
            channel = grpc.insecure_channel(
                pool["address"], options=(("grpc.use_local_subchannel_pool", 1), ("grpc.enable_retries", 0))
            )
            self.channels.append(channel)
            grpc.channel_ready_future(channel).result(timeout=5)
            return pyudf_pb2_grpc.PyUDFWorkerStub(channel)

        def execute(client, pid):
            response = client.Execute(request, timeout=3)
            self.assertEqual(response.WhichOneof("result"), "outputs", response)
            with pa.ipc.open_stream(response.outputs) as reader:
                batches = list(reader)
            self.assertEqual(len(batches), 1)
            self.assertEqual(batches[0].column(0).to_pylist(), [3, 6])
            self.assertEqual(batches[0].column(1).to_pylist(), [pid, pid])

        client = connect()
        execute(client, original)
        execute(client, original)
        request.params.fields["crash"].bool_value = True
        with self.assertRaises(grpc.RpcError) as caught:
            client.Execute(request, timeout=3)
        self.assertEqual(caught.exception.code(), grpc.StatusCode.UNAVAILABLE)
        request.params.fields.clear()
        self.eventually(lambda: len(self.workers(pool)) == 2)
        self.assert_gone([original])
        replacement = self.workers(pool)[-1]
        self.assertNotEqual(replacement, original)
        client = connect()
        execute(client, replacement)
        execute(client, replacement)
        self.assertEqual([int(pid) for pid in loads.read_text().splitlines()], [original, replacement])
        self.assertIsNone(pool["process"].poll())
        self.stop(pool)

    def test_replacement_fork_failure_retries_without_stopping_survivors(self):
        pool = self.start("replacement_fork_failure")
        self.ready(pool)
        original = self.workers(pool)
        os.kill(original[0], signal.SIGKILL)
        self.eventually(lambda: len(self.workers(pool)) >= 3)
        self.assertTrue((pool["directory"] / "fork-failed").exists())
        self.assertTrue(self.alive(original[1]))
        self.ready(pool)
        self.stop(pool)

    def test_fork_and_worker_initialization_failures_are_retried(self):
        for mode in ("partial_fork", "fail_worker"):
            with self.subTest(mode=mode):
                pool = self.start(mode)
                self.ready(pool)  # Test-only query of the surviving worker.
                if mode == "partial_fork":
                    self.eventually(lambda: "worker fork failed" in (pool["directory"] / "process.log").read_text())
                else:
                    self.eventually(lambda: len(self.workers(pool)) >= 3)
                self.assertIsNone(pool["process"].poll())
                self.stop(pool)

    def test_stop_during_startup_does_not_replace_workers(self):
        pool = self.start("delayed")
        client = self.health(pool)
        self.eventually(lambda: self.status(client) == health_pb2.HealthCheckResponse.SERVING)
        self.stop(pool)
        self.assertEqual(len(self.workers(pool)), 2)

    def test_alive_unready_worker_is_not_replaced(self):
        pool = self.start("replacement_hang", count=1)
        self.ready(pool)
        os.kill(self.workers(pool)[0], signal.SIGKILL)
        self.eventually(lambda: len(self.workers(pool)) == 2)
        replacement = self.workers(pool)[1]
        time.sleep(1.2)
        self.assertTrue(self.alive(replacement))
        self.assertEqual(len(self.workers(pool)), 2)
        self.stop(pool)

    def test_parent_death_does_not_stop_pool_without_signal(self):
        pool = self.start("parent")
        self.ready(pool)
        pids = self.workers(pool)
        supervisor = int((pool["directory"] / "supervisor.pid").read_text())
        pool["process"].kill()
        pool["process"].wait(timeout=3)
        time.sleep(1.2)
        self.assertTrue(self.alive(supervisor))
        os.kill(supervisor, signal.SIGTERM)
        self.assert_orphan_exited(supervisor)
        self.assert_gone(pids)

    def test_worker_exits_only_when_signalled(self):
        pool = self.start(count=1)
        self.ready(pool)
        worker = self.workers(pool)[0]
        pool["process"].kill()
        pool["process"].wait(timeout=3)
        time.sleep(1.2)
        self.assertTrue(self.alive(worker))
        os.kill(worker, signal.SIGTERM)
        self.assert_orphan_exited(worker, signal.SIGTERM)

    def test_blocked_udf_is_terminated_without_finalizers(self):
        for mode in ("normal", "ignore_term"):
            with self.subTest(mode=mode):
                pool = self.start(mode, count=1)
                self.ready(pool)
                directory = pool["directory"]
                wheel = directory / "blocking.whl"
                source = f"""
import time
from pathlib import Path
class UDF:
 def transform_query(self,p,c):
  Path({str(directory / "entered")!r}).touch()
  try: time.sleep(60)
  finally: Path({str(directory / "finally")!r}).touch()
  return c
 def close(self): Path({str(directory / "close")!r}).touch()
def factory(ctx): return UDF()
"""
                with zipfile.ZipFile(wheel, "w") as archive:
                    archive.writestr("blocking/__init__.py", source)
                    archive.writestr("blocking.dist-info/entry_points.txt", "[milvus.pyudf]\nmain=blocking:factory\n")
                batch = pa.record_batch([pa.array([1])], names=["c0"])
                sink = pa.BufferOutputStream()
                with pa.ipc.new_stream(sink, batch.schema) as writer:
                    writer.write_batch(batch)
                channel = grpc.insecure_channel(pool["address"])
                self.channels.append(channel)
                request = pyudf_pb2.ExecuteRequest(
                    resource_name="blocking", udf_path=str(wheel), stage="custom", inputs=sink.getvalue().to_pybytes()
                )
                rpc = pyudf_pb2_grpc.PyUDFWorkerStub(channel).Execute.future(request, timeout=10)
                self.eventually(lambda: (directory / "entered").exists())
                begin = time.monotonic()
                self.stop(pool)
                if mode == "ignore_term":
                    self.assertGreaterEqual(time.monotonic() - begin, 4.9)
                self.assertFalse((directory / "finally").exists())
                self.assertFalse((directory / "close").exists())
                with self.assertRaises(grpc.RpcError):
                    rpc.result()

    def test_bind_failure_restarts_worker_without_health_checks(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            address = f"127.0.0.1:{listener.getsockname()[1]}"
            pool = self.start(count=1, address=address)
            self.eventually(lambda: len(self.workers(pool)) >= 2)
            self.assertIsNone(pool["process"].poll())
            self.stop(pool)

    def test_no_ownership_guard_between_pools(self):
        first = self.start(count=1)
        self.ready(first)
        second = self.start(count=1, address=first["address"])
        # Health alone could hit the first pool. Its own ready log proves that
        # the second pool also started: duplicate addresses are a deployment error.
        self.eventually(lambda: "test worker listening" in (second["directory"] / "process.log").read_text())
        self.assertIsNone(second["process"].poll())
        self.stop(second)
        self.assertIsNone(first["process"].poll())
        self.stop(first)


if __name__ == "__main__":
    unittest.main()
