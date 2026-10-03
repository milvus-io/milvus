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

"""Standalone closed-loop gRPC benchmark of the checked-out PyUDF worker pool."""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import math
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import zipfile
from array import array
from collections import Counter
from importlib.metadata import version
from pathlib import Path

import grpc
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from milvus_pyudf_runtime.proto import pyudf_pb2 as wire  # noqa: E402

METHOD = "/milvus.proto.pyudf.PyUDFWorker/Execute"
UDF = """
import pyarrow.compute as pc
class Add:
    def transform_query(self, params, columns):
        return [pc.add(columns[0], columns[1])]
def create_udf(context):
    return Add()
"""


def arguments():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--duration", type=float, default=10, help="steady-state submission window in seconds")
    parser.add_argument("--warmup", type=float, default=2, help="warmup seconds per client process")
    parser.add_argument("--rows", type=int, default=128, help="rows per query batch")
    parser.add_argument("--queries", type=int, default=1, help="query batches per RPC")
    parser.add_argument("--workers", type=int, default=1, help="server worker processes")
    parser.add_argument("--server-threads", type=int, default=10, help="gRPC executor threads per worker")
    parser.add_argument("--server-max-rpcs", type=int, default=100, help="admitted RPCs per worker")
    parser.add_argument("--client-processes", type=int, default=1, help="independent load generator processes")
    parser.add_argument("--concurrency", type=int, default=16, help="in-flight RPCs per client process")
    parser.add_argument("--connections", type=int, default=10, help="independent channels per client process")
    parser.add_argument("--rpc-timeout", type=float, default=5, help="per-RPC timeout in seconds")
    parser.add_argument("--startup-timeout", type=float, default=30, help="channel readiness timeout in seconds")
    parser.add_argument("--max-message-bytes", type=int, default=64 << 20, help="server and client message limit")
    parser.add_argument("--json", type=Path, help="also save the complete report to this file")
    parser.add_argument("--client-dir", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--client-index", type=int, default=0, help=argparse.SUPPRESS)
    args = parser.parse_args()
    for key in (
        "queries",
        "workers",
        "server_threads",
        "server_max_rpcs",
        "client_processes",
        "concurrency",
        "connections",
    ):
        if not 1 <= getattr(args, key) < 1 << 31:
            parser.error(f"--{key.replace('_', '-')} must be a positive int32")
    for key in ("duration", "rpc_timeout", "startup_timeout", "warmup"):
        value = getattr(args, key)
        if not math.isfinite(value) or value < 0 or (key != "warmup" and value == 0):
            parser.error(
                f"--{key.replace('_', '-')} must be finite and {'nonnegative' if key == 'warmup' else 'positive'}"
            )
    if args.rows < 0 or not 1 << 20 <= args.max_message_bytes <= 1 << 30:
        parser.error("rows must be nonnegative; max-message-bytes must be in 1 MiB..1 GiB")
    if sys.platform not in ("linux", "darwin"):
        parser.error("the worker supervisor requires Linux or macOS")
    return args


def prepare(directory, args):
    wheel = directory / "bench_add.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("bench_add/__init__.py", UDF)
        archive.writestr("bench_add.dist-info/entry_points.txt", "[milvus.pyudf]\nmain=bench_add:create_udf\n")
    a = pa.array(range(args.rows), type=pa.int64())
    b = pa.array([1] * args.rows, type=pa.int64())
    batch = pa.record_batch([a, b], names=["a", "b"])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        for _ in range(args.queries):
            writer.write_batch(batch)
    request = wire.ExecuteRequest(
        resource_name="bench_add",
        udf_path=str(wheel),
        stage="L2_rerank",
        inputs=sink.getvalue().to_pybytes(),
        input_column_indices=[0, 1],
    )
    payload = request.SerializeToString()
    if len(payload) > args.max_message_bytes:
        raise ValueError(f"request is {len(payload)} bytes, above max-message-bytes={args.max_message_bytes}")
    (directory / "request.pb").write_bytes(payload)
    return len(payload)


def validate(response, args):
    if response.WhichOneof("result") != "outputs":
        raise RuntimeError(f"validation RPC failed: {response}")
    expected = pa.array(range(1, args.rows + 1), type=pa.int64())
    with pa.ipc.open_stream(response.outputs) as reader:
        batches = list(reader)
    if len(batches) != args.queries:
        raise RuntimeError("incorrect output query count")
    for batch in batches:
        if batch.num_columns != 1 or not batch.column(0).equals(expected):
            raise RuntimeError("incorrect a + b output")


def write_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value))
    temporary.replace(path)


async def load_client(args):
    directory = args.client_dir
    address = (directory / "address").read_text()
    request = (directory / "request.pb").read_bytes()
    options = (
        ("grpc.use_local_subchannel_pool", 1),
        ("grpc.enable_retries", 0),
        ("grpc.max_send_message_length", args.max_message_bytes),
        ("grpc.max_receive_message_length", args.max_message_bytes),
    )
    channels = [grpc.aio.insecure_channel(address, options=options) for _ in range(args.connections)]
    methods = [
        channel.unary_unary(METHOD, response_deserializer=wire.ExecuteResponse.FromString) for channel in channels
    ]
    cursor = itertools.count()

    async def phase(deadline, measure):
        latencies = array("d")
        failures = Counter()
        examples = {}
        output_bytes = 0

        async def lane():
            nonlocal output_bytes
            while time.monotonic() < deadline:
                started = time.perf_counter()
                try:
                    response = await methods[next(cursor) % len(methods)](request, timeout=args.rpc_timeout)
                    elapsed_ms = (time.perf_counter() - started) * 1000
                    result = response.WhichOneof("result")
                    if result == "outputs":
                        if measure:
                            latencies.append(elapsed_ms)
                            output_bytes += len(response.outputs)
                        continue
                    key = f"worker:{response.error.code}" if result == "error" else "protocol:missing_result"
                    detail = response.error.message if result == "error" else "response has no result"
                except grpc.RpcError as exc:
                    key, detail = f"grpc:{exc.code().name}", exc.details()
                failures[key] += 1
                examples.setdefault(key, str(detail)[:300])

        await asyncio.gather(*(lane() for _ in range(args.concurrency)))
        return latencies, dict(failures), examples, output_bytes

    try:
        await asyncio.wait_for(asyncio.gather(*(c.channel_ready() for c in channels)), args.startup_timeout)
        first = time.perf_counter()
        response = await methods[0](request, timeout=args.rpc_timeout)
        first_ms = (time.perf_counter() - first) * 1000
        validate(response, args)
        _, warmup_errors, _, _ = await phase(time.monotonic() + args.warmup, False)
        write_json(directory / f"ready-{args.client_index}.json", {"first_rpc_ms": first_ms})
        signal_file = directory / "start.json"
        wait_deadline = time.monotonic() + args.startup_timeout + args.warmup + args.rpc_timeout + 10
        while not signal_file.exists():
            if time.monotonic() >= wait_deadline:
                raise TimeoutError("benchmark start barrier timed out")
            await asyncio.sleep(0.01)
        start = json.loads(signal_file.read_text())["start"]
        await asyncio.sleep(max(0, start - time.monotonic()))
        cpu_start = time.process_time()
        latencies, errors, examples, output_bytes = await phase(start + args.duration, True)
        finished = time.monotonic()
        cpu_seconds = time.process_time() - cpu_start
        # Correctness checks and sample-file writes are outside the timed phase.
        validate(await methods[0](request, timeout=args.rpc_timeout), args)
        with (directory / f"latency-{args.client_index}.bin").open("wb") as output:
            latencies.tofile(output)
        write_json(
            directory / f"result-{args.client_index}.json",
            {
                "finished": finished,
                "successes": len(latencies),
                "errors": errors,
                "error_examples": examples,
                "warmup_errors": warmup_errors,
                "output_ipc_bytes": output_bytes,
                "cpu_seconds": cpu_seconds,
            },
        )
    finally:
        await asyncio.gather(*(channel.close() for channel in channels))


def process_stats(supervisor_pid):
    """Best-effort Linux process counters; no extra dependency on psutil."""
    if sys.platform != "linux":
        return {}
    children = Path(f"/proc/{supervisor_pid}/task/{supervisor_pid}/children")
    pids = [supervisor_pid]
    try:
        pids += [int(pid) for pid in children.read_text().split()]
    except OSError:
        return {}
    result = {}
    for pid in pids:
        try:
            fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            result[str(pid)] = {
                "role": "supervisor" if pid == supervisor_pid else "worker",
                "cpu_seconds": (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
                "rss_mib": int(fields[21]) * os.sysconf("SC_PAGE_SIZE") / (1 << 20),
            }
        except (OSError, ValueError, IndexError):
            continue
    return result


def check_processes(server, clients, log_path):
    if server.poll() is not None or any(client.poll() not in (None, 0) for client in clients):
        raise RuntimeError(f"benchmark subprocess failed; log tail:\n{log_path.read_text()[-12000:]}")


def percentile(values, percent):
    if not values:
        return None
    return values[max(0, math.ceil(len(values) * percent / 100) - 1)]


def benchmark(args):
    with tempfile.TemporaryDirectory(prefix="pyudf-bench-") as temporary:
        directory = Path(temporary)
        request_bytes = prepare(directory, args)
        with socket.socket() as reserve:
            reserve.bind(("127.0.0.1", 0))
            address = f"127.0.0.1:{reserve.getsockname()[1]}"
        (directory / "address").write_text(address)
        command = [
            sys.executable,
            "-I",
            "-c",
            "import sys,runpy; sys.path.insert(0,sys.argv.pop(1)); runpy.run_module('milvus_pyudf_runtime.supervisor',run_name='__main__')",
            str(ROOT),
            "--address",
            address,
            "--worker-count",
            str(args.workers),
            "--grpc-concurrency",
            str(args.server_threads),
            "--max-concurrent-rpcs",
            str(args.server_max_rpcs),
            "--max-message-bytes",
            str(args.max_message_bytes),
            "--shutdown-timeout-ms",
            "5000",
        ]
        clients = []
        log_path = directory / "process.log"
        with log_path.open("wb") as log:
            server = subprocess.Popen(command, stdout=log, stderr=log, start_new_session=True)
            try:
                startup = time.monotonic()
                for index in range(args.client_processes):
                    clients.append(
                        subprocess.Popen(
                            [
                                sys.executable,
                                "-I",
                                str(Path(__file__).resolve()),
                                *sys.argv[1:],
                                "--client-dir",
                                str(directory),
                                "--client-index",
                                str(index),
                            ],
                            stdout=log,
                            stderr=log,
                        )
                    )
                ready_deadline = startup + args.startup_timeout + args.warmup + 2 * args.rpc_timeout + 10
                while not all((directory / f"ready-{i}.json").exists() for i in range(len(clients))):
                    check_processes(server, clients, log_path)
                    if time.monotonic() >= ready_deadline:
                        raise TimeoutError(f"startup/warmup timed out:\n{log_path.read_text()[-12000:]}")
                    time.sleep(0.02)
                ready_seconds = time.monotonic() - startup
                before = process_stats(server.pid)
                sampled_at = time.monotonic()
                start = sampled_at + 0.1
                write_json(directory / "start.json", {"start": start})
                finish_deadline = start + args.duration + 2 * args.rpc_timeout + 10
                while not all((directory / f"result-{i}.json").exists() for i in range(len(clients))):
                    check_processes(server, clients, log_path)
                    if time.monotonic() >= finish_deadline:
                        raise TimeoutError(f"measurement timed out:\n{log_path.read_text()[-12000:]}")
                    time.sleep(0.02)
                after = process_stats(server.pid)
                sample_seconds = time.monotonic() - sampled_at
                for client in clients:
                    if client.wait(timeout=3) != 0:
                        raise RuntimeError(f"load generator failed:\n{log_path.read_text()[-12000:]}")
                results = [json.loads((directory / f"result-{i}.json").read_text()) for i in range(len(clients))]
                first = [
                    json.loads((directory / f"ready-{i}.json").read_text())["first_rpc_ms"] for i in range(len(clients))
                ]
                samples = array("d")
                for i in range(len(clients)):
                    samples.frombytes((directory / f"latency-{i}.bin").read_bytes())
                successes = sum(r["successes"] for r in results)
                if successes != len(samples):
                    raise RuntimeError("latency samples do not match successful RPC count")
                ordered = sorted(samples)
                errors, warmup_errors, examples = Counter(), Counter(), {}
                for result in results:
                    errors.update(result["errors"])
                    warmup_errors.update(result["warmup_errors"])
                    examples.update(result["error_examples"])
                elapsed = max(r["finished"] for r in results) - start
                server_stats = {}
                for pid, stats in after.items():
                    delta = stats["cpu_seconds"] - before[pid]["cpu_seconds"] if pid in before else None
                    server_stats[pid] = {
                        "role": stats["role"],
                        "rss_mib_end": stats["rss_mib"],
                        "cpu_seconds": delta,
                        "average_cpu_cores": delta / sample_seconds if delta is not None else None,
                    }
                return {
                    "benchmark": "PyUDF a + b, closed-loop gRPC, pre-encoded input",
                    "runtime_source": str(ROOT),
                    "python": sys.version.split()[0],
                    "logical_cpus": os.cpu_count(),
                    "versions": {p: version(p) for p in ("grpcio", "pyarrow", "protobuf")},
                    "config": {k: v for k, v in vars(args).items() if k not in ("json", "client_dir", "client_index")},
                    "startup_and_warmup_seconds": ready_seconds,
                    "first_rpc_ms_per_client": first,
                    "request_bytes": request_bytes,
                    "elapsed_seconds_including_drain": elapsed,
                    "successes": successes,
                    "failures": sum(errors.values()),
                    "rpc_per_second": successes / elapsed,
                    "rows_per_second": successes * args.rows * args.queries / elapsed,
                    "latency_ms_successful_rpcs": {
                        "mean": sum(samples) / len(samples) if samples else None,
                        "p50": percentile(ordered, 50),
                        "p95": percentile(ordered, 95),
                        "p99": percentile(ordered, 99),
                        "max": ordered[-1] if ordered else None,
                    },
                    "errors": dict(errors),
                    "error_examples": examples,
                    "warmup_errors": dict(warmup_errors),
                    "output_ipc_mib_per_second": sum(r["output_ipc_bytes"] for r in results) / elapsed / (1 << 20),
                    "client_cpu_seconds": [r["cpu_seconds"] for r in results],
                    "server_processes": server_stats,
                    "server_stats_interval_seconds": sample_seconds,
                    "worker_pids_changed": set(before) != set(after),
                }
            finally:
                for client in clients:
                    if client.poll() is None:
                        client.terminate()
                for client in clients:
                    try:
                        client.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        client.kill()
                        client.wait()
                if server.poll() is None:
                    server.terminate()
                try:
                    server.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    pass
                try:
                    os.killpg(server.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                server.wait()


def main():
    args = arguments()
    if args.client_dir is not None:
        asyncio.run(load_client(args))
        return 0
    report = benchmark(args)
    rendered = json.dumps(report, indent=2)
    print(rendered)
    if args.json is not None:
        args.json.write_text(rendered + "\n")
    return 1 if report["failures"] or not report["successes"] or report["worker_pids_changed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
