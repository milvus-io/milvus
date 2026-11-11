"""Search admission, cancellation and worker-pool concurrency on dedicated profiles."""

import io
import json
import statistics
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import grpc
import pyarrow as pa
import pytest
import requests
from common import common_func as cf
from common.pyudf_helpers import assert_pyudf_deadline, build_wheel
from minio import Minio
from pymilvus import FunctionChain, FunctionChainStage, MilvusClient, MilvusException
from pymilvus.function_chain import col
from pymilvus.function_chain.chain import FunctionChainExpr
from workloads.pyudf.probe import pyudf_probe


@pytest.fixture
def runtime_connection(minio_host, minio_bucket):
    uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
    token = cf.param_info.param_token or f"{cf.param_info.param_user}:{cf.param_info.param_password}"
    client = MilvusClient(uri, token=token)
    storage = Minio(f"{minio_host}:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
    try:
        yield client, storage, minio_bucket
    finally:
        client.close()


def require_config(request, expected):
    endpoint = request.config.getoption("--pyudf-management-endpoint")
    assert endpoint, "This L3 selection requires its isolated runtime management endpoint"
    for key, value in expected.items():
        response = requests.get(endpoint.rstrip("/") + "/management/config/get", params={"keys": key}, timeout=10)
        response.raise_for_status()
        assert response.json()["configs"][0]["value"] == str(value), response.json()


@pytest.mark.tags("L3", "pyudf")
def test_pyudf_expired_active_call_retains_capacity_until_actual_completion(
    request, runtime_connection, record_property
):
    """target: late admission; method: wait past deadline; expected: executing Python retains capacity."""
    require_config(
        request,
        {
            "function.pyUDF.server.workerCount": 1,
            "function.pyUDF.server.grpcConcurrency": 1,
            "function.pyUDF.server.maxConcurrentRPCs": 2,
            "function.pyUDF.rpcTimeout": "5s",
        },
    )
    with pyudf_probe(*runtime_connection) as probe:

        def invoke(mode, timeout):
            return probe.client.search(
                probe.name,
                data=[[0.0, 0.0]],
                anns_field="vector",
                limit=3,
                search_params={"metric_type": "L2"},
                function_chains=probe.chain(mode),
                timeout=timeout,
                retry_on_rate_limit=False,
            )

        begin = time.monotonic()
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                active = pool.submit(invoke, "server_timeout", 12)
                assert_pyudf_deadline(lambda: active.result(timeout=7))
                assert 4.5 < time.monotonic() - begin < 7, "Service deadline did not occur before Python completion"
                late = pool.submit(invoke, "normal", 1)
                time.sleep(0.15)
                with pytest.raises(MilvusException) as rejected:
                    invoke("normal", 1)
                assert rejected.value.code == 12, rejected.value
                probe.check(with_udf=False)
                assert_pyudf_deadline(lambda: late.result(timeout=2))
        finally:
            time.sleep(max(0, 8.5 - (time.monotonic() - begin)))
        state = {hit["distance"] for hit in probe.search("state")[0]}
        assert state == {203}, state
        record_property("late_window", "after 5s RPC timeout, before 8s actual Python completion")
        record_property("counter", "only warmup and timed-out active invocation completed before call3")
        probe.check()
        probe.check(with_udf=False)


CONCURRENT_MODULE = """import os,threading,time
import pyarrow as pa
_factory_calls=0
class UDF:
 def __init__(self):
  self.lock=threading.Lock(); self.active=0; self.peak=0
 def transform_query(self,p,c):
  with self.lock:
   self.active+=1; self.peak=max(self.peak,self.active)
  try:
   time.sleep(0.1)
   if p.get('pid'): return [pa.array([os.getpid()]*len(c[0]), type=pa.float32())]
   assert _factory_calls==1, 'factory instantiated twice in one worker'
   return [pa.array([x.as_py()*2 for x in c[0]], type=pa.float32())]
  finally:
   with self.lock: self.active-=1
def create_udf(ctx):
 global _factory_calls
 _factory_calls+=1
 ctx.logger.warning('PYUDF_R2_FACTORY pid=%s count=%s', os.getpid(), _factory_calls)
 return UDF()
"""


@pytest.mark.tags("L3", "pyudf")
@pytest.mark.parametrize(
    "workers,pool_size",
    [(w, p) for w in (1, 2, 4) for p in (1, 4, 10)],
    ids=[f"w{w}-p{p}" for w in (1, 2, 4) for p in (1, 4, 10)],
)
def test_pyudf_worker_pool_matrix_cold_resources_and_precise_results(
    request, runtime_connection, record_property, workers, pool_size
):
    """target: nine pool profiles; method: simultaneous cold loads; expected: precise results and one factory per worker."""
    require_config(
        request,
        {
            "function.pyUDF.server.workerCount": workers,
            "function.pyUDF.connectionPoolSize": pool_size,
        },
    )
    client, storage, bucket = runtime_connection
    registered = []
    uploaded = []
    with pyudf_probe(*runtime_connection) as probe:
        try:
            resources = []
            for _ in range(2):
                name = "pyudf_r2_concurrent_" + uuid.uuid4().hex[:12]
                data = build_wheel(name, CONCURRENT_MODULE)
                key = "pyudf_r2/" + name + ".whl"
                storage.put_object(bucket, key, io.BytesIO(data), len(data))
                uploaded.append(key)
                client.add_file_resource(name, key)
                registered.append(name)
                resources.append(name)

            # Snapshot polling is deliberately separated from successful factory calls.
            # A resource-not-ready response may be retried; any other failure is retained.
            def invoke(resource, pid=False):
                expression = FunctionChainExpr(
                    "py_udf", args=(col("ts"),), params={"resource_name": resource, "udf_params": {"pid": pid}}
                )
                chain = (
                    FunctionChain(FunctionChainStage.L2_RERANK)
                    .map("$score", expression)
                    .sort(col("$score"), desc=True, tie_break_col=col("$id"))
                )
                deadline = time.monotonic() + 60
                while True:
                    try:
                        return client.search(
                            probe.name,
                            data=[[0.0, 0.0]] * 2,
                            anns_field="vector",
                            limit=3,
                            search_params={"metric_type": "L2"},
                            function_chains=chain,
                            output_fields=["ts"],
                            timeout=15,
                            retry_on_rate_limit=False,
                        )
                    except MilvusException as error:
                        if time.monotonic() >= deadline or not (
                            "file resource snapshot is not ready" in str(error)
                            or "file resource" in str(error)
                            and "not found" in str(error)
                        ):
                            raise
                        time.sleep(0.1)

            barrier = threading.Barrier(8)

            def cold(index):
                barrier.wait(timeout=5)
                return invoke(resources[index % 2])

            with ThreadPoolExecutor(max_workers=8) as threads:
                futures = [threads.submit(cold, index) for index in range(8)]
                for future in futures:
                    response = future.result(timeout=80)
                    assert len(response) == 2
                    for hits in response:
                        assert [hit["id"] for hit in hits] == [3, 2, 1], hits
                        assert [hit["distance"] for hit in hits] == [60, 40, 20], hits
                        assert [hit["entity"]["ts"] for hit in hits] == [30, 20, 10], hits
                pid_futures = [threads.submit(invoke, resources[index % 2], True) for index in range(16)]
                pids = {
                    int(hit["distance"]) for future in pid_futures for hits in future.result(timeout=30) for hit in hits
                }
            assert 1 <= len(pids) <= workers, pids
            record_property("actual_worker_pids", json.dumps(sorted(pids)))
            record_property("profile", f"workers={workers},pool={pool_size}; no uniform-distribution assertion")
            probe.check()
            probe.check(with_udf=False)
        finally:
            for resource in registered:
                client.remove_file_resource(resource)
            for key in uploaded:
                storage.remove_object(bucket, key)


UPPER_WORKER_MODULE = """import os,threading,time
import pyarrow as pa
class UDF:
 def __init__(self,ctx):
  self.ctx=ctx; self.lock=threading.Lock(); self.reported=False
 def transform_query(self,p,c):
  if p.get('pid'):
   return [pa.array([os.getpid()]*len(c[0]),type=pa.float32())]
  time.sleep(0.01)
  with self.lock:
   if not self.reported:
    self.ctx.logger.warning('PYUDF_UPPER_BUSINESS resource=%s pid=%s',p['resource'],os.getpid())
    self.reported=True
  return [pa.array([x.as_py()*2 for x in c[0]],type=pa.float32())]
def create_udf(ctx): return UDF(ctx)
"""


@pytest.mark.tags("L3", "pyudf")
def test_pyudf_256_workers_serving_search_with_bounded_load(request, runtime_connection, record_property):
    """target: upper worker profile; method: Search and PID probes; expected: exact results under bounded load."""
    require_config(
        request,
        {
            "function.pyUDF.server.workerCount": 256,
            "function.pyUDF.connectionPoolSize": 1024,
            "function.pyUDF.server.grpcConcurrency": 1024,
            "function.pyUDF.server.maxConcurrentRPCs": 65536,
            "function.pyUDF.maxMessageBytes": 1073741824,
        },
    )
    client, storage, bucket = runtime_connection
    resource = "pyudf_upper_" + uuid.uuid4().hex[:12]
    path = "pyudf_r2/" + resource + ".whl"
    wheel = build_wheel(resource, UPPER_WORKER_MODULE)
    storage.put_object(bucket, path, io.BytesIO(wheel), len(wheel))
    registered = False
    try:
        client.add_file_resource(resource, path)
        registered = True
        record_property("upper_server", json.dumps(client.get_server_version(detail=True)))
        record_property("upper_resource", resource)
        with pyudf_probe(*runtime_connection) as probe:
            chains = {}
            for pid in (False, True):
                expression = FunctionChainExpr(
                    "py_udf",
                    args=(col("ts"),),
                    params={"resource_name": resource, "udf_params": {"pid": pid, "resource": resource}},
                )
                chains[pid] = (
                    FunctionChain(FunctionChainStage.L2_RERANK)
                    .map("$score", expression)
                    .sort(col("$score"), desc=True, tie_break_col=col("$id"))
                )

            def invoke(pid=False):
                started = time.monotonic()
                hits = client.search(
                    probe.name,
                    data=[[0.0, 0.0]],
                    anns_field="vector",
                    limit=3,
                    search_params={"metric_type": "L2"},
                    function_chains=chains[pid],
                    output_fields=["ts"],
                    timeout=30,
                    retry_on_rate_limit=False,
                )[0]
                assert {hit["id"]: hit["entity"]["ts"] for hit in hits} == {1: 10, 2: 20, 3: 30}, hits
                if pid:
                    pids = {int(hit["distance"]) for hit in hits}
                    assert len(pids) == 1 and all(hit["distance"] in pids for hit in hits), hits
                    return pids.pop()
                assert [hit["id"] for hit in hits] == [3, 2, 1], hits
                assert [hit["distance"] for hit in hits] == [60, 40, 20], hits
                return time.monotonic() - started

            ready_by = time.monotonic() + 60
            while True:
                try:
                    invoke()
                    break
                except MilvusException as error:
                    missing = "file resource snapshot is not ready" in str(error) or (
                        "file resource" in str(error) and "not found" in str(error)
                    )
                    if not missing or time.monotonic() >= ready_by:
                        raise
                    time.sleep(0.1)

            with ThreadPoolExecutor(max_workers=32) as pool:
                pids = set(pool.map(lambda _: invoke(True), range(2048)))
            assert 1 <= len(pids) <= 256, pids
            record_property("upper_search_worker_pids", json.dumps(sorted(pids)))
            for concurrency, seconds in ((1, 60), (8, 60), (32, 60), (32, 300)):
                begin = time.monotonic()
                started_unix = time.time()
                stop_at = begin + seconds

                def load():
                    latencies, errors = [], []
                    attempts = 0
                    while time.monotonic() < stop_at:
                        attempts += 1
                        try:
                            latencies.append(invoke())
                        except Exception as error:
                            errors.append({"type": type(error).__name__, "detail": str(error)[:500]})
                            # Preserve errors without hammering an unhealthy target.
                            time.sleep(0.1)
                    return attempts, latencies, errors

                with ThreadPoolExecutor(max_workers=concurrency) as pool:
                    windows = list(pool.map(lambda _: load(), range(concurrency)))
                elapsed = time.monotonic() - begin
                latencies = sorted(value for _, values, _ in windows for value in values)
                errors = [error for _, _, failures in windows for error in failures]
                summary = {
                    "concurrency": concurrency,
                    "requested_seconds": seconds,
                    "elapsed_seconds": elapsed,
                    "started_unix": started_unix,
                    "ended_unix": time.time(),
                    "attempts": sum(item[0] for item in windows),
                    "successes": len(latencies),
                    "errors": len(errors),
                    "error_examples": errors[:10],
                    "completed_qps": len(latencies) / elapsed,
                    "p50_seconds": statistics.median(latencies) if latencies else None,
                    "p95_seconds": latencies[int((len(latencies) - 1) * 0.95)] if latencies else None,
                    "p99_seconds": latencies[int((len(latencies) - 1) * 0.99)] if latencies else None,
                }
                record_property(f"upper_load_{concurrency}_{seconds}", json.dumps(summary))
                assert latencies and not errors, summary
                probe.check(with_udf=False)
            probe.check()
            assert client.query(probe.name, filter="", output_fields=["count(*)"])[0]["count(*)"] == 3
    finally:
        if registered:
            client.remove_file_resource(resource)
        storage.remove_object(bucket, path)


CANCELLATION_MODULE = """import os,time
from pathlib import Path
import pyarrow as pa
MARKER = Path(__MARKER__)
OBSERVER = __OBSERVER__
def mark(value):
 with MARKER.open('a') as f: f.write(value+'\\n')
class UDF:
 def transform_query(self,p,c):
  if OBSERVER:
   if p.get('cleanup'):
    MARKER.unlink(missing_ok=True); value=0
   else:
    lines=MARKER.read_text().splitlines() if MARKER.exists() else []
    value=lines.count(p['event'])
   return [pa.array([value]*len(c[0]), type=pa.float32())]
  mark('T_START')
  if p.get('slow'): time.sleep(1.5)
  mark('T_END')
  return [pa.array([v.as_py()*2 for v in c[0]], type=pa.float32())]
def create_udf(ctx):
 if not OBSERVER:
  mark('F_START'); time.sleep(3); mark('F_END')
 return UDF()
"""


@pytest.mark.tags("L3", "pyudf")
def test_pyudf_search_cancels_loader_wait_and_later_query_batches(request, runtime_connection, record_property):
    """target: Search cancellation; method: persisted entry markers; expected: no expired transform or later batch."""
    require_config(
        request,
        {"function.pyUDF.server.workerCount": 1, "function.pyUDF.server.grpcConcurrency": 10},
    )
    client, storage, bucket = runtime_connection
    registered, uploaded = [], []
    marker = "/tmp/pyudf_r2_cancel_" + uuid.uuid4().hex
    with pyudf_probe(*runtime_connection) as probe:

        def search(resource, params, timeout=10, nq=1):
            expression = FunctionChainExpr(
                "py_udf", args=(col("ts"),), params={"resource_name": resource, "udf_params": params}
            )
            chain = FunctionChain(FunctionChainStage.L2_RERANK).map("$score", expression).sort(col("$score"), desc=True)
            return client.search(
                probe.name,
                data=[[0.0, 0.0]] * nq,
                anns_field="vector",
                limit=3,
                search_params={"metric_type": "L2"},
                function_chains=chain,
                output_fields=["ts"],
                timeout=timeout,
                retry_on_rate_limit=False,
            )

        observer = None
        try:
            resources = []
            for is_observer in (False, True):
                name = "pyudf_r2_cancel_" + uuid.uuid4().hex[:12]
                module = CANCELLATION_MODULE.replace("__MARKER__", repr(marker)).replace(
                    "__OBSERVER__", repr(is_observer)
                )
                wheel = build_wheel(name, module)
                path = "pyudf_r2/" + name + ".whl"
                storage.put_object(bucket, path, io.BytesIO(wheel), len(wheel))
                uploaded.append(path)
                client.add_file_resource(name, path)
                registered.append(name)
                resources.append(name)
            blocking, observer = resources

            def count(event):
                result = search(observer, {"event": event})
                scores = {hit["distance"] for hit in result[0]}
                assert len(scores) == 1, result
                return int(scores.pop())

            deadline = time.monotonic() + 60
            while True:
                try:
                    assert count("F_START") == 0
                    break
                except MilvusException as error:
                    if time.monotonic() >= deadline or not (
                        "file resource snapshot is not ready" in str(error)
                        or "file resource" in str(error)
                        and "not found" in str(error)
                    ):
                        raise
                    time.sleep(0.1)
            started = time.monotonic()
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(search, blocking, {}, 1)
                while count("F_START") == 0:
                    assert time.monotonic() - started < 0.8, "Factory entry was not observed before first deadline"
                    time.sleep(0.02)
                waiting = pool.submit(search, blocking, {}, 0.25)
                independent_started = time.monotonic()
                assert count("F_START") == 1
                independent_elapsed = time.monotonic() - independent_started
                assert independent_elapsed < 1, "Independent resource waited on the cold resource factory"
                assert_pyudf_deadline(lambda: waiting.result(timeout=2))
                assert_pyudf_deadline(lambda: first.result(timeout=2))
            time.sleep(max(0, 3.5 - (time.monotonic() - started)))
            assert count("F_START") == count("F_END") == 1
            assert count("T_START") == count("T_END") == 0, "Expired loader requests executed transform_query"

            started = time.monotonic()
            assert_pyudf_deadline(lambda: search(blocking, {"slow": True}, 0.5, nq=3))
            time.sleep(max(0, 2 - (time.monotonic() - started)))
            assert count("T_START") == count("T_END") == 1, "Later query batch executed after cancellation"
            recovered = search(blocking, {}, nq=3)
            assert len(recovered) == 3
            for hits in recovered:
                assert [hit["id"] for hit in hits] == [3, 2, 1], hits
                assert [hit["distance"] for hit in hits] == [60, 40, 20], hits
                assert [hit["entity"]["ts"] for hit in hits] == [30, 20, 10], hits
            assert count("T_START") == count("T_END") == 4
            record_property("independent_resource_seconds", independent_elapsed)
            record_property("factory_calls", 1)
            record_property("expired_loader_transform_calls", 0)
            record_property("cancelled_nq3_started_batches", 1)
            probe.check()
            probe.check(with_udf=False)
        finally:
            if observer:
                search(observer, {"cleanup": True})
            for resource in registered:
                client.remove_file_resource(resource)
            for path in uploaded:
                storage.remove_object(bucket, path)


@pytest.mark.tags("L3", "pyudf")
def test_pyudf_admission_queue_expiry_and_server_deadline(request, runtime_connection, record_property):
    """Reject excess RPCs; skip expired queued code; finish timed-out running code once."""
    endpoint = request.config.getoption("--pyudf-management-endpoint")
    assert endpoint, "Requires an explicitly configured isolated runtime profile"
    expected = {
        "function.pyUDF.server.workerCount": "1",
        "function.pyUDF.server.grpcConcurrency": "1",
        "function.pyUDF.server.maxConcurrentRPCs": "2",
        "function.pyUDF.rpcTimeout": "5s",
    }
    for key, value in expected.items():
        response = requests.get(endpoint.rstrip("/") + "/management/config/get", params={"keys": key}, timeout=10)
        response.raise_for_status()
        assert response.json()["configs"][0]["value"] == value, response.json()
    client, storage, minio_bucket = runtime_connection
    with pyudf_probe(client, storage, minio_bucket) as probe:

        def invoke(mode, timeout):
            return client.search(
                probe.name,
                data=[[0.0, 0.0]],
                anns_field="vector",
                limit=3,
                search_params={"metric_type": "L2"},
                function_chains=probe.chain(mode),
                timeout=timeout,
                retry_on_rate_limit=False,
            )

        started = time.monotonic()
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                active = pool.submit(invoke, "server_timeout", 12)
                time.sleep(0.3)
                queued = pool.submit(invoke, "state", 1)
                time.sleep(0.3)
                with pytest.raises(MilvusException) as admission:
                    invoke("normal", 1)
                assert admission.value.code == 12, admission.value
                with pytest.raises((MilvusException, grpc.RpcError)) as expiry:
                    queued.result(timeout=3)
                if isinstance(expiry.value, grpc.RpcError):
                    assert expiry.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED, expiry.value
                else:
                    assert expiry.value.code == 10001, expiry.value
                # Ordinary Search remains available while the worker's only executor is occupied.
                probe.check(with_udf=False)
                with pytest.raises(MilvusException) as server_timeout:
                    active.result(timeout=7)
                assert server_timeout.value.code == 10001, server_timeout.value
                assert 4.5 <= time.monotonic() - started < 7.5
        finally:
            # An Execute timeout cannot end the eight-second Python invocation.
            time.sleep(max(0, 8.5 - (time.monotonic() - started)))
        state = {hit["distance"] for hit in probe.search("state")[0]}
        assert state == {203}, f"Only warmup and active call may have completed before state call: {state}"
        record_property("runtime_counter", "completed=2/call=3; rejected and expired queued calls never invoked")
        probe.check()
        probe.check(with_udf=False)


@pytest.mark.tags("L3", "pyudf")
@pytest.mark.parametrize("mode", ["output_under", "output_over", "repeated_under", "distinct_over"])
def test_pyudf_message_limit_and_repeated_input_deduplication(request, runtime_connection, record_property, mode):
    """Check both message directions at 1 MiB, keeping precise scalar recovery controls."""
    endpoint = request.config.getoption("--pyudf-management-endpoint")
    assert endpoint, "Requires the isolated 1 MiB runtime profile"
    response = requests.get(
        endpoint.rstrip("/") + "/management/config/get", params={"keys": "function.pyUDF.maxMessageBytes"}, timeout=10
    )
    response.raise_for_status()
    assert response.json()["configs"][0]["value"] == "1048576", response.json()
    client, storage, minio_bucket = runtime_connection
    with pyudf_probe(client, storage, minio_bucket) as probe:
        size = 400000 if mode == "output_over" else 300000
        # Mirror the documented single-column output layout to account for the
        # complete protobuf response, including its bytes-field tag and varint.
        batch = pa.record_batch([pa.array(["x" * size] * 3, type=pa.string())], names=["c0"])
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema, options=pa.ipc.IpcWriteOptions(use_threads=False)) as writer:
            writer.write_batch(batch)
        payload_bytes = sink.tell()
        response_bytes = 1 + (payload_bytes.bit_length() + 6) // 7 + payload_bytes
        record_property("modeled_complete_output_protobuf_bytes", response_bytes)
        assert (response_bytes > 1048576) == (mode == "output_over")

        def expression(operation, columns):
            return FunctionChainExpr(
                "py_udf",
                args=tuple(col(name) for name in columns),
                params={"resource_name": probe.resource, "udf_params": {"mode": operation, "size": size}},
            )

        chain = FunctionChain(FunctionChainStage.L2_RERANK).map("wide_a", expression("wide_output", ["ts"]))
        if mode == "distinct_over":
            chain = chain.map("wide_b", expression("wide_output", ["ts"]))
            chain = chain.map("wide_c", expression("wide_output", ["ts"]))
            inputs = ["wide_a", "wide_b", "wide_c", "ts"]
        elif mode == "repeated_under":
            inputs = ["wide_a", "wide_a", "wide_a", "ts"]
        else:
            inputs = ["wide_a", "ts"]
        chain = chain.map("$score", expression("wide_input", inputs)).sort(col("$score"), desc=True)

        def search():
            return client.search(
                probe.name,
                data=[[0.0, 0.0]],
                anns_field="vector",
                limit=3,
                search_params={"metric_type": "L2"},
                function_chains=chain,
                timeout=15,
                retry_on_rate_limit=False,
            )

        if mode.endswith("over"):
            with pytest.raises(MilvusException) as rejected:
                search()
            assert rejected.value.code == 12, rejected.value
            assert "message" in str(rejected.value).lower(), rejected.value
            record_property("actual_transport_error", str(rejected.value))
        else:
            hits = search()[0]
            assert [hit["id"] for hit in hits] == [3, 2, 1], hits
            assert [hit["distance"] for hit in hits] == [60, 40, 20], hits
        probe.check()
        probe.check(with_udf=False)
