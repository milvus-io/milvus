# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Trusted request scope is invocation-owned even when a UDF instance is shared."""

import concurrent.futures
import importlib
import logging
import threading

import grpc
import pyarrow as pa
import pytest
import test_worker as helpers

from milvus_pyudf_runtime import worker as worker_module
from milvus_pyudf_runtime.proto import pyudf_pb2 as wire


@pytest.fixture
def worker(monkeypatch):
    rpc_finished = threading.Event()
    execute = worker_module.PyUDFWorkerService.Execute

    def observe_rpc(self, request, context):
        context.add_callback(rpc_finished.set)
        return execute(self, request, context)

    monkeypatch.setattr(worker_module.PyUDFWorkerService, "Execute", observe_rpc)
    harness = helpers.WorkerTests()
    harness.setUp()
    harness.rpc_finished = rpc_finished
    try:
        yield harness
    finally:
        harness.tearDown()


def scope_request(worker, path, *, stage="ingestion", batches=None, user="alice"):
    request = worker.request(path, batches)
    request.stage, request.execution_version = stage, 6
    request.request_context.CopyFrom(
        wire.RequestContext(
            database="db-" + user,
            collection="docs-" + user,
            user=user,
            resource_name="chain/step/0/" + user,
        )
    )
    return request


def test_real_rpc_concurrent_scope_and_logs_do_not_leak_through_cached_factory(worker, caplog):
    caplog.set_level(logging.INFO, logger="milvus.pyudf.worker")
    path, package = worker.wheel("""
import threading
from dataclasses import FrozenInstanceError
import pyarrow as pa
barrier=threading.Barrier(4)
loads=0
loggers=[]
class UDF:
 def __init__(self,initial):
  assert initial.resource_name=='test'
  assert not hasattr(initial,'user') and not hasattr(initial,'database')
  self.initial=initial
 def transform(self,params,columns,*,context):
  assert context.database=='db-'+context.user
  assert context.collection=='docs-'+context.user
  assert context.resource_name=='chain/step/0/'+context.user
  assert context.input_schema.names==['query','query']
  assert context.input_schema.metadata=={b'source':b'requestscope'}
  assert params['user']=='spoof' and params['database']=='spoof'
  try: context.user='spoof'
  except FrozenInstanceError: pass
  else: raise AssertionError('scope must be frozen')
  try: context.logger.extra['user']='spoof'
  except TypeError: pass
  else: raise AssertionError('log metadata must be immutable')
  loggers.append(context.logger)
  barrier.wait(timeout=2)
  context.logger.info('business scope verified',extra={'user':'spoof'})
  field=pa.field('scope',pa.struct([(name,pa.string()) for name in ('database','collection','user','resource_name')]))
  row={name:getattr(context,name) for name in ('database','collection','user','resource_name')}
  return pa.record_batch([pa.array([row]*len(columns[0]),type=field.type)],schema=pa.schema([field],metadata={'runtime':'scope'}))
def factory(initial):
 global loads
 loads+=1
 return UDF(initial)
""")
    requests = []
    for index in range(16):
        schema = pa.schema([pa.field("query", pa.int64())], metadata={"source": "requestscope"})
        batch = pa.record_batch([pa.array([1, 2])], schema=schema)
        request = scope_request(worker, path, user=str(index), batches=[batch])
        request.input_column_indices.extend([0, 0])
        for key in ("database", "collection", "user", "resource_name"):
            request.params.fields[key].string_value = "spoof"
        requests.append(request)
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(worker.execute, requests))
    for index, response in enumerate(results):
        assert response.WhichOneof("result") == "outputs", response
        assert response.execution_version == 6
        batch = helpers.decode(response.outputs)[0]
        assert batch.schema.metadata == {b"runtime": b"scope"}
        value = batch.column(0)[0].as_py()
        assert value == {
            "database": f"db-{index}",
            "collection": f"docs-{index}",
            "user": str(index),
            "resource_name": f"chain/step/0/{index}",
        }
    module = importlib.import_module(package)
    assert module.loads == 1
    assert len({id(logger) for logger in module.loggers}) == 16
    records = [record for record in caplog.records if record.message == "business scope verified"]
    assert len(records) == 16
    for record in records:
        assert record.database == "db-" + record.user
        assert record.collection == "docs-" + record.user
        assert record.resource_name == "chain/step/0/" + record.user


def test_real_rpc_scoped_pre_and_expanded_rerank_keep_query_identity(worker):
    path, _ = worker.wheel("""
import pyarrow as pa
class UDF:
 def transform_query(self,params,columns,*,context):
  assert context.database=='db-alice' and context.user=='alice'
  assert context.query_count==2 and context.prepared_query_counts==(3,)
  assert context.query_origins==((0,0,1),)
  if context.stage=='pre_process':
   assert context.row_query_origins==(0,0,1)
  else:
   assert context.original_query_index==context.query_origins[0][context.query_index]
  return columns
def factory(context): return UDF()
""")
    for stage in ("pre_process", "L2_rerank"):
        batches = (
            [pa.record_batch([pa.array(["a", "b", "c"])], names=["query"])]
            if stage == "pre_process"
            else [
                pa.record_batch([pa.array(values, type=pa.string())], names=["text"]) for values in ([], ["a"], ["c"])
            ]
        )
        request = scope_request(worker, path, stage=stage, batches=batches)
        request.query_context.original_queries.add(data_type="VarChar", values=[b"ab", b"c"])
        request.query_context.queries.add(
            data_type="VarChar", values=[b"a", b"b", b"c"], original_query_indices=[0, 0, 1]
        )
        if stage == "pre_process":
            request.query_context.row_query_origins.extend([0, 0, 1])
        response = worker.execute(request)
        assert response.WhichOneof("result") == "outputs", response
        assert response.execution_version == 6
        assert [batch.num_rows for batch in helpers.decode(response.outputs)] == [batch.num_rows for batch in batches]


@pytest.mark.parametrize("reduction", [False, True])
def test_real_rpc_scoped_query_table_and_typed_empty_reduction(worker, reduction):
    path, _ = worker.wheel("""
import pyarrow as pa
class UDF:
 def transform_batch(self,params,batches,*,context):
  assert context.query_count==0 and context.table_result
  assert context.collection=='docs-alice' and context.resource_name=='chain/step/0/alice'
  return [[pa.array([context.user]*(1 if context.reduction else len(columns[0])),type=pa.string())] for columns in batches]
def factory(context): return UDF()
""")
    batch = pa.record_batch([pa.array([], type=pa.string())], names=["text"])
    request = scope_request(worker, path, stage="post_process", batches=[batch])
    request.table_result, request.reduction = True, reduction
    response = worker.execute(request)
    assert response.WhichOneof("result") == "outputs", response
    result = helpers.decode(response.outputs)[0]
    assert result.column(0).to_pylist() == (["alice"] if reduction else [])


@pytest.mark.parametrize("version", [0, 2, 3, 4, 5])
def test_scope_cannot_be_silently_downgraded_or_read_from_params(worker, version):
    request = scope_request(worker, worker.directory / "not-loaded.whl")
    request.execution_version = version
    worker.assert_code(worker.execute(request), wire.UNSUPPORTED)


def test_v6_requires_scope_presence_before_loading(worker):
    request = worker.request(worker.directory / "not-loaded.whl")
    request.execution_version = 6
    worker.assert_code(worker.execute(request), wire.INTERNAL)


def test_legacy_context_has_empty_identity_and_artifact_log_label(worker):
    path, _ = worker.wheel("""
class UDF:
 def transform_query(self,params,columns,*,context):
  assert context.database==context.collection==context.user==''
  assert context.resource_name=='test' and context.logger.extra['resource_name']=='test'
  return columns
def factory(context): return UDF()
""")
    batches = [pa.record_batch([pa.array(rows, type=pa.string())], names=["text"]) for rows in ([], ["a"], ["b"])]
    request = worker.query_request(path, batches)
    request.params.fields["user"].string_value = "spoof"
    response = worker.execute(request)
    assert response.WhichOneof("result") == "outputs", response
    assert response.execution_version == 2


def test_worker_failure_log_uses_host_scope_and_preserves_error_category(worker, caplog):
    caplog.set_level(logging.ERROR, logger="milvus.pyudf.worker")
    path, _ = worker.wheel("""
class UDF:
 def transform(self,params,columns,*,context):
  raise ValueError('deliberate user-code failure')
def factory(context): return UDF()
""")
    request = scope_request(worker, path)
    request.params.fields["user"].string_value = "spoof"
    worker.assert_code(worker.execute(request), wire.UDF_FAILED)
    records = [record for record in caplog.records if "Execute failed" in record.message]
    assert len(records) == 1
    assert records[0].database == "db-alice" and records[0].collection == "docs-alice"
    assert records[0].user == "alice" and records[0].resource_name == "chain/step/0/alice"


def test_request_logger_allocation_failure_preserves_oom_category(worker, monkeypatch):
    def fail(request):
        raise MemoryError("controlled logger allocation failure")

    monkeypatch.setattr(worker_module, "_request_logger", fail)
    request = scope_request(worker, worker.directory / "not-loaded.whl")
    worker.assert_code(worker.execute(request), wire.OUT_OF_MEMORY)


@pytest.mark.parametrize("cancel", [False, True])
def test_scoped_deadline_and_cancel_keep_existing_grpc_behavior(worker, cancel, caplog):
    caplog.set_level(logging.WARNING, logger="milvus.pyudf.worker")
    path, package = worker.wheel("""
import threading
entered=threading.Event()
release=threading.Event()
class UDF:
 def transform(self,params,columns,*,context):
  entered.set()
  assert context.user=='alice'
  release.wait(timeout=3)
  return columns
def factory(context): return UDF()
""")
    request = scope_request(worker, path)
    # Load the wheel once to obtain its local synchronization events.
    worker.worker.service.loader.load("test", path, "ingestion", lambda: None)
    module = importlib.import_module(package)
    worker.release_events.append(module.release)
    future = worker.client.Execute.future(request, timeout=2 if cancel else 0.05)
    assert module.entered.wait(timeout=1)
    if cancel:
        future.cancel()
        with pytest.raises(grpc.FutureCancelledError):
            future.result(timeout=1)
    else:
        with pytest.raises(grpc.RpcError) as caught:
            future.result(timeout=1)
        assert caught.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
    # Client cancellation can finish before the server receives it. Wait for
    # the server's RPC callback before letting the UDF return its late output.
    assert worker.rpc_finished.wait(timeout=1)
    module.release.set()
    # Draining only this fixture's executor confirms late output is discarded.
    worker.executor.shutdown(wait=True)
    records = [record for record in caplog.records if "cancelled or expired" in record.message]
    assert records and records[-1].user == "alice"
    assert records[-1].resource_name == "chain/step/0/alice"
