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

import importlib

import pyarrow as pa
import pyarrow.compute as pc
import pytest
import test_worker as worker_helpers

from milvus_pyudf_runtime.proto import pyudf_pb2 as wire


@pytest.fixture
def worker():
    harness = worker_helpers.WorkerTests()
    harness.setUp()
    try:
        yield harness
    finally:
        harness.tearDown()


def request_with_queries(worker, path, batches, stage, prepared=None, origins=None):
    request = worker.request(path, batches)
    request.stage, request.execution_version = stage, 5
    request.query_context.original_queries.add(data_type="VarChar", tag="$0", values=[b"a|b", b"c"])
    request.query_context.queries.add(data_type="VarChar", tag="$0", values=prepared or [b"a|b", b"c"])
    if origins is not None:
        request.query_context.queries[0].original_query_indices.extend(origins)
    return request


def test_real_rpc_pre_expansion_then_embedding_preserves_original_context_and_schema(worker):
    path, package = worker.wheel("""
import pyarrow as pa
calls=[]
class UDF:
 def transform_query(self, params, columns, *, context):
  calls.append((context.query_count,context.row_query_origins,len(columns[0])))
  assert context.original_queries[0].values==('a|b','c')
  assert context.queries[0].values==('a|b','c')
  assert context.prepared_query_counts==(2,)
  if params['mode']=='expand':
   assert context.row_query_origins==(0,1)
   return [pa.array([v.as_py().split('|') for v in columns[0]],type=pa.list_(pa.string()))]
  assert context.query_origins==((0,1),)
  assert context.row_query_origins==(0,0,1)
  assert context.input_schema.names==['query']
  assert context.input_schema.metadata[b'request']==b'expanded'
  assert context.input_schema.field(0).metadata[b'milvus.data_type']==b'21'
  field=pa.field('embedding',pa.list_(pa.float32(),2),nullable=False,metadata={'milvus.data_type':'101','dim':'2'})
  return pa.record_batch([pa.array([[float(len(v.as_py())),1.] for v in columns[0]],type=field.type)],
    schema=pa.schema([field],metadata={'model':'deterministic'}))
def factory(context): return UDF()
""")
    field = pa.field("query", pa.string(), nullable=False, metadata={"milvus.data_type": "21"})
    schema = pa.schema([field], metadata={"request": "expanded"})
    batch = pa.record_batch([pa.array(["a|b", "c"])], schema=schema)
    request = request_with_queries(worker, path, [batch], "pre_process")
    request.execution_version = 4  # The first operator keeps the legacy schema contract.
    request.params.fields["mode"].string_value = "expand"
    response = worker.execute(request)
    assert response.WhichOneof("result") == "outputs", response
    expanded = worker_helpers.decode(response.outputs)[0].column(0)
    assert expanded.to_pylist() == [["a", "b"], ["c"]]

    flattened = pa.record_batch([pc.list_flatten(expanded)], schema=schema)
    request = request_with_queries(worker, path, [flattened], "pre_process")
    request.query_context.row_query_origins.extend([0, 0, 1])
    request.input_column_indices.extend([0])
    request.params.fields["mode"].string_value = "embedding"
    response = worker.execute(request)
    assert response.WhichOneof("result") == "outputs", response
    assert response.execution_version == 5
    result = worker_helpers.decode(response.outputs)[0]
    assert result.num_rows == 3
    assert result.schema.metadata == {b"model": b"deterministic"}
    assert result.schema.field(0).metadata == {b"milvus.data_type": b"101", b"dim": b"2"}
    assert result.schema.field(0).nullable is False
    assert importlib.import_module(package).calls == [(2, (0, 1), 2), (2, (0, 0, 1), 3)]


def test_real_rpc_expanded_rerank_maps_child_query_to_original_including_empty_hits(worker):
    path, package = worker.wheel("""
import pyarrow as pa
calls=[]
class UDF:
 def transform_query(self, params, columns, *, context):
  assert context.query_count==2 and context.prepared_query_counts==(3,)
  assert context.query_origins==((0,0,1),) and context.row_query_origins==()
  index=context.query_index
  original=context.original_query_index
  assert original==context.query_origins[0][index]
  calls.append((index,original,len(columns[0])))
  return [pa.array([context.original_queries[0].values[original]]*len(columns[0]),type=pa.string())]
def factory(context): return UDF()
""")
    batches = [pa.record_batch([pa.array(rows, type=pa.string())], names=["text"]) for rows in ([], ["a", "b"], ["c"])]
    request = request_with_queries(worker, path, batches, "L2_rerank", [b"a", b"b", b"c"], [0, 0, 1])
    response = worker.execute(request)
    assert response.WhichOneof("result") == "outputs", response
    assert [batch.column(0).to_pylist() for batch in worker_helpers.decode(response.outputs)] == [
        [],
        ["a|b", "a|b"],
        ["c"],
    ]
    assert importlib.import_module(package).calls == [(0, 0, 0), (1, 0, 2), (2, 1, 1)]


def test_real_rpc_post_table_preserves_independent_ann_query_mappings(worker):
    path, package = worker.wheel("""
calls=0
class UDF:
 def transform_batch(self, params, batches, *, context):
  global calls
  calls+=1
  assert context.table_result and context.query_count==2
  assert context.query_index is None and context.original_query_index is None
  assert context.prepared_query_counts==(3,1)
  assert context.query_origins==((0,0,1),(1,))
  assert context.row_query_origins==()
  assert context.result_offsets==(0,2,2,3,4)
  return batches
def factory(context): return UDF()
""")
    batches = [
        pa.record_batch([pa.array(rows, type=pa.string())], names=["text"]) for rows in (["a", "b"], [], ["c"], ["c"])
    ]
    request = request_with_queries(worker, path, batches, "post_process", [b"a", b"b", b"c"], [0, 0, 1])
    request.table_result = True
    request.query_context.original_queries.add(data_type="VarChar", tag="$0", values=[b"a|b", b"c"])
    request.query_context.queries.add(data_type="VarChar", tag="$0", values=[b"c"], original_query_indices=[1])
    response = worker.execute(request)
    assert response.WhichOneof("result") == "outputs", response
    assert [batch.num_rows for batch in worker_helpers.decode(response.outputs)] == [2, 0, 1, 1]
    assert importlib.import_module(package).calls == 1


@pytest.mark.parametrize("origins", [[0], [0, 2, 1], [0, 1, 0], [0, -1, 1]])
def test_real_rpc_rejects_invalid_query_mapping_before_loading(worker, origins):
    batches = [pa.record_batch([pa.array(["candidate"])], names=["text"])] * 3
    request = request_with_queries(
        worker, worker.directory / "not-loaded.whl", batches, "L2_rerank", [b"a", b"b", b"c"], origins
    )
    worker.assert_code(worker.execute(request), wire.INTERNAL)


def test_real_rpc_mapping_requires_v5_and_pre_row_mapping_handles_typed_empty(worker):
    path, _ = worker.wheel("""
class UDF:
 def transform_query(self,params,columns,*,context):
  assert context.query_count==2 and context.row_query_origins==()
  assert len(columns[0])==0 and context.input_schema.field(0).type.__str__()=='string'
  return columns
def factory(context): return UDF()
""")
    batch = pa.record_batch([pa.array([], type=pa.string())], names=["query"])
    request = request_with_queries(worker, path, [batch], "pre_process")
    response = worker.execute(request)
    assert response.WhichOneof("result") == "outputs", response
    assert worker_helpers.decode(response.outputs)[0].num_rows == 0
    request.execution_version = 4
    worker.assert_code(worker.execute(request), wire.INTERNAL)

    request = request_with_queries(
        worker, worker.directory / "not-loaded.whl", [batch] * 3, "L2_rerank", [b"a", b"b", b"c"], [0, 0, 1]
    )
    request.execution_version = 4
    worker.assert_code(worker.execute(request), wire.UNSUPPORTED)
