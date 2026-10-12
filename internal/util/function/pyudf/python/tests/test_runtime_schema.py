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

from decimal import Decimal
from types import MappingProxyType

import pyarrow as pa
import pytest
import test_worker as worker_helpers

from milvus_pyudf_runtime.instance import PyUDFInstance
from milvus_pyudf_runtime.ipc import OutputIPC
from milvus_pyudf_runtime.proto import pyudf_pb2 as wire


@pytest.fixture
def worker():
    harness = worker_helpers.WorkerTests()
    harness.setUp()
    try:
        yield harness
    finally:
        harness.tearDown()


def test_real_rpc_exposes_selected_input_schema_and_retains_identity_metadata(worker):
    path, _ = worker.wheel("""
class UDF:
 def transform(self, params, columns, *, context):
  assert context.input_schema.names == ['embedding', 'embedding']
  assert context.input_schema.field(0).nullable is False
  assert context.input_schema.field(0).metadata[b'milvus.data_type'] == b'102'
  assert context.input_schema.field(0).metadata[b'dim'] == b'2'
  return columns
def factory(context): return UDF()
""")
    field = pa.field("embedding", pa.binary(4), nullable=False, metadata={"milvus.data_type": "102", "dim": "2"})
    batch = pa.record_batch([pa.array([b"abcd", b"efgh"], type=field.type)], schema=pa.schema([field]))
    request = worker.request(path, [batch])
    request.stage, request.execution_version = "ingestion", 4
    request.input_column_indices.extend([0, 0])
    response = worker.execute(request)
    assert response.WhichOneof("result") == "outputs", response
    result = worker_helpers.decode(response.outputs)[0]
    assert result.schema.names == ["c0", "c1"]
    assert result.schema.field(0).metadata == field.metadata
    assert result.schema.field(1).metadata == field.metadata
    assert result.schema.field(0).nullable is False
    assert result.column(0).to_pylist() == [b"abcd", b"efgh"]


@pytest.mark.parametrize("as_table", [False, True])
def test_real_rpc_preserves_returned_table_schema_for_empty_groups(worker, as_table):
    path, _ = worker.wheel("""
import pyarrow as pa
class UDF:
 def transform_batch(self, params, batches, *, context):
  assert context.input_schema.names == ['text']
  result=[]
  field=pa.field('embedded',pa.binary(4),nullable=False,metadata={'milvus.data_type':'103','dim':'2'})
  schema=pa.schema([field],metadata={'model':'test-model'})
  for columns in batches:
   values=pa.array([b'abcd']*len(columns[0]),type=field.type)
   batch=pa.record_batch([values],schema=schema)
   result.append(pa.Table.from_batches([batch]) if params['as_table'] else batch)
  return result
def factory(context): return UDF()
""")
    batches = [pa.record_batch([pa.array(rows, type=pa.string())], names=["text"]) for rows in (["a", "b"], [], ["c"])]
    request = worker.request(path, batches)
    request.stage, request.execution_version = "post_process", 4
    request.table_result = True
    request.params.fields["as_table"].bool_value = as_table
    response = worker.execute(request)
    assert response.WhichOneof("result") == "outputs", response
    result = worker_helpers.decode(response.outputs)
    assert [batch.num_rows for batch in result] == [2, 0, 1]
    assert all(batch.schema.metadata == {b"model": b"test-model"} for batch in result)
    assert all(batch.schema.field(0).metadata == {b"milvus.data_type": b"103", b"dim": b"2"} for batch in result)
    assert all(batch.schema.field(0).nullable is False for batch in result)


def test_real_rpc_rejects_logical_schema_change_between_groups(worker):
    path, _ = worker.wheel("""
import pyarrow as pa
class UDF:
 def transform_batch(self, params, batches, *, context):
  result=[]
  for index,columns in enumerate(batches):
   field=pa.field('v',pa.binary(4),metadata={'milvus.data_type':str(102+index),'dim':'2'})
   result.append(pa.record_batch([pa.array([b'abcd']*len(columns[0]),type=field.type)],schema=pa.schema([field])))
  return result
def factory(context): return UDF()
""")
    batch = pa.record_batch([pa.array(["a"])], names=["text"])
    request = worker.request(path, [batch, batch])
    request.stage, request.execution_version, request.table_result = "post_process", 4, True
    response = worker.execute(request)
    assert response.WhichOneof("result") == "error"
    assert response.error.code == wire.UDF_FAILED


def test_temporal_decimal_and_nested_fields_survive_runtime_output_ipc():
    schema = pa.schema(
        [
            pa.field("date", pa.date32()),
            pa.field("time", pa.time64("us")),
            pa.field("timestamp", pa.timestamp("us", "UTC")),
            pa.field("decimal", pa.decimal128(12, 2)),
            pa.field("payload", pa.struct([pa.field("text", pa.large_string())])),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([0], type=schema.field(0).type),
            pa.array([1], type=schema.field(1).type),
            pa.array([2], type=schema.field(2).type),
            pa.array([Decimal("123.45")], type=schema.field(3).type),
            pa.array([{"text": "hello"}], type=schema.field(4).type),
        ],
        schema=schema,
    )

    class UDF:
        def transform(self, params, columns):
            return pa.Table.from_batches([batch])

    outputs = PyUDFInstance(UDF()).execute(MappingProxyType({}), [pa.array(["a"])], "ingestion")
    ipc = OutputIPC()
    ipc.append(outputs)
    result = worker_helpers.decode(ipc.finish(lambda: None))[0]
    assert result.schema.equals(schema, check_metadata=True)
    assert result.to_pydict() == batch.to_pydict()
