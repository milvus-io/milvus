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

"""Complete Arrow IPC messages: one batch per query, no application fragments."""

from collections.abc import Callable

import pyarrow as pa

from .errors import WorkerError
from .proto import pyudf_pb2 as wire


def decode_inputs(payload: bytes, check_active: Callable[[], None]) -> list[pa.RecordBatch]:
    try:
        check_active()
        with pa.ipc.open_stream(payload, options=pa.ipc.IpcReadOptions(use_threads=False)) as reader:
            batches = []
            for batch in reader:
                check_active()
                batch.validate(full=True)
                batches.append(batch)
        if not batches:
            raise WorkerError("input IPC must contain at least one query batch")
        check_active()
        return batches
    except MemoryError:
        raise
    except (pa.ArrowException, ValueError) as exc:
        raise WorkerError("invalid input Arrow IPC") from exc


class ArrowOutputs(tuple):
    """Validated positional columns with an optional self-describing schema."""

    def __new__(cls, columns, schema=None):
        result = super().__new__(cls, columns)
        result.schema = schema
        return result


class OutputIPC:
    """Collect query batches with consistent schemas and encode the IPC result."""

    def __init__(self):
        self.schema = None
        self.batches = []

    def append(self, outputs: tuple[pa.Array, ...], *, inputs=(), input_schema=None) -> None:
        schema = getattr(outputs, "schema", None)
        if schema is None:
            fields = []
            for i, output in enumerate(outputs):
                field = pa.field(f"c{i}", output.type)
                # Identity outputs can retain logical types without guessing
                # from bytes or from equal values of unrelated columns.
                if input_schema is not None:
                    for index, source in enumerate(inputs):
                        if output is source:
                            field = input_schema.field(index).with_name(f"c{i}")
                            break
                fields.append(field)
            schema = pa.schema(fields)
        if len(schema) != len(outputs) or any(field.type != output.type for field, output in zip(schema, outputs)):
            raise WorkerError("UDF output schema differs from its columns", wire.UDF_FAILED)
        if self.schema is None:
            self.schema = schema
        elif not self.schema.equals(schema, check_metadata=True):
            raise WorkerError("UDF output column count/type differs between query batches", wire.UDF_FAILED)
        if outputs:
            batch = pa.record_batch(list(outputs), schema=schema)
        else:
            # No output columns carry no row count; encode an empty batch.
            batch = pa.RecordBatch.from_struct_array(pa.nulls(0, type=pa.struct([])))
        self.batches.append(batch)

    def finish(self, check_active: Callable[[], None]) -> bytes:
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(
            sink, self.schema, options=pa.ipc.IpcWriteOptions(compression=None, use_threads=False)
        ) as writer:
            for batch in self.batches:
                check_active()
                writer.write_batch(batch)
        check_active()
        return sink.getvalue().to_pybytes()
