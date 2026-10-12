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

"""A loaded UDF owns its callable and the execution/return contract."""

from __future__ import annotations

import inspect
from collections.abc import Mapping, Sequence
from typing import Any

import pyarrow as pa

from .errors import PyUDFExecutionError, PyUDFLoadError
from .ipc import ArrowOutputs
from .proto import pyudf_pb2 as wire


def _supported_type(dtype: pa.DataType) -> bool:
    if (
        pa.types.is_boolean(dtype)
        or pa.types.is_signed_integer(dtype)
        or pa.types.is_unsigned_integer(dtype)
        or dtype in (pa.float32(), pa.float64(), pa.string(), pa.large_string())
        or pa.types.is_binary(dtype)
        or pa.types.is_large_binary(dtype)
        or pa.types.is_fixed_size_binary(dtype)
        or pa.types.is_date32(dtype)
        or pa.types.is_date64(dtype)
        or pa.types.is_time32(dtype)
        or pa.types.is_time64(dtype)
        or pa.types.is_duration(dtype)
        or pa.types.is_timestamp(dtype)
        or pa.types.is_decimal128(dtype)
    ):
        return True
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype):
        return _supported_type(dtype.value_type)
    if pa.types.is_struct(dtype):
        return all(_supported_type(field.type) for field in dtype)
    return False


class PyUDFInstance:
    """Shared across requests; concurrency safety belongs to the user UDF."""

    def __init__(self, instance: Any):
        try:
            transform = getattr(instance, "transform", None)
            transform_query = getattr(instance, "transform_query", None)
            transform_batch = getattr(instance, "transform_batch", None)
            close = getattr(instance, "close", None)
        except MemoryError:
            raise
        except Exception as exc:
            raise PyUDFLoadError("PyUDF instance attribute access raised an exception") from exc
        methods = {"transform": transform, "transform_query": transform_query, "transform_batch": transform_batch}
        callables = [name for name, method in methods.items() if callable(method)]
        if len(callables) != 1:
            raise PyUDFLoadError(
                "PyUDF instance must implement exactly one callable transform or transform_query or transform_batch"
            )
        if close is not None and not callable(close):
            raise PyUDFLoadError("PyUDF close attribute must be callable when present")
        self.instance = instance
        self.callable_name = callables[0]
        self._close = close
        # Determine the signature before execution. Never retry a user call on
        # TypeError: its body may already have performed external side effects.
        try:
            signature = inspect.signature(methods[self.callable_name])
            signature.bind(None, None, context=None)
            self._accepts_context = True
        except (TypeError, ValueError):
            self._accepts_context = False

    def execute_query(
        self,
        params: Mapping[str, Any],
        columns: Sequence[pa.Array],
    ) -> tuple[pa.Array, ...]:
        """Backward compatible entry for a query-stage callable."""
        return self.execute(params, columns, "l2_rerank")

    def execute(
        self,
        params: Mapping[str, Any],
        columns: Sequence[pa.Array],
        stage: str,
        context=None,
    ) -> tuple[pa.Array, ...]:
        """Execute a logical input group, keeping nested values in Arrow arrays."""
        expected = "transform" if stage == "ingestion" else "transform_query"
        if self.callable_name != expected:
            raise PyUDFExecutionError(f"loaded PyUDF does not implement {expected} for stage {stage}")
        if not isinstance(params, Mapping):
            raise PyUDFExecutionError("params must be an immutable mapping", wire.INTERNAL)
        if not isinstance(columns, Sequence) or not all(isinstance(column, pa.Array) for column in columns):
            raise PyUDFExecutionError("columns must contain only pyarrow.Array objects", wire.INTERNAL)

        result = self._invoke(params, tuple(columns), context)
        return self._validate_outputs(result)

    def execute_batch(self, params, batches, context) -> tuple[tuple[pa.Array, ...], ...]:
        """Invoke POST once for all current groups, including typed empty groups."""
        if self.callable_name != "transform_batch":
            raise PyUDFExecutionError(
                "POST_PROCESS execution version 2 requires transform_batch(params, batches, context=...)"
            )
        if not isinstance(params, Mapping):
            raise PyUDFExecutionError("params must be an immutable mapping", wire.INTERNAL)
        inputs = tuple(tuple(columns) for columns in batches)
        if any(not all(isinstance(column, pa.Array) for column in columns) for columns in inputs):
            raise PyUDFExecutionError("batch columns must contain pyarrow.Array objects", wire.INTERNAL)
        result = self._invoke(params, inputs, context)
        if not isinstance(result, Sequence) or isinstance(result, (str, bytes, bytearray)):
            raise PyUDFExecutionError("transform_batch must return one output-column sequence per query")
        if len(result) != len(inputs):
            raise PyUDFExecutionError("transform_batch changed query group count")
        output = []
        for index, group in enumerate(result):
            values = self._validate_outputs(group)
            if not values:
                raise PyUDFExecutionError("transform_batch must return at least one output column per query")
            expected_rows = 1 if context.reduction else len(inputs[index][0])
            if any(len(value) != expected_rows for value in values):
                if context.reduction:
                    raise PyUDFExecutionError(f"reducer must return exactly one row for group {index}")
                raise PyUDFExecutionError(f"transform_batch changed candidate count for query {index}")
            output.append(values)
        return tuple(output)

    def _invoke(self, params, inputs, context):
        try:
            method = getattr(self.instance, self.callable_name)
            if self._accepts_context:
                return method(params, inputs, context=context)
            return method(params, inputs)
        except MemoryError:
            raise
        except Exception as exc:
            raise PyUDFExecutionError(f"PyUDF {self.callable_name} raised an exception") from exc

    def _validate_outputs(self, result):
        expected = self.callable_name

        schema = None
        if isinstance(result, (pa.RecordBatch, pa.Table)):
            try:
                result.validate(full=True)
                schema = result.schema
                result = (
                    tuple(column.combine_chunks() for column in result.columns)
                    if isinstance(result, pa.Table)
                    else tuple(result.columns)
                )
            except MemoryError:
                raise
            except Exception as exc:
                raise PyUDFExecutionError(f"{expected} returned an invalid Arrow table") from exc

        if not isinstance(result, Sequence) or isinstance(result, (str, bytes, bytearray)):
            raise PyUDFExecutionError(f"{expected} must return a sequence of pyarrow.Array")
        try:
            outputs = tuple(result)
        except MemoryError:
            raise
        except Exception as exc:
            raise PyUDFExecutionError(f"cannot read {expected} output sequence") from exc
        if not all(isinstance(output, pa.Array) for output in outputs):
            raise PyUDFExecutionError(f"{expected} outputs must be pyarrow.Array objects")
        for index, output in enumerate(outputs):
            if not _supported_type(output.type):
                raise PyUDFExecutionError(f"{expected} output {index} has unsupported type {output.type}")
            try:
                output.validate(full=True)
            except MemoryError:
                raise
            except Exception as exc:
                raise PyUDFExecutionError(f"{expected} output {index} is an invalid Arrow array") from exc
            if len(output) != len(outputs[0]):
                raise PyUDFExecutionError(f"{expected} output columns must have equal lengths")
        return ArrowOutputs(outputs, schema)

    def close(self) -> None:
        """Explicit instance cleanup; never called by worker process shutdown."""
        if self._close is not None:
            try:
                self._close()
            except BaseException as exc:
                raise PyUDFLoadError("PyUDF close raised an exception") from exc
