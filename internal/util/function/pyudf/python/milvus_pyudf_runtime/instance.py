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

from collections.abc import Mapping, Sequence
from typing import Any

import pyarrow as pa

from .errors import PyUDFExecutionError, PyUDFLoadError
from .proto import pyudf_pb2 as wire


class PyUDFInstance:
    """Shared across requests; concurrency safety belongs to the user UDF."""

    def __init__(self, instance: Any):
        try:
            transform = getattr(instance, "transform", None)
            transform_query = getattr(instance, "transform_query", None)
            close = getattr(instance, "close", None)
        except MemoryError:
            raise
        except Exception as exc:
            raise PyUDFLoadError("PyUDF instance attribute access raised an exception") from exc
        if callable(transform) == callable(transform_query):
            raise PyUDFLoadError("PyUDF instance must implement exactly one callable transform or transform_query")
        if close is not None and not callable(close):
            raise PyUDFLoadError("PyUDF close attribute must be callable when present")
        self.instance = instance
        self.callable_name = "transform" if callable(transform) else "transform_query"
        self._close = close

    def execute_query(
        self,
        params: Mapping[str, Any],
        columns: Sequence[pa.Array],
    ) -> tuple[pa.Array, ...]:
        """Execute one query without a runtime lock; validate its Arrow outputs."""
        if self.callable_name != "transform_query":
            raise PyUDFExecutionError("loaded PyUDF does not implement transform_query")
        if not isinstance(params, Mapping):
            raise PyUDFExecutionError("params must be an immutable mapping", wire.INTERNAL)
        if not isinstance(columns, Sequence) or not all(isinstance(column, pa.Array) for column in columns):
            raise PyUDFExecutionError("columns must contain only pyarrow.Array objects", wire.INTERNAL)

        try:
            result = getattr(self.instance, self.callable_name)(params, tuple(columns))
        except MemoryError:
            raise
        except Exception as exc:
            raise PyUDFExecutionError("PyUDF transform_query raised an exception") from exc

        if not isinstance(result, Sequence) or isinstance(result, (str, bytes, bytearray)):
            raise PyUDFExecutionError("transform_query must return a sequence of pyarrow.Array")
        try:
            outputs = tuple(result)
        except MemoryError:
            raise
        except Exception as exc:
            raise PyUDFExecutionError("cannot read transform_query output sequence") from exc
        if not all(isinstance(output, pa.Array) for output in outputs):
            raise PyUDFExecutionError("transform_query outputs must be pyarrow.Array objects")
        for index, output in enumerate(outputs):
            if not (
                pa.types.is_boolean(output.type)
                or pa.types.is_signed_integer(output.type)
                or output.type in (pa.float32(), pa.float64(), pa.string())
            ):
                raise PyUDFExecutionError(f"transform_query output {index} has unsupported type {output.type}")
            try:
                output.validate(full=True)
            except MemoryError:
                raise
            except Exception as exc:
                raise PyUDFExecutionError(f"transform_query output {index} is an invalid Arrow array") from exc
            if len(output) != len(outputs[0]):
                raise PyUDFExecutionError("transform_query output columns must have equal lengths")
        return outputs

    def close(self) -> None:
        """Explicit instance cleanup; never called by worker process shutdown."""
        if self._close is not None:
            try:
                self._close()
            except BaseException as exc:
                raise PyUDFLoadError("PyUDF close raised an exception") from exc
