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

"""Strict Python execution boundary for loaded PyUDF instances."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Any

import pyarrow as pa

from .wrapper import LoadedPyUDFInstance


class PyUDFExecutionError(RuntimeError):
    """Raised when execution or returned values violate the PyUDF contract."""


def freeze_params(value: Any) -> Any:
    """Recursively convert params to immutable mappings and tuples."""
    if isinstance(value, Mapping):
        return MappingProxyType({key: freeze_params(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(freeze_params(item) for item in value)
    return value


def run_transform_query(
    loaded: LoadedPyUDFInstance,
    params: Mapping[str, Any],
    columns: Sequence[pa.Array],
    expected_rows: int,
) -> tuple[pa.Array, ...]:
    """Execute one serialized query and return strictly validated Arrow arrays."""
    if not isinstance(loaded, LoadedPyUDFInstance):
        raise PyUDFExecutionError("loaded value must be a LoadedPyUDFInstance")
    if loaded.callable_name != "transform_query":
        raise PyUDFExecutionError("loaded PyUDF does not implement transform_query")
    if loaded.concurrency_mode != "serialized":
        raise PyUDFExecutionError("transform_query requires serialized concurrency mode")
    if not isinstance(params, Mapping):
        raise PyUDFExecutionError("params must be an immutable mapping")
    if not isinstance(columns, Sequence) or not all(
        isinstance(column, pa.Array) for column in columns
    ):
        raise PyUDFExecutionError("columns must contain only pyarrow.Array objects")
    if not isinstance(expected_rows, int) or isinstance(expected_rows, bool) or expected_rows < 0:
        raise PyUDFExecutionError("expected_rows must be a nonnegative integer")

    try:
        result = getattr(loaded.instance, loaded.callable_name)(params, tuple(columns))
    except Exception as exc:
        raise PyUDFExecutionError("PyUDF transform_query raised an exception") from exc

    if not isinstance(result, Sequence) or isinstance(result, (str, bytes, bytearray)):
        raise PyUDFExecutionError("transform_query must return a sequence of pyarrow.Array")
    outputs = tuple(result)
    if not all(isinstance(output, pa.Array) for output in outputs):
        raise PyUDFExecutionError("transform_query outputs must be pyarrow.Array objects")
    for index, output in enumerate(outputs):
        if len(output) != expected_rows:
            raise PyUDFExecutionError(
                f"transform_query output {index} has {len(output)} rows, expected {expected_rows}"
            )
    return outputs
