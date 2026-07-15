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

"""Arrow C Data helpers used by the embedded PyUDF runtime."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa


class PyUDFArrowError(RuntimeError):
    """Raised when an object violates the trusted Arrow bridge contract."""


def _address(value: int, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise PyUDFArrowError(f"{name} must be a positive integer address")
    return value


def import_array(array_address: int, schema_address: int) -> pa.Array:
    """Consume one ArrowArray/ArrowSchema descriptor pair without copying."""
    return pa.Array._import_from_c(
        _address(array_address, "array_address"),
        _address(schema_address, "schema_address"),
    )


def make_chunked_array(chunks: Sequence[pa.Array]) -> pa.ChunkedArray:
    """Build one ChunkedArray without accepting implicit Python conversions."""
    if not isinstance(chunks, Sequence):
        raise PyUDFArrowError("chunks must be a sequence of pyarrow.Array objects")
    if not all(isinstance(chunk, pa.Array) for chunk in chunks):
        raise PyUDFArrowError("chunks must contain only pyarrow.Array objects")
    try:
        return pa.chunked_array(chunks)
    except (TypeError, ValueError, pa.ArrowException) as exc:
        raise PyUDFArrowError("cannot build PyUDF ChunkedArray") from exc


def export_array(array: pa.Array, array_address: int, schema_address: int) -> None:
    """Move one PyArrow Array into empty ArrowArray/ArrowSchema descriptors."""
    if not isinstance(array, pa.Array):
        raise PyUDFArrowError("export object must be a pyarrow.Array")
    array._export_to_c(
        _address(array_address, "array_address"),
        _address(schema_address, "schema_address"),
    )
