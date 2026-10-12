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

"""Immutable initialization and per-call context for PyUDF implementations."""

from __future__ import annotations

import logging
import platform
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

import pyarrow as pa


@dataclass(frozen=True, slots=True)
class PyUDFContext:
    """Immutable runtime information that is safe to retain in a UDF instance."""

    resource_name: str
    wheel_path: str
    stage: str
    logger: logging.Logger
    runtime_info: Mapping[str, str] = field(
        default_factory=lambda: MappingProxyType(
            {
                "python_implementation": platform.python_implementation(),
                "python_version": platform.python_version(),
                "python_executable": sys.executable,
            }
        )
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "runtime_info", MappingProxyType(dict(self.runtime_info)))


@dataclass(frozen=True, slots=True)
class PyUDFQueryInput:
    """One ANN input: values[i] belongs to query i.

    VarChar values are strings; vector values retain the original encoded bytes
    and their PlaceholderType name in data_type. No vector precision is lost.
    """

    data_type: str
    values: tuple[str | bytes, ...]
    tag: str


@dataclass(frozen=True, slots=True)
class PyUDFCallContext:
    """Request-owned context; never part of the cached factory instance.

    queries/original_queries contain one entry per ANN input. query_count is
    original NQ; prepared_query_counts may differ per ANN branch. query_origins
    maps every prepared query to its original query. During PRE, queries remains
    the stage-entry snapshot, while row_query_origins follows the current rows.
    Rerank receives the prepared query_index and original_query_index. POST receives every candidate group
    once, with query_index=None; result_offsets also describe a flattened view.
    database/collection/user are host-provided per-call scope. resource_name
    names this operator for logs; the cached factory retains the wheel resource.
    logger binds the four scope fields for this invocation only.
    TABLE groups follow the current operator: they may combine queries or
    partition by keys. Query metadata still describes the original request.
    """

    stage: str
    query_count: int
    query_index: int | None = None
    queries: tuple[PyUDFQueryInput, ...] = ()
    original_queries: tuple[PyUDFQueryInput, ...] = ()
    result_offsets: tuple[int, ...] = ()
    # Table groups describe the current operator inputs, independently of NQ.
    table_result: bool = False
    # A reducer returns one row per group; a map preserves each group's rows.
    reduction: bool = False
    # Fields follow the selected positional arguments, including logical Milvus
    # type metadata. Arrow carries the schema; no function registration is needed.
    input_schema: pa.Schema | None = None
    prepared_query_counts: tuple[int, ...] = ()
    query_origins: tuple[tuple[int, ...], ...] = ()
    row_query_origins: tuple[int, ...] = ()
    original_query_index: int | None = None
    # Trusted host scope; never sourced from UDF params or cached in the factory.
    database: str = ""
    collection: str = ""
    user: str = ""
    resource_name: str = ""
    logger: logging.LoggerAdapter | None = None
