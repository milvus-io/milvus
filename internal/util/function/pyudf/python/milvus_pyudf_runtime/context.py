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

    queries/original_queries contain one entry per ANN input, each with NQ
    values. Rerank receives query_index. POST receives every candidate group
    once, with query_index=None; result_offsets also describe a flattened view.
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
