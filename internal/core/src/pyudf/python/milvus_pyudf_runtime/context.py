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

"""Stable initialization context passed to a PyUDF factory."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import platform
import sys
from types import MappingProxyType
from typing import Mapping


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
