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

"""Worker-local errors carry the category selected at the failing operation."""

from .config import MAX_ERROR_MESSAGE_BYTES
from .proto import pyudf_pb2 as wire


class WorkerError(RuntimeError):
    def __init__(self, message: str, code: int = wire.INTERNAL):
        super().__init__(message)
        self.code = code


class PyUDFLoadError(WorkerError):
    """Load failure; direct resource I/O is classified separately at its source."""

    def __init__(self, message: str, code: int = wire.UDF_FAILED):
        super().__init__(message, code)


class PyUDFExecutionError(WorkerError):
    """User execution/return contract failure, or an explicitly internal invariant."""

    def __init__(self, message: str, code: int = wire.UDF_FAILED):
        super().__init__(message, code)


class RequestInactive(Exception):
    """Control flow: stop future work after the original RPC is cancelled."""


def resource_io_code(error: OSError) -> int:
    if isinstance(error, FileNotFoundError):
        return wire.RESOURCE_NOT_FOUND
    if isinstance(error, PermissionError):
        return wire.RESOURCE_PERMISSION_DENIED
    return wire.RESOURCE_IO_FAILED


def error_response(error: BaseException, code: int) -> wire.ExecuteResponse:
    parts = []
    seen = set()
    current = error
    while current is not None and id(current) not in seen and len(parts) < 4:
        seen.add(id(current))
        try:
            parts.append((str(current) or type(current).__name__)[:MAX_ERROR_MESSAGE_BYTES])
        except BaseException:
            parts.append(type(current).__name__)
        current = current.__cause__
    message = (
        ": ".join(parts).encode("utf-8", errors="replace")[:MAX_ERROR_MESSAGE_BYTES].decode("utf-8", errors="ignore")
    )
    return wire.ExecuteResponse(error=wire.ExecuteError(code=code, message=message))
