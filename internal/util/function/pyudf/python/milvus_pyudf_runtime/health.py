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

"""Standard unary health interface exposing only this worker's local state.

Import this module only inside workers, after fork. It imports gRPC.
"""

from collections.abc import Callable

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc

from .proto import pyudf_pb2

HEALTH_SERVICE_NAME = pyudf_pb2.DESCRIPTOR.services_by_name["PyUDFWorker"].full_name


class WorkerHealthServicer(health_pb2_grpc.HealthServicer):
    """Report local serving state when explicitly queried; never probe other workers."""

    def __init__(self, is_serving: Callable[[], bool]):
        self._is_serving = is_serving

    def Check(self, request, context):
        if request.service not in ("", HEALTH_SERVICE_NAME):
            context.abort(grpc.StatusCode.NOT_FOUND, "unknown PyUDF health service")
        status = (
            health_pb2.HealthCheckResponse.SERVING if self._is_serving() else health_pb2.HealthCheckResponse.NOT_SERVING
        )
        return health_pb2.HealthCheckResponse(status=status)


def register_health_service(server, is_serving: Callable[[], bool]) -> None:
    """Register Health/Check on the Execute server and its existing gRPC executor.

    Go and supervisor do not call this interface automatically. Check adds no
    executor and does not gate Execute. Watch is intentionally unsupported.
    """
    health_pb2_grpc.add_HealthServicer_to_server(WorkerHealthServicer(is_serving), server)
