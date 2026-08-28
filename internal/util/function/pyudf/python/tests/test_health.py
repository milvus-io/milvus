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

"""Health/Check reports local worker state and respects the protobuf service name."""

import sys
import unittest
from pathlib import Path
from unittest.mock import Mock

import grpc
from grpc_health.v1 import health_pb2

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from milvus_pyudf_runtime.health import HEALTH_SERVICE_NAME, WorkerHealthServicer  # noqa: E402


class WorkerHealthTests(unittest.TestCase):
    def test_local_serving_state(self):
        serving = False
        service = WorkerHealthServicer(lambda: serving)
        request = health_pb2.HealthCheckRequest(service=HEALTH_SERVICE_NAME)
        context = Mock()
        self.assertEqual(service.Check(request, context).status, health_pb2.HealthCheckResponse.NOT_SERVING)
        serving = True
        self.assertEqual(service.Check(request, context).status, health_pb2.HealthCheckResponse.SERVING)
        context.abort.assert_not_called()

    def test_unknown_service(self):
        context = Mock()
        context.abort.side_effect = RuntimeError("unknown service")
        service = WorkerHealthServicer(lambda: True)
        with self.assertRaises(RuntimeError):
            service.Check(health_pb2.HealthCheckRequest(service="unknown.Service"), context)
        self.assertEqual(context.abort.call_args.args[0], grpc.StatusCode.NOT_FOUND)
        self.assertEqual(
            service.Check(health_pb2.HealthCheckRequest(), Mock()).status, health_pb2.HealthCheckResponse.SERVING
        )


if __name__ == "__main__":
    unittest.main()
