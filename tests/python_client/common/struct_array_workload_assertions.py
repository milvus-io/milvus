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

"""Independent nested-value assertions and uncached build identity for CI workloads."""

import pytest
from pymilvus.client.prepare import Prepare
from pymilvus.client.utils import check_status


def _equal(actual, expected):
    if isinstance(expected, dict):
        assert isinstance(actual, dict) and set(actual) == set(expected), (actual, expected)
        for key in expected:
            _equal(actual[key], expected[key])
    elif isinstance(expected, list):
        assert isinstance(actual, list) and len(actual) == len(expected), (actual, expected)
        for left, right in zip(actual, expected):
            _equal(left, right)
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, abs=2e-6, rel=2e-6)
    else:
        assert actual == expected, (actual, expected)


def identity(client):
    # Detailed SDK version metadata is cached by pooled handlers across reconnects.
    response = client._get_connection()._stub.Connect(Prepare.register_request("", ""), timeout=10)
    check_status(response.status)
    info = response.server_info
    return {
        "version": info.build_tags,
        "git_commit": info.git_commit,
        "build_time": info.build_time,
        "go_version": info.go_version,
        "deploy_mode": info.deploy_mode,
    }
