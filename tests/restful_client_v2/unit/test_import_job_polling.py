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

"""Import polling regressions that do not require a running Milvus server."""

from types import SimpleNamespace

import pytest
from api import milvus


@pytest.mark.parametrize(
    ("terminal_state", "transition_at", "expected_success", "expected_elapsed"),
    [
        ("Completed", 356.5, True, 360),
        ("Completed", 361, False, 360),
        ("Failed", 12, False, 15),
    ],
)
def test_import_polling_observes_final_state_within_original_deadline(
    monkeypatch, terminal_state, transition_at, expected_success, expected_elapsed
):
    elapsed = 0

    def sleep(seconds):
        nonlocal elapsed
        elapsed += seconds

    def progress(job_id, db_name):
        assert job_id == "job-1" and db_name == "test-db"
        state = terminal_state if elapsed >= transition_at else "Importing"
        return {"code": 0, "data": {"state": state, "jobId": job_id}}

    monkeypatch.setattr(milvus, "time", SimpleNamespace(monotonic=lambda: elapsed, sleep=sleep))
    client = milvus.ImportJobClient("http://unused", "unused")
    monkeypatch.setattr(client, "get_import_job_progress", progress)

    response, succeeded = client.wait_import_job_state("job-1", "Completed", timeout=360, interval=5, db_name="test-db")

    assert succeeded is expected_success
    assert elapsed == expected_elapsed
    expected_state = terminal_state if elapsed >= transition_at else "Importing"
    assert response["data"]["state"] == expected_state
