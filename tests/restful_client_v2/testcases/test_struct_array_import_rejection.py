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

"""Struct array import rejection regression cases."""

import pytest
from base.struct_array import StructArrayRestTestBase
from utils.constant import CaseLabel
from utils.struct_array_data import make_mutation_rows


class TestStructArrayImportRejection(StructArrayRestTestBase):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("source", ["binlog", "snapshot", "numpy"])
    def test_unsupported_struct_import_creates_no_job_or_rows(self, source):
        """
        target: reject source formats that cannot represent native nullable/nested struct children
        method: submit backup, snapshot and NumPy imports; compare jobs and complete live data
        expected: the documented error is returned before job admission, without changing data
        """
        self.create_struct_collection(
            True,
            dim=24,
            capacity=3,
            extra_fields=[{"fieldName": "tenant", "dataType": "Int32"}],
        )
        rows = make_mutation_rows(self.schema)
        self.assert_success(self.vector_client.vector_insert({"collectionName": self.name, "data": rows}))
        model = {row["id"]: row for row in rows}
        before = self.assert_success(self.import_job_client.list_import_jobs({"collectionName": self.name}))
        files, options, code, message = {
            "binlog": ([["insert", "delta"]], {"backup": "true"}, 1100, "binlog import (backup restore)"),
            "snapshot": (
                [["s3://source/root/snapshots/1/metadata/2.json"]],
                {"backup": "true", "source_type": "snapshot"},
                1100,
                "snapshot-source import",
            ),
            "numpy": (
                [[f"{self.name}/{field}.npy" for field in ("id", "normal_vector", "clips")]],
                {},
                2100,
                "numpy import does not support struct data",
            ),
        }[source]
        response = self.import_job_client.create_import_jobs(
            {
                "collectionName": self.name,
                "partitionName": "_default",
                "files": files,
                "options": options,
            }
        )
        assert response["code"] == code and message in response["message"], response
        assert not response.get("data", {}).get("jobId"), response
        after = self.assert_success(self.import_job_client.list_import_jobs({"collectionName": self.name}))
        assert after == before
        self.assert_rows(model)
