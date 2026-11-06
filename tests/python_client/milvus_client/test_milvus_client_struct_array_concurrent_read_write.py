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

"""Struct array concurrent read write regression cases."""

import json
import os

import allure
import pytest
from common import common_func as cf
from common.common_type import CaseLabel
from common.struct_array_concurrent_workload import MixedHistory, WorkloadConfig
from common.struct_array_nullable_helpers import StructArrayNullableBase


class TestMilvusClientStructArrayConcurrentReadWrite(StructArrayNullableBase):
    skip_global_role_cleanup = True

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("seed,smoke", [(7, True), (41, False), (109, False)], ids=["smoke", "seed41", "seed109"])
    def test_concurrent_reads_upserts_deletes_preserve_acknowledged_rows(self, seed, smoke, request, tmp_path):
        """
        target: preserve Struct values and logical offsets under overlapping DML/read/DDL
        method: per-key histories, immutable search controls, finite actor budgets and reload
        expected: interval-valid reads, exact acknowledged final values and fresh child identities
        """
        client = self._client()
        name = cf.gen_unique_str(f"struct_mixed_{seed}")
        request.addfinalizer(lambda: client.drop_collection(name, timeout=60))
        evidence = os.environ.get("STRUCT_MIXED_EVIDENCE", str(tmp_path))
        overrides = json.loads(os.environ.get("STRUCT_MIXED_CONFIG", "{}"))
        options = {"write_budget": 6 if smoke else 18, "read_budget": 10 if smoke else 30, **overrides}
        history = MixedHistory(client, name, seed, smoke, evidence, config=WorkloadConfig(**options))
        try:
            history.run()
        finally:
            # Preserve the journal even if failure reconciliation itself raises.
            history.journal.close()
            for artifact in sorted(history.path.glob(f"{name}*")):
                if artifact.suffix in (".json", ".jsonl"):
                    allure.attach.file(
                        str(artifact),
                        name=artifact.name,
                        attachment_type=allure.attachment_type.JSON
                        if artifact.suffix == ".json"
                        else allure.attachment_type.TEXT,
                    )
