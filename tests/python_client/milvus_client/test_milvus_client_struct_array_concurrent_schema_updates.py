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

"""Struct array concurrent schema updates regression cases."""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from common.common_type import CaseLabel
from common.struct_array_isolation_helpers import StructArrayIsolationHelpersBase
from common.struct_array_nullable_helpers import (
    _clips,
    _core_children,
    _record,
)
from pymilvus import DataType


class TestMilvusClientStructArrayConcurrentSchemaUpdates(StructArrayIsolationHelpersBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("kind", ["scalar", "nested", "vector"])
    def test_concurrent_writes_and_add_child(self, kind):
        """
        target: preserve acknowledged writes while adding a nullable child to mixed segments
        method: three finite writers and one DDL actor start together, then compare an independent model
        expected: every write survives, new child backfills NULL, indexed vector reload remains valid
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(12)])
        barrier = threading.Barrier(4)
        dtype, options = {
            "scalar": (DataType.INT64, {}),
            "nested": (DataType.ARRAY, {"element_type": DataType.INT64, "max_capacity": 4, "element_nullable": True}),
            "vector": (DataType.FLOAT_VECTOR, {"dim": 8}),
        }[kind]

        def writer(worker):
            barrier.wait(timeout=30)
            rows = []
            for revision in range(6):
                batch = [
                    _record(100 + worker * 100 + j, _clips(j + worker, revision), revision=revision) for j in range(8)
                ]
                result = client.upsert(name, batch, timeout=60)
                assert result["upsert_count"] == len(batch)
                rows = batch
            return rows

        def evolve():
            barrier.wait(timeout=30)
            client.add_collection_field(name, "clips[added]", dtype, nullable=True, timeout=60, **options)

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(writer, i) for i in range(3)]
            ddl = pool.submit(evolve)
            for future in futures:
                model.update({row["id"]: row for row in future.result(timeout=180)})
            ddl.result(timeout=90)
        for row in model.values():
            for element in row["clips"] or []:
                element["added"] = None
        self._check(client, name, model)
        if kind == "vector":
            index = client.prepare_index_params()
            index.add_index("clips[added]", index_type="FLAT", metric_type="L2")
            client.create_index(name, index)
        self._reload(client, name)
        self._check(client, name, model)
