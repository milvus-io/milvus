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

"""Struct array partition isolation regression cases."""

import copy

import pytest
from common.common_type import CaseLabel
from common.struct_array_isolation_helpers import StructArrayIsolationHelpersBase
from common.struct_array_nullable_helpers import (
    _clips,
    _core_children,
    _equal,
    _record,
)
from pymilvus import DataType


class TestMilvusClientStructArrayPartitionIsolation(StructArrayIsolationHelpersBase):
    @pytest.mark.tags(CaseLabel.L2)
    def test_multiple_structs_and_partition_isolation(self):
        """
        target: isolate nullable nested values by Struct, partition and collection
        method: opposing canaries, partition-scoped reads/deletes, schema evolution and reload
        expected: exact retained values and key sets; peer partitions and collections stay unchanged
        """
        client = self._client()
        name = self._create(client, _core_children())
        peer = self._create(client, _core_children())
        peer_model = self._seed(client, peer, [_record(n, _clips(n + 20)) for n in range(12)])
        other = client.create_struct_field_schema()
        other.add_field("score", DataType.INT64, nullable=True)
        other.add_field(
            "tags", DataType.ARRAY, element_type=DataType.INT64, max_capacity=4, nullable=True, element_nullable=True
        )
        client.add_collection_struct_field(name, "other", other, max_capacity=8, nullable=True)
        model = {}
        partitions = {}
        for partition, start in (("left", 0), ("right", 100)):
            client.create_partition(name, partition)
            rows = [_record(n, _clips(n)) for n in range(start, start + 12)]
            for row in rows:
                row["other"] = [{"score": -row["id"], "tags": [None, -row["id"]]}, {"score": None, "tags": None}]
            client.insert(name, rows, partition_name=partition)
            model.update({r["id"]: copy.deepcopy(r) for r in rows})
            partitions[partition] = {r["id"] for r in rows}
        self._reload(client, name)
        for partition, ids in partitions.items():
            rows = client.query(
                name, filter="id >= 0", partition_names=[partition], output_fields=["*"], consistency_level="Strong"
            )
            assert {r["id"] for r in rows} == ids
            for row in rows:
                _equal(row, model[row["id"]])
        client.add_collection_field(name, "clips[added]", DataType.INT64, nullable=True)
        for row in model.values():
            for element in row["clips"] or []:
                element["added"] = None
        client.delete(name, filter="id >= 0", partition_name="left")
        model = {pk: row for pk, row in model.items() if pk in partitions["right"]}
        self._reload(client, name)
        self._check(client, name, model)
        self._check(client, peer, peer_model)
