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

"""Struct array field properties regression cases."""

import pytest
from common.common_type import CaseLabel, CheckTasks
from common.struct_array_nullable_helpers import (
    StructArrayNullableBase,
    _clips,
    _core_children,
    _record,
    _vector,
)
from common.struct_array_predicate_helpers import _expression_rows
from pymilvus import DataType
from pymilvus.client.utils import check_status
from pymilvus.grpc_gen import milvus_pb2


class TestMilvusClientStructArrayFieldProperties(StructArrayNullableBase):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("target", ["clips", "clips[label]", "clips[tags]"])
    @pytest.mark.parametrize(
        "property_name,value",
        [("nullable", False), ("element_nullable", False), ("max_capacity", 16), ("max_length", 64)],
    )
    def test_struct_field_immutable_properties(self, target, property_name, value):
        """
        target: preserve struct nullability and layout parameters under AlterCollectionField
        method: reject one property change and compare full schema, rows and a description control
        expected: specific struct-property rejection, no partial change, valid alter still succeeds
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, _expression_rows())
        self.release_collection(client, name)
        before = client.describe_collection(name)
        result, ok = self.alter_collection_field(
            client, name, field_name=target, field_params={property_name: value}, check_task=CheckTasks.check_nothing
        )
        assert not ok and getattr(result, "code", None) == 1100, result
        assert "can not be altered on struct" in str(result), result
        assert client.describe_collection(name) == before
        self.alter_collection_field(
            client, name, field_name=target, field_params={"field.description": "document control"}
        )
        self.load_collection(client, name)
        self._check(client, name, model)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("target", ["clips", "clips[label]", "clips[tags]", "clips[emb]"])
    @pytest.mark.parametrize(
        "property_name,value",
        [
            ("mmap.enabled", "true"),
            ("mmap.enabled", "false"),
            ("warmup", "sync"),
            ("warmup", "async"),
            ("warmup", "disable"),
            ("field.description", "nullable struct description"),
        ],
    )
    def test_struct_field_allowed_properties_persist(self, target, property_name, value):
        """
        target: alter allowed parent/child properties without changing sibling metadata or data
        method: release, alter, describe, reload and check every retained value and sibling field
        expected: property persists across reload and unrelated fields remain identical
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(16)])
        self.flush(client, name)
        self.release_collection(client, name)
        before = client.describe_collection(name)
        self.alter_collection_field(client, name, field_name=target, field_params={property_name: value})
        self.load_collection(client, name)
        self._check(client, name, model)
        after = client.describe_collection(name)
        old_parent = next(f for f in before["fields"] if f["name"] == "clips")
        parent = next(f for f in after["fields"] if f["name"] == "clips")
        selected = (
            parent if target == "clips" else next(f for f in parent["struct_fields"] if f["name"] == target[6:-1])
        )

        def wire_properties():
            response = client._get_connection()._stub.DescribeCollection(
                milvus_pb2.DescribeCollectionRequest(collection_name=name), timeout=30
            )
            check_status(response.status)
            struct = next(f for f in response.schema.struct_array_fields if f.name == "clips")
            field = struct if target == "clips" else next(f for f in struct.fields if f.name == target[6:-1])
            return {p.key: p.value for p in field.type_params}

        if property_name != "field.description":
            assert wire_properties()[property_name] == value
        if property_name == "field.description":
            assert selected["description"] == value, selected
        if target != "clips":
            for old, new in zip(old_parent["struct_fields"], parent["struct_fields"]):
                if old["name"] != selected["name"]:
                    assert old == new
        self._reload(client, name)
        assert client.describe_collection(name) == after
        self._check(client, name, model)
        if property_name != "field.description":
            assert wire_properties()[property_name] == value
            described_key = "mmap_enabled" if property_name == "mmap.enabled" else property_name
            assert str(selected.get("params", {}).get(described_key)).lower() == value, selected

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_child_removes_index_and_readd_has_no_old_values(self):
        """
        target: remove a child and its index and prevent old values returning after name reuse
        method: drop indexed nullable vector, reject old paths, re-add as scalar and reload
        expected: exact keys/index inventory, new field ID, NULL backfill and successful new writes
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(16)])
        old_id = self._child_id(client, name, "emb")
        assert "clips[emb]" in client.list_indexes(name)
        self.drop_collection_field(client, name, field_name="clips[emb]")
        assert "clips[emb]" not in client.list_indexes(name)
        for row in model.values():
            for element in row["clips"] or []:
                del element["emb"]
        self._check(client, name, model)
        result, ok = self.query(
            client, name, filter="id >= 0", output_fields=["clips[emb]"], check_task=CheckTasks.check_nothing
        )
        assert not ok, result
        result, ok = self.search(
            client,
            name,
            [_vector(1)],
            anns_field="clips[emb]",
            limit=1,
            search_params={"metric_type": "L2"},
            check_task=CheckTasks.check_nothing,
        )
        assert not ok, result
        self.add_collection_field(client, name, "clips[emb]", DataType.INT64, nullable=True)
        assert self._child_id(client, name, "emb") > old_id
        for row in model.values():
            for element in row["clips"] or []:
                element["emb"] = None
        self._reload(client, name)
        self._check(client, name, model)
        model[2]["clips"][0]["emb"] = 999
        self.upsert(client, name, [model[2]])
        self._reload(client, name)
        self._check(client, name, model)
