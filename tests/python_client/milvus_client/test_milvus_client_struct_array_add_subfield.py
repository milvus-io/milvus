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

"""Struct array add subfield regression cases."""

import copy
import time

import pytest
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from common.struct_array_nullable_helpers import (
    StructArrayNullableBase,
    _child,
    _clips,
    _core_children,
    _record,
    _vector,
)
from pymilvus import DataType
from utils.util_log import test_log as log


class TestMilvusClientStructArrayAddSubfield(StructArrayNullableBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("kind", ["scalar", "vector", "array"])
    @pytest.mark.parametrize("parent_nullable", [False, True])
    @pytest.mark.parametrize("before_ddl", ["growing", "sealed"])
    def test_add_child_backfills_each_existing_element(self, kind, parent_nullable, before_ddl):
        """
        target: add a real sub-field to a populated struct without changing original offsets
        method: add scalar/vector/Array to growing or sealed rows, write and drop/re-add it
        expected: N null children per old row, preserved parent validity, fresh IDs on re-add
        """
        client = self._client()
        name = self._create(
            client, {"anchor": _child(DataType.INT64, False), "old": _child(DataType.INT64)}, parent_nullable
        )
        rows = [
            _record(n, None if parent_nullable and n % 11 == 0 else [{"anchor": j, "old": n + j} for j in range(n % 6)])
            for n in range(1200)
        ]
        model = self._seed(client, name, rows, split=False)
        if before_ddl == "sealed":
            self._reload(client, name)
        dtype, options = {
            "scalar": _child(DataType.VARCHAR),
            "vector": _child(DataType.FLOAT_VECTOR),
            "array": _child(DataType.ARRAY, element_type=DataType.INT64, max_capacity=4, element_nullable=True),
        }[kind]
        self.add_collection_field(client, name, "clips[added]", dtype, **options)
        for row in model.values():
            for element in row["clips"] or []:
                element["added"] = None
        self._check(client, name, model)
        if kind == "vector":
            indexes, _ = self.prepare_index_params(client)
            indexes.add_index("clips[added]", index_type="FLAT", metric_type="L2")
            self.create_index(client, name, indexes)
        self.refresh_load(client, name)
        changes = []
        for n in list(range(0, 1200, 17)) + [1300]:
            value = {"scalar": "雪", "vector": _vector(n), "array": [n, None]}[kind]
            patch = _record(
                n, [{"anchor": 0, "old": n, "added": value}, {"anchor": 1, "old": None, "added": None}], revision=3
            )
            changes.append(patch)
        self.upsert(client, name, changes)
        model.update({r["id"]: copy.deepcopy(r) for r in changes})
        self._reload(client, name)
        self._check(client, name, model)
        old_id = self._child_id(client, name, "added")
        self.drop_collection_field(client, name, field_name="clips[added]")
        self.add_collection_field(client, name, "clips[added]", DataType.INT64, nullable=True)
        for row in model.values():
            for element in row["clips"] or []:
                element["added"] = None
        self._check(client, name, model)
        new_id = self._child_id(client, name, "added")
        assert new_id > old_id
        self._reload(client, name)
        self._check(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("path", ["missing[new]", "clips[]", "[new]", "clips[new][leaf]", "clips[anchor]"])
    def test_invalid_add_child_is_non_mutating(self, path):
        """
        target: reject malformed, missing-parent and duplicate child additions atomically
        method: compare schema and full rows around failed DDL, then add a valid child
        expected: rejected DDL leaves schema/data intact and a valid addition still works
        """
        client = self._client()
        name = self._create(client, _core_children())
        model = self._seed(client, name, [_record(n, _clips(n)) for n in range(20)])
        before, _ = self.describe_collection(client, name)
        response, succeeded = self.add_collection_field(
            client, name, path, DataType.INT64, nullable=True, check_task=CheckTasks.check_nothing
        )
        assert not succeeded and getattr(response, "code", None) in (1, 1100), response
        after, _ = self.describe_collection(client, name)
        assert after["fields"] == before["fields"]
        self._check(client, name, model)
        self.add_collection_field(client, name, "clips[valid_new]", DataType.INT64, nullable=True)
        for row in model.values():
            for element in row["clips"] or []:
                element["valid_new"] = None
        self._reload(client, name)
        self._check(client, name, model)

    @pytest.mark.tags(CaseLabel.L2)
    def test_added_children_survive_compaction_and_mmap(self):
        """
        target: materialize child backfill without changing nested validity or original fields
        method: two sealed batches, add scalar/Array children, partial writes, real compaction and mmap
        expected: completed compaction has output segments and all rows/predicates stay correct
        """
        client = self._client()
        name = self._create(client, _core_children())
        rows = [_record(n, _clips(n)) for n in range(1500)]
        model = self._seed(client, name, rows)
        self.flush(client, name)
        self.add_collection_field(client, name, "clips[new_scalar]", DataType.INT64, nullable=True)
        self.add_collection_field(
            client,
            name,
            "clips[new_array]",
            DataType.ARRAY,
            nullable=True,
            element_type=DataType.VARCHAR,
            max_length=16,
            max_capacity=3,
            element_nullable=True,
        )
        for row in model.values():
            for element in row["clips"] or []:
                element.update(new_scalar=None, new_array=None)
        changes = []
        for n in range(0, 1500, 7):
            row = copy.deepcopy(model[n])
            for element in row["clips"] or []:
                element.update(new_scalar=n, new_array=["雪", None])
            changes.append(row)
        self.upsert(client, name, changes)
        model.update({r["id"]: r for r in changes})
        self._reload(client, name)
        self._check(client, name, model)
        deadline = time.monotonic() + 120
        while True:
            compact_id, _ = self.compact(client, name)
            if compact_id > 0:
                break
            assert time.monotonic() < deadline, "No compaction accepted"
            time.sleep(2)
        assert self.wait_for_compaction_ready(client, compact_id, timeout=300)
        # The shared wrapper has no get_compaction_plans method.
        plans = client.get_compaction_plans(compact_id, timeout=30)
        assert any(p.sources and p.targets for p in plans.plans), plans
        log.info(f"STRUCT_ENHANCE_COMPACTION id={compact_id} plans={plans}")
        self.release_collection(client, name)
        self.alter_collection_properties(client, name, properties={"mmap.enabled": True})
        self.load_collection(client, name)
        self._check(client, name, model)
        self._predicates(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    def test_new_vector_child_search_ready_after_refresh(self):
        """
        target: make a newly indexed vector child searchable when refresh reports completion
        method: add to sealed rows, create FLAT index, refresh, search before any new write
        expected: existing null children yield zero hits without a field-not-loaded error
        """
        client = self._client()
        # Match the minimal reproducer: no unrelated top-level vector index.
        name = cf.gen_unique_str("struct_refresh")
        schema, _ = self.create_schema(client, auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        struct, _ = self.create_struct_field_schema(client)
        struct.add_field("v", DataType.FLOAT_VECTOR, dim=2, nullable=True)
        struct.add_field("score", DataType.INT64, nullable=True)
        schema.add_field(
            "clips", DataType.ARRAY, element_type=DataType.STRUCT, struct_schema=struct, max_capacity=8, nullable=False
        )
        indexes, _ = self.prepare_index_params(client)
        indexes.add_index("clips[v]", index_type="FLAT", metric_type="L2")
        self.create_collection(client, name, schema=schema, index_params=indexes, consistency_level="Strong")
        rows = [{"id": n, "clips": [{"v": [float(n), 0.0], "score": n}, {"v": None, "score": None}]} for n in range(5)]
        model = self._seed(client, name, rows, split=False)
        self._reload(client, name)
        self.add_collection_field(client, name, "clips[new_vector]", DataType.FLOAT_VECTOR, dim=2, nullable=True)
        indexes, _ = self.prepare_index_params(client)
        indexes.add_index("clips[new_vector]", index_type="FLAT", metric_type="L2")
        self.create_index(client, name, indexes)
        self.refresh_load(client, name)
        result, _ = self.search(
            client, name, [[7.0, 0.0]], anns_field="clips[new_vector]", limit=10, search_params={"metric_type": "L2"}
        )
        assert result == [[]]
        for row in model.values():
            for element in row["clips"] or []:
                element["new_vector"] = None
        self._check(client, name, model)
