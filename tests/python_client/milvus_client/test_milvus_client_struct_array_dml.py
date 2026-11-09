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

"""Struct Array dml E2E coverage."""

import copy
import json
import random
import time

import pytest
from base.struct_array import MetadataStructArrayTestBase, StructArrayTestBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.struct_array_data import (
    CAPACITY,
    DIM,
    SCALAR_IDS,
    SCALARS,
    assert_values_equal,
    child_schema,
    make_metadata_row,
    make_nullable_elements,
    make_nullable_row,
    make_primary_key,
    make_required_element,
    make_required_elements,
    normalized_vector,
    nullable_children,
    scalar_boundary_value,
)
from common.struct_array_wire import (
    encode_validity_masks,
    iter_wire_fields,
    set_validity_masks,
    wire_child,
    wire_struct,
)
from pymilvus import CollectionSchema, DataType
from pymilvus.client.prepare import Prepare
from pymilvus.client.utils import check_status
from pymilvus.grpc_gen import milvus_pb2, schema_pb2
from utils.util_log import test_log as log


class TestMilvusClientStructArrayDml(MetadataStructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("pk_type", ["int", "str"])
    @pytest.mark.parametrize("initial", ["mixed", "all_null"])
    @pytest.mark.parametrize("seed", [17, 59, 101])
    def test_insert_upsert_delete_preserve_seeded_struct_rows(self, pk_type, initial, seed):
        """
        target: preserve every field through mixed old/new-key mutation histories
        method: execute 24 deterministic state transitions and three flush/reload checkpoints
        expected: complete rows match an independent PK map after every acknowledged mutation
        """
        client = self._client()
        name, schema = self.create_metadata_collection(client, pk_type=pk_type)
        rows = [make_metadata_row(schema, i) for i in range(24)]
        if initial == "all_null":
            for row in rows:
                row["clips"] = None
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        self.insert(client, name, rows[:12])
        self.flush(client, name)
        self.insert(client, name, rows[12:])
        rng = random.Random(seed)
        history = []
        write_failures = []
        deleted = []
        for step in range(24):
            before = copy.deepcopy(model)
            operation = (
                "partial_omit",
                "partial_null",
                "partial_empty",
                "partial_replace",
                "full_replace",
                "delete",
                "reinsert",
                "insert",
            )[step % 8]
            existing = rng.sample(sorted(model), min(2, len(model)))
            if operation == "delete":
                self.delete(client, name, ids=existing)
                deleted = existing
                for pk in existing:
                    del model[pk]
                history.append({"step": step, "operation": operation, "ids": existing})
            else:
                ids = deleted if operation == "reinsert" else existing + [make_primary_key(1000 + step, pk_type)]
                if operation == "insert":
                    ids = [make_primary_key(2000 + step, pk_type)]
                batch = []
                for pk in ids:
                    number = pk if pk_type == "int" else int(pk.rsplit("_", 1)[1])
                    row = make_metadata_row(schema, number, step + 1)
                    if operation.startswith("partial"):
                        patch = {key: row[key] for key in ("id", "revision", "bucket", "normal_vector")}
                        if operation != "partial_omit":
                            patch["clips"] = {
                                "partial_null": None,
                                "partial_empty": [],
                                "partial_replace": row["clips"],
                            }[operation]
                        expected = copy.deepcopy(model.get(pk, {**row, "note": None, "clips": None}))
                        expected.update(copy.deepcopy(patch))
                        model[pk] = expected
                        batch.append(patch)
                    else:
                        model[pk] = copy.deepcopy(row)
                        batch.append(row)
                history.append({"step": step, "operation": operation, "rows": copy.deepcopy(batch)})
                log.info(f"STRUCT_REQUEST seed={seed}: {json.dumps(history[-1])}")
                if operation == "insert":
                    self.insert(client, name, batch)
                else:
                    response, succeeded = self.upsert(
                        client,
                        name,
                        batch,
                        partial_update=operation.startswith("partial"),
                        check_task=CheckTasks.check_nothing,
                    )
                    if not succeeded:
                        # Retain the failure, verify rejection did not change data,
                        # and finish the history before classifying the known rejection.
                        self.assert_metadata_rows(client, name, before, pk_type)
                        model = before
                        failure = {"request": history[-1], "failure": str(response)}
                        write_failures.append(failure)
            log.info(f"STRUCT_HISTORY seed={seed} pk={pk_type} initial={initial}: {json.dumps(history[-1])}")
            try:
                self.assert_metadata_rows(client, name, model, pk_type)
                if step % 8 == 7:
                    self.reload_collection(client, name)
                    self.assert_metadata_rows(client, name, model, pk_type)
                    self.assert_required_predicates(client, name, model)
            except AssertionError as error:
                raise AssertionError(f"seed={seed}; history={json.dumps(history)}") from error
        assert not write_failures, json.dumps({"seed": seed, "failed_writes": write_failures})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("capacity,dim", [(1, 16), (2, 24), (128, 64), (4096, 128)])
    def test_struct_array_capacity_boundary_and_integer_extremes(self, capacity, dim):
        """
        target: retain exact INT64 and UTF-8 values at array capacity boundaries
        method: write NULL, empty, one, capacity-1 and capacity arrays and reload twice
        expected: no truncation, integer precision loss, array reordering or NULL/empty collapse
        """
        client = self._client()
        name, schema = self.create_metadata_collection(
            client,
            capacity=capacity,
            vectors=False,
            dim=dim,
            extra_fields=[cf.gen_int32_field(name="tenant"), cf.gen_string_field(name="region", max_length=12)],
        )
        rows = [make_metadata_row(schema, i) for i in range(5)]
        for row, length in zip(rows, [None, 0, 1, max(0, capacity - 1), capacity]):
            row["clips"] = (
                None
                if length is None
                else [make_required_element(schema.struct_fields[0], row["id"], j) for j in range(length)]
            )
            for j, element in enumerate(row["clips"] or []):
                element["value"] = [-(2**63), 2**63 - 1, 0][j % 3]
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        self.insert(client, name, rows)
        self.assert_metadata_rows(client, name, model)
        for _ in range(2):
            self.reload_collection(client, name)
            self.assert_metadata_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("position", [0, 3, 6], ids=["first", "middle", "last"])
    @pytest.mark.parametrize(
        "invalid", ["capacity", "missing_child", "null_element", "dimension", "scalar_type", "utf8_length"]
    )
    def test_struct_array_invalid_batch_is_non_mutating_and_recoverable(self, invalid, position):
        """
        target: reject malformed batches without writing their otherwise valid siblings
        method: place one malformed row at three batch positions, compare full state, then retry corrected data
        expected: input rejection, unchanged collection and successful complete corrected batch
        """
        client = self._client()
        name, schema = self.create_metadata_collection(client)
        existing = [make_metadata_row(schema, i) for i in range(14)]
        model = {row["id"]: copy.deepcopy(row) for row in existing}
        self.insert(client, name, existing)
        self.reload_collection(client, name)
        good = [make_metadata_row(schema, 100 + i) for i in range(7)]
        for row in good:
            row["clips"] = [make_required_element(schema.struct_fields[0], row["id"], 0)]
        bad = copy.deepcopy(good)
        target = bad[position]
        if invalid == "capacity":
            target["clips"] *= CAPACITY + 1
        elif invalid == "missing_child":
            del target["clips"][0]["value"]
        elif invalid == "null_element":
            target["clips"][0] = None
        elif invalid == "dimension":
            target["clips"][0]["emb"] = [1.0] * (DIM - 1)
        elif invalid == "scalar_type":
            target["clips"][0]["value"] = "not_an_integer"
        else:
            target["clips"][0]["tag"] = "雪" * 22
        error, _ = self.insert(client, name, bad, check_task=CheckTasks.err_res, check_items={ct.err_msg: ""})
        assert error.code in (1, 1100), (invalid, error)
        words = {
            "capacity": ("capacity", "length"),
            "missing_child": ("missing", "field", "required"),
            "null_element": ("null", "none", "dict"),
            "dimension": ("dim", "size", "length"),
            "scalar_type": ("int", "type", "field"),
            "utf8_length": ("length", "string"),
        }[invalid]
        assert any(word in error.message.lower() for word in words), (invalid, error)
        self.assert_metadata_rows(client, name, model)
        self.insert(client, name, good)
        model.update({row["id"]: row for row in good})
        self.reload_collection(client, name)
        self.assert_metadata_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("seed", [11, 37, 73])
    def test_struct_array_sibling_offsets_and_partial_update_are_independent(self, seed):
        """
        target: isolate offsets and row validity between two structs with identical child names
        method: use different NULL/length distributions and alternately replace one struct across reloads
        expected: omitted sibling fields remain bit-for-bit logically unchanged
        """
        client = self._client()
        name, schema = self.create_metadata_collection(client, second=True)
        rows = [make_metadata_row(schema, i) for i in range(48)]
        for row in rows:
            row["other"] = make_required_elements(schema.struct_fields[1], row["id"] + seed, capacity=3)
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        self.insert(client, name, rows)
        for revision in range(1, 7):
            field = "clips" if revision % 2 else "other"
            batch = []
            for pk in range(revision % 3, 48, 3):
                patch = {
                    "id": pk,
                    field: make_required_elements(
                        next(struct for struct in schema.struct_fields if struct.name == field),
                        pk + seed,
                        revision,
                        capacity=revision,
                    ),
                }
                batch.append(patch)
                model[pk].update(copy.deepcopy(patch))
            self.upsert(client, name, batch, partial_update=True)
            self.assert_metadata_rows(client, name, model)
            self.reload_collection(client, name)
            self.assert_metadata_rows(client, name, model)
        self.assert_element_search(client, name, model, "L2", "FLAT", "clips")
        self.assert_element_search(client, name, model, "L2", "FLAT", "other")

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("pk_type", ["int", "str"])
    def test_struct_array_partition_iterator_after_delete_and_partial_update(self, pk_type):
        """
        target: isolate partition state and retain complete array projections in iterators
        method: delete and partially update one partition, then iterate both across reload
        expected: exact partition-specific PK maps with no duplicates or missing rows
        """
        client = self._client()
        name, schema = self.create_metadata_collection(client, pk_type=pk_type, second=True)
        models = {}
        for part, start in (("left_part", 0), ("right_part", 1000)):
            self.create_partition(client, name, part)
            rows = [make_metadata_row(schema, i) for i in range(start, start + 73)]
            for row in rows:
                number = row["id"] if pk_type == "int" else int(row["id"].rsplit("_", 1)[1])
                row["other"] = make_required_elements(schema.struct_fields[1], number + 3, capacity=2)
            self.insert(client, name, rows, partition_name=part)
            models[part] = {row["id"]: copy.deepcopy(row) for row in rows}
        self.reload_collection(client, name)
        deleted = [make_primary_key(i, pk_type) for i in range(0, 73, 5)]
        self.delete(client, name, ids=deleted, partition_name="left_part")
        for pk in deleted:
            del models["left_part"][pk]
        patches = []
        for number in range(1, 73, 5):
            patch = {
                "id": make_primary_key(number, pk_type),
                "clips": make_required_elements(schema.struct_fields[0], number, 8),
                "revision": 8,
            }
            patches.append(patch)
            models["left_part"][patch["id"]].update(copy.deepcopy(patch))
        self.upsert(client, name, patches, partial_update=True, partition_name="left_part")
        for phase in range(2):
            for part, model in models.items():
                self.assert_metadata_rows(client, name, model, pk_type, [part])
                iterator, _ = self.query_iterator(
                    client,
                    name,
                    filter="id >= 0" if pk_type == "int" else 'id != ""',
                    output_fields=["*"],
                    batch_size=7,
                    partition_names=[part],
                    consistency_level="Strong",
                )
                seen = {}
                try:
                    while True:
                        batch = iterator.next()
                        if not batch:
                            break
                        for row in batch:
                            assert row["id"] not in seen
                            seen[row["id"]] = row
                finally:
                    iterator.close()
                assert set(seen) == set(model)
                for pk, expected in model.items():
                    assert_values_equal(seen[pk], expected, exact_fields=False)
            self.reload_collection(client, name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("new_field_vectors", [False, True], ids=["scalar", "vector"])
    def test_struct_array_schema_evolution_mutation_and_compaction(self, new_field_vectors):
        """
        target: retain old-row backfill and independent offsets across schema changes and compaction
        method: add a struct to existing sealed rows, mix new/old-key partial writes, compact and reload
        expected: exact complete data and element search before and after a completed compaction
        """
        client = self._client()
        name, schema = self.create_metadata_collection(client)
        rows = [make_metadata_row(schema, i) for i in range(1200)]
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        for batch in (rows[:600], rows[600:]):
            self.insert(client, name, batch)
            self.flush(client, name)
        added_children = self.required_child_schema(client, vectors=new_field_vectors)
        self.add_collection_struct_field(
            client,
            name,
            "added",
            added_children,
            CAPACITY,
        )
        schema.add_field(
            "added",
            DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=added_children,
            max_capacity=CAPACITY,
            nullable=True,
        )
        for row in model.values():
            row["added"] = None
        if new_field_vectors:
            indexes, _ = self.prepare_index_params(client)
            indexes.add_index("added[emb]", index_type="FLAT", metric_type="L2")
            self.create_index(client, name, indexes)
        self.refresh_load(client, name)
        self.assert_metadata_rows(client, name, model)
        patches = []
        for number in list(range(0, 1200, 3)) + list(range(1200, 1260)):
            source = make_metadata_row(schema, number, 4)
            patch = {key: source[key] for key in ("id", "revision", "bucket", "normal_vector")}
            patch["added"] = make_required_elements(schema.struct_fields[-1], number + 2, 4)
            expected = copy.deepcopy(model.get(number, {**source, "note": None, "clips": None}))
            expected.update(copy.deepcopy(patch))
            model[number] = expected
            patches.append(patch)
        self.upsert(client, name, patches, partial_update=True)
        deleted = list(range(1, 1200, 7))
        self.delete(client, name, ids=deleted)
        for pk in deleted:
            del model[pk]
        self.reload_collection(client, name)
        self.assert_metadata_rows(client, name, model)
        self.assert_element_search(client, name, model, "L2", "FLAT")
        assert self.wait_for_index_ready(client, name, index_name="normal_vector", timeout=300)
        deadline = time.monotonic() + 120
        compact_id = -1
        while time.monotonic() < deadline:
            compact_id, _ = self.compact(client, name)
            if compact_id > 0:
                break
            time.sleep(2)
        assert compact_id > 0, "No compaction accepted within the bounded polling window"
        assert self.wait_for_compaction_ready(client, compact_id, timeout=300)
        log.info(f"STRUCT_COMPACTION collection={name} completed_id={compact_id}")
        self.reload_collection(client, name)
        self.assert_metadata_rows(client, name, model)
        self.assert_required_predicates(client, name, model)
        self.assert_element_search(client, name, model, "L2", "FLAT")
        if new_field_vectors:
            self.assert_element_search(client, name, model, "L2", "FLAT", "added")


class TestMilvusClientStructArrayPartitionIsolation(StructArrayTestBase):
    skip_global_role_cleanup = True

    @pytest.mark.tags(CaseLabel.L2)
    def test_multiple_structs_and_partition_isolation(self):
        """
        target: isolate nullable nested values by Struct, partition and collection
        method: opposing canaries, partition-scoped reads/deletes, schema evolution and reload
        expected: exact retained values and key sets; peer partitions and collections stay unchanged
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        peer, peer_schema = self.create_struct_collection(client, nullable_children())
        peer_model = self.insert_rows_with_model(
            client,
            peer,
            [
                make_nullable_row(peer_schema, n, make_nullable_elements(peer_schema.struct_fields[0], n + 20))
                for n in range(12)
            ],
        )
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
            rows = [
                make_nullable_row(schema, n, make_nullable_elements(schema.struct_fields[0], n))
                for n in range(start, start + 12)
            ]
            for row in rows:
                row["other"] = [{"score": -row["id"], "tags": [None, -row["id"]]}, {"score": None, "tags": None}]
            client.insert(name, rows, partition_name=partition)
            model.update({r["id"]: copy.deepcopy(r) for r in rows})
            partitions[partition] = {r["id"] for r in rows}
        self.reload_collection(client, name)
        for partition, ids in partitions.items():
            rows = client.query(
                name, filter="id >= 0", partition_names=[partition], output_fields=["*"], consistency_level="Strong"
            )
            assert {r["id"] for r in rows} == ids
            for row in rows:
                assert_values_equal(row, model[row["id"]])
        client.add_collection_field(name, "clips[added]", DataType.INT64, nullable=True)
        for row in model.values():
            for element in row["clips"] or []:
                element["added"] = None
        client.delete(name, filter="id >= 0", partition_name="left")
        model = {pk: row for pk, row in model.items() if pk in partitions["right"]}
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)
        self.assert_rows(client, peer, peer_model)


class TestMilvusClientStructArrayNullableScalarChildren(StructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dtype", SCALARS, ids=SCALAR_IDS)
    @pytest.mark.parametrize("parent_nullable", [False, True], ids=["required_parent", "nullable_parent"])
    def test_nullable_scalar_child_preserves_null_slots(self, dtype, parent_nullable):
        """
        target: distinguish a null child from a null parent or an empty struct array
        method: all scalar types, independent child masks, omissions, replacement and reload
        expected: every logical element, value and schema nullability matches retained inputs
        """
        client = self._client()
        name, schema = self.create_struct_collection(
            client, {"anchor": child_schema(DataType.INT64, False), "value": child_schema(dtype)}, parent_nullable
        )
        rows = []
        for n in range(64):
            clips = [
                {"anchor": j, "value": None if (n + j) % 3 == 0 else scalar_boundary_value(dtype, n + j)}
                for j in range(n % 7)
            ]
            if n % 11 == 0 and parent_nullable:
                clips = None
            rows.append(make_nullable_row(schema, n, clips))
        model = self.insert_rows_with_model(client, name, rows)
        self.assert_rows(client, name, model)
        changes = [
            make_nullable_row(
                schema,
                n,
                [{"anchor": 0, "value": None}, {"anchor": 1, "value": scalar_boundary_value(dtype, n)}],
                revision=1,
            )
            for n in range(0, 72, 3)
        ]
        self.upsert(client, name, changes)
        model.update({r["id"]: r for r in changes})
        omitted = make_nullable_row(schema, 100, [{"anchor": 0}, {"anchor": 1}])
        self.insert(client, name, [omitted])
        model[100] = {**copy.deepcopy(omitted), "clips": [{"anchor": 0, "value": None}, {"anchor": 1, "value": None}]}
        self.assert_rows(client, name, model)
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)
        actual, _ = self.query(
            client, name, filter="MATCH_ANY(clips, $[value] IS NULL)", output_fields=["id"], limit=1000
        )
        assert {r["id"] for r in actual} == {
            pk for pk, r in model.items() if any(e["value"] is None for e in r["clips"] or [])
        }

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("pk_type", ["int", "str"])
    @pytest.mark.parametrize("seed", [41, 109])
    def test_upsert_delete_reinsert_preserve_nullable_children(self, pk_type, seed):
        """
        target: retain null children and nested arrays through mixed-key mutations
        method: 24 seeded omit/replace/null/empty/delete/reinsert transitions with full models
        expected: acknowledged writes, fields and element predicates survive every reload
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children(), pk_type=pk_type)
        model = self.insert_rows_with_model(
            client,
            name,
            [make_nullable_row(schema, n, make_nullable_elements(schema.struct_fields[0], n)) for n in range(40)],
        )
        rng = random.Random(seed)
        for step in range(24):
            ids = rng.sample(sorted(model), 2)
            if step % 6 == 4:
                self.delete(client, name, ids=ids)
                for pk in ids:
                    del model[pk]
            else:
                ids.append(make_nullable_row(schema, 1000 + step, [])["id"])
                batch = []
                for pk in ids:
                    n = pk if pk_type == "int" else int(pk.rsplit("_", 1)[1])
                    source = make_nullable_row(
                        schema, n, make_nullable_elements(schema.struct_fields[0], n, step + 1), step + 1
                    )
                    if step % 6 == 0:
                        patch = {k: source[k] for k in ("id", "normal_vector", "revision")}
                        expected = copy.deepcopy(model.get(pk, {**source, "clips": None}))
                        expected.update(patch)
                    else:
                        source["clips"] = None if step % 6 == 2 else [] if step % 6 == 3 else source["clips"]
                        patch = source
                        expected = copy.deepcopy(source)
                    batch.append(patch)
                    model[pk] = expected
                log.info(f"STRUCT_ENHANCE_HISTORY seed={seed} step={step} payload={json.dumps(batch)}")
                self.upsert(client, name, batch, partial_update=step % 6 != 5)
            self.assert_rows(client, name, model)
            if step % 8 == 7:
                self.reload_collection(client, name)
                self.assert_rows(client, name, model)
                self.assert_nullable_predicates(client, name, model)


class TestMilvusClientStructArrayNestedArrayChildren(StructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dtype", SCALARS, ids=SCALAR_IDS)
    @pytest.mark.parametrize(
        "inner_nullable,leaf_nullable", [(False, False), (False, True), (True, False), (True, True)]
    )
    def test_array_child_independent_null_levels(self, dtype, inner_nullable, leaf_nullable):
        """
        target: preserve inner-array and leaf validity independently for every scalar leaf type
        method: write empty, boundary-length, null-inner and null-leaf values; replace and reload
        expected: exact nested values, independent capacities and no null-to-zero coercion
        """
        client = self._client()
        options = dict(element_type=dtype, max_capacity=4, element_nullable=leaf_nullable)
        if dtype == DataType.VARCHAR:
            options["max_length"] = 32
        name, schema = self.create_struct_collection(
            client,
            {
                "anchor": child_schema(DataType.INT64, False),
                "tags": child_schema(DataType.ARRAY, inner_nullable, **options),
            },
        )
        rows = []
        for n in range(64):
            clips = []
            for j in range(n % 7):
                tags = [
                    None if leaf_nullable and (n + j + k) % 3 == 0 else scalar_boundary_value(dtype, n + j + k)
                    for k in range((n + j) % 5)
                ]
                if inner_nullable and (n + j) % 4 == 0:
                    tags = None
                clips.append({"anchor": j, "tags": tags})
            rows.append(make_nullable_row(schema, n, None if n % 13 == 0 else clips))
        model = self.insert_rows_with_model(client, name, rows)
        self.assert_rows(client, name, model)
        for n in range(0, 64, 5):
            patch = {
                "id": n,
                "clips": [{"anchor": 0, "tags": []}, {"anchor": 1, "tags": [scalar_boundary_value(dtype, n)]}],
            }
            self.upsert(client, name, [patch], partial_update=True)
            model[n].update(copy.deepcopy(patch))
        desc, _ = self.describe_collection(client, name)
        restored = CollectionSchema.construct_from_dict(desc).struct_fields[0]
        tags = next(field for field in restored.fields if field.name == "tags")
        assert restored.max_capacity == 8 and restored.nullable
        assert tags.nullable == inner_nullable and tags.element_nullable == leaf_nullable
        assert tags.element_type == dtype and tags.params["max_capacity"] == 4
        if not inner_nullable:
            self.assert_insert_rejected(
                client, name, [make_nullable_row(schema, 900, [{"anchor": 0, "tags": None}])], model
            )
        if not leaf_nullable:
            self.assert_insert_rejected(
                client, name, [make_nullable_row(schema, 901, [{"anchor": 0, "tags": [None]}])], model
            )
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)
        actual, _ = self.query(
            client, name, filter="MATCH_ANY(clips, array_length($[tags]) == 0)", output_fields=["id"], limit=1000
        )
        assert {r["id"] for r in actual} == {
            pk for pk, r in model.items() if any(e["tags"] == [] for e in r["clips"] or [])
        }

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "malformation",
        [
            "required_null",
            "required_missing",
            "null_struct_element",
            "outer_capacity",
            "inner_capacity",
            "leaf_type",
            "vector_dimension",
            "string_length",
        ],
    )
    @pytest.mark.parametrize("bad_position", [0, 1, 2], ids=["first", "middle", "last"])
    def test_invalid_nested_batch_preserves_data_and_recovers(self, malformation, bad_position):
        """
        target: reject invalid child payloads without admitting valid sibling rows
        method: place one malformed row at each batch position, compare model, repair and retry
        expected: parameter rejection, no row changes, complete successful recovery
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client,
            name,
            [make_nullable_row(schema, n, make_nullable_elements(schema.struct_fields[0], n)) for n in range(24)],
        )
        clips = [{"anchor": 0, "score": 1, "label": "ok", "emb": normalized_vector(3), "tags": [1, None]}]
        good = [make_nullable_row(schema, 300 + i, clips) for i in range(3)]
        bad = copy.deepcopy(good)
        elements = bad[bad_position]["clips"]
        if malformation == "required_null":
            elements[0]["anchor"] = None
        elif malformation == "required_missing":
            del elements[0]["anchor"]
        elif malformation == "null_struct_element":
            elements[0] = None
        elif malformation == "outer_capacity":
            bad[bad_position]["clips"] = elements * 9
        elif malformation == "inner_capacity":
            elements[0]["tags"] = [None] * 5
        elif malformation == "leaf_type":
            elements[0]["tags"] = ["wrong"]
        elif malformation == "vector_dimension":
            elements[0]["emb"] = [1.0] * 7
        else:
            elements[0]["label"] = "雪" * 33
        self.assert_insert_rejected(client, name, bad, model)
        self.insert(client, name, good)
        model.update({r["id"]: r for r in good})
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)


class TestMilvusClientStructArrayPartialUpdatePaths(StructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("target", ["score", "emb", "tags", "element"])
    @pytest.mark.parametrize("null_value", [False, True])
    def test_path_replace_preserves_unselected_nullable_children(self, target, null_value):
        """
        target: update one logical child or element while preserving other values and validity
        method: send PATH_REPLACE by raw RPC because the current SDK drops its path member
        expected: exactly the selected position changes and the whole model survives reload
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        clips = [
            {
                "anchor": j,
                "score": None if j % 2 else j,
                "label": None,
                "emb": None if j % 2 else normalized_vector(j),
                "tags": None if j % 2 else [j, None],
            }
            for j in range(4)
        ]
        rows = [make_nullable_row(schema, n, clips) for n in range(6)]
        model = self.insert_rows_with_model(client, name, rows)
        element = {
            "anchor": 9,
            "score": None if null_value else 7,
            "label": None,
            "emb": None if null_value else normalized_vector(90),
            "tags": None if null_value else [None, 7],
        }
        handler = client._get_connection()
        request = handler._prepare_row_upsert_request(name, [{"id": 2, "clips": [element]}], partial_update=True)
        # The ordinary SDK row encoder only sets VectorArray.dim. Raw PATH_REPLACE
        # validates both dimensions before the normal insert normalization runs.
        for field in wire_struct(request).fields:
            if field.HasField("vectors") and field.vectors.HasField("vector_array"):
                field.vectors.dim = field.vectors.vector_array.dim
        if target != "element":
            selected = copy.deepcopy(wire_child(request, target))
            del wire_struct(request).fields[:]
            wire_struct(request).fields.append(selected)
        path = "[1]" if target == "element" else f"[1][{target}]"
        request.field_ops.add(field_name="clips", op=3, path=path)
        check_status(handler._stub.Upsert(request, timeout=30).status)
        if target == "element":
            model[2]["clips"][1] = element
        else:
            model[2]["clips"][1][target] = element[target]
        self.assert_rows(client, name, model)
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)


class TestMilvusClientStructArrayPartialUpdateRejection(StructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("op", [1, 2], ids=["append", "remove"])
    @pytest.mark.parametrize("target", ["clips", "clips[score]", "clips[tags]"])
    def test_unsupported_struct_array_ops_are_non_mutating(self, op, target):
        """
        target: reject ARRAY_APPEND/REMOVE at the actual struct operation boundary
        method: send SDK-encoded existing/new PK batches through raw Upsert RPC
        expected: explicit struct-operation rejection, unchanged rows and valid replacement
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client,
            name,
            [make_nullable_row(schema, n, make_nullable_elements(schema.struct_fields[0], n)) for n in range(12)],
        )
        before = client.describe_collection(name)
        rows = [
            make_nullable_row(schema, n, make_nullable_elements(schema.struct_fields[0], n, 1), revision=1)
            for n in (2, 100, 3)
        ]
        handler = client._get_connection()
        request = handler._prepare_row_upsert_request(name, rows, partial_update=True)
        request.field_ops.add(field_name=target, op=op)
        status = handler._stub.Upsert(request, timeout=30).status
        assert status.code == 1100, status
        assert "not supported for struct field" in status.reason, status
        assert client.describe_collection(name) == before
        self.assert_rows(client, name, model)
        del request.field_ops[:]
        check_status(handler._stub.Upsert(request, timeout=30).status)
        model.update({row["id"]: row for row in rows})
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("position", [0, 1, 2])
    def test_invalid_partial_batch_preserves_existing_and_new_rows(self, position):
        """
        target: reject malformed partial updates atomically at Proxy
        method: corrupt required child payload in a batch containing new and existing PKs
        expected: no acknowledged sibling change; repaired request persists all rows
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client,
            name,
            [make_nullable_row(schema, n, make_nullable_elements(schema.struct_fields[0], n)) for n in range(12)],
        )
        rows = [
            make_nullable_row(
                schema, n, [{"anchor": n, "score": None, "label": None, "emb": None, "tags": [None]}], revision=1
            )
            for n in (2, 100, 3)
        ]
        handler = client._get_connection()
        good = handler._prepare_row_upsert_request(name, rows, partial_update=True)
        bad = copy.deepcopy(good)
        struct = next(f for f in bad.fields_data if f.field_name == "clips").struct_arrays
        anchor = next(f for f in struct.fields if f.field_name in ("anchor", "clips[anchor]"))
        del anchor.scalars.array_data.data[position].long_data.data[:]
        status = handler._stub.Upsert(bad, timeout=30).status
        assert status.code == 1100, status
        self.assert_rows(client, name, model)
        check_status(handler._stub.Upsert(good, timeout=30).status)
        model.update({row["id"]: row for row in rows})
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)


class TestMilvusClientStructArrayStorageV3Flush(StructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("entrance", ["create", "add_struct", "add_child"])
    @pytest.mark.parametrize("kind", ["scalar", "vector", "nested"])
    def test_growing_source_flush_requires_supported_storage_version(self, request, entrance, kind):
        """
        target: reject native-list schema changes only under V3 growing-source flush
        method: independently configured deployment, three DDL entrances and three layouts
        expected: precise configuration rejection without metadata changes, or durable NULL backfill
        """
        mode = request.config.getoption("--struct-storage-mode")
        if mode is None:
            pytest.skip("Requires explicit --struct-storage-mode and a verified isolated deployment")
        client = self._client()
        dtype, options = {
            "scalar": (DataType.INT64, {"nullable": True}),
            "vector": (DataType.FLOAT_VECTOR, {"nullable": True, "dim": 8}),
            "nested": (
                DataType.ARRAY,
                {"nullable": True, "element_type": DataType.INT64, "element_nullable": True, "max_capacity": 4},
            ),
        }[kind]
        children = client.create_struct_field_schema()
        children.add_field("value", dtype, **options)
        nullable_schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        nullable_schema.add_field("id", DataType.INT64, is_primary=True)
        nullable_schema.add_field("normal_vector", DataType.FLOAT_VECTOR, dim=8)
        nullable_schema.add_field(
            "extra", DataType.ARRAY, element_type=DataType.STRUCT, struct_schema=children, nullable=True, max_capacity=8
        )
        if entrance == "create":
            name = cf.gen_unique_str("struct_storage_guard")
            try:
                raw = Prepare.create_collection_request(name, nullable_schema)
                status = client._get_connection()._stub.CreateCollection(raw, timeout=30)
                if mode == "v3-gsf":
                    assert status.code == 1100 and "growing-source flush is enabled" in status.reason, status
                    assert not client.has_collection(name)
                else:
                    check_status(status)
                    assert client.has_collection(name)
            finally:
                client.drop_collection(name)
            # A schema without nullable elements must still be admitted with the guard enabled.
            control, schema = self.create_struct_collection(client, {"anchor": child_schema(DataType.INT64, False)})
            model = self.insert_rows_with_model(
                client, control, [make_nullable_row(schema, n, [{"anchor": n}]) for n in range(6)]
            )
            self.reload_collection(client, control)
            self.assert_rows(client, control, model)
            return

        name, schema = self.create_struct_collection(client, {"anchor": child_schema(DataType.INT64, False)})
        model = self.insert_rows_with_model(
            client,
            name,
            [make_nullable_row(schema, n, None if n == 0 else [] if n == 1 else [{"anchor": n}]) for n in range(6)],
        )
        before = client.describe_collection(name)
        if entrance == "add_struct":
            wire = schema_pb2.CollectionSchema.FromString(
                Prepare.create_collection_request("unused", nullable_schema).schema
            )
            raw = milvus_pb2.AddCollectionStructFieldRequest(
                collection_name=name, struct_array_field_schema=wire.struct_array_fields[0]
            )
            status = client._get_connection()._stub.AddCollectionStructField(raw, timeout=30)
            if mode == "v3-gsf":
                assert status.code == 1100 and "growing-source flush is enabled" in status.reason, status
            else:
                check_status(status)
                for row in model.values():
                    row["extra"] = None
        else:
            result, succeeded = self.add_collection_field(
                client, name, "clips[value]", dtype, **options, check_task=CheckTasks.check_nothing
            )
            if mode == "v3-gsf":
                assert not succeeded and getattr(result, "code", None) == 1100, result
                assert "growing-source flush is enabled" in str(result), result
            else:
                assert succeeded, result
                for row in model.values():
                    for element in row["clips"] or []:
                        element["value"] = None
        if mode == "v3-gsf":
            assert client.describe_collection(name) == before
        elif kind == "vector":
            indexes = client.prepare_index_params()
            indexes.add_index(
                "extra[value]" if entrance == "add_struct" else "clips[value]", index_type="FLAT", metric_type="L2"
            )
            self.create_index(client, name, indexes)
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)


class TestMilvusClientStructArrayProtobufValidity(MetadataStructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("location", ["legacy", "current", "dual"])
    @pytest.mark.parametrize("profile", ["all_null", "mixed"])
    def test_struct_array_wire_validity_locations_are_equivalent(self, location, profile):
        """
        target: accept legacy, field-specific and matching dual row masks recursively
        method: prepare protobuf input, explicitly rewrite masks, submit directly to Proxy and reload
        expected: identical complete rows and three-valued predicates for all supported encodings
        """
        client = self._client()
        name, schema = self.create_metadata_collection(client)
        rows = [make_metadata_row(schema, i) for i in range(21)]
        if profile == "all_null":
            for row in rows:
                row["clips"] = None
                row["note"] = None
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        handler = client._get_connection()
        template = handler._prepare_row_insert_request(name, [make_metadata_row(schema, 4)])
        request = encode_validity_masks(handler._prepare_row_insert_request(name, rows), location, template)
        response = handler._stub.Insert(request, timeout=30)
        assert response.status.code == 0, response.status
        assert response.insert_cnt == len(rows)
        log.info(f"WIRE_ACCEPT location={location} profile={profile} rows={response.insert_cnt}")
        self.assert_metadata_rows(client, name, model)
        self.reload_collection(client, name)
        self.assert_metadata_rows(client, name, model)
        self.assert_required_predicates(client, name, model)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("target", ["note", "clips[value]", "clips[emb]"])
    @pytest.mark.parametrize("malformation", ["conflict", "short", "long"])
    def test_struct_array_wire_invalid_mask_has_no_side_effects(self, target, malformation):
        """
        target: reject conflicting or misaligned masks at the actual Proxy boundary
        method: corrupt a direct protobuf batch, compare full retained data, retry original payload
        expected: parameter rejection, no valid sibling writes, then complete successful recovery
        """
        client = self._client()
        name, schema = self.create_metadata_collection(client)
        existing = [make_metadata_row(schema, i) for i in range(14)]
        model = {row["id"]: copy.deepcopy(row) for row in existing}
        self.insert(client, name, existing)
        self.reload_collection(client, name)
        rows = [make_metadata_row(schema, 100 + i) for i in range(7)]
        handler = client._get_connection()
        good = encode_validity_masks(handler._prepare_row_insert_request(name, rows), "dual")
        bad = copy.deepcopy(good)
        field = dict(iter_wire_fields(bad))[target]
        mask = list(field.valid_data)
        assert len(mask) == len(rows)
        if malformation == "conflict":
            other = mask[:]
            other[len(other) // 2] = not other[len(other) // 2]
            set_validity_masks(field, mask, other)
        else:
            malformed = mask[:-1] if malformation == "short" else mask + [True]
            set_validity_masks(field, malformed, malformed)
        response = handler._stub.Insert(bad, timeout=30)
        log.info(f"WIRE_REJECT target={target} malformed={malformation} status={response.status}")
        assert response.status.code == 1100, response.status
        assert response.status.reason
        if malformation == "conflict":
            assert "different legacy and field-specific valid_data" in response.status.reason
        self.assert_metadata_rows(client, name, model)
        response = handler._stub.Insert(good, timeout=30)
        assert response.status.code == 0, response.status
        assert response.insert_cnt == len(rows)
        model.update({row["id"]: row for row in rows})
        self.reload_collection(client, name)
        self.assert_metadata_rows(client, name, model)


class TestMilvusClientStructArrayNestedValidity(StructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("level", ["scalar", "vector", "inner", "leaf"])
    @pytest.mark.parametrize("corruption", ["short", "long", "payload_count"])
    def test_invalid_inner_validity_reaches_proxy_and_recovers(self, level, corruption):
        """
        target: enforce logical validity and compact payload lengths at every inner level
        method: corrupt a real SDK-generated RPC, retain a valid control, compare full rows
        expected: Proxy parameter rejection without side effects and valid retry succeeds
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client,
            name,
            [make_nullable_row(schema, n, make_nullable_elements(schema.struct_fields[0], n)) for n in range(20)],
        )
        clips = [
            {"anchor": 0, "score": 1, "label": "one", "emb": normalized_vector(4), "tags": [1, None, 3]},
            {"anchor": 1, "score": None, "label": None, "emb": None, "tags": None},
            {"anchor": 2, "score": 3, "label": "three", "emb": normalized_vector(6), "tags": []},
        ]
        row = make_nullable_row(schema, 100, clips)
        handler = client._get_connection()
        good = handler._prepare_row_insert_request(name, [row])
        bad = copy.deepcopy(good)
        if level == "scalar":
            mask = wire_child(bad, "score").scalars.array_data.data[0].valid_data
        elif level == "vector":
            mask = wire_child(bad, "emb").vectors.vector_array.data[0].valid_data
        elif level == "inner":
            mask = wire_child(bad, "tags").scalars.array_data.data[0].valid_data
        else:
            mask = wire_child(bad, "tags").scalars.array_data.data[0].array_data.data[0].valid_data
        assert list(mask) == [True, False, True], (level, list(mask))
        if corruption == "short":
            del mask[-1]
        elif corruption == "long":
            mask.append(True)
        else:
            mask[1] = True
        response = handler._stub.Insert(bad, timeout=30)
        assert response.status.code == 1100, response.status
        self.assert_rows(client, name, model)
        check_status(handler._stub.Insert(good, timeout=30).status)
        model[row["id"]] = row
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)
