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

"""Struct array dml regression cases."""

import copy
import json
import random
import time

import pytest
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.struct_array_row_model import (
    CAPACITY,
    DIM,
    StructArrayRowModelBase,
    _assert_value,
    _element,
    _pk,
    _profile,
    _row,
)
from utils.util_log import test_log as log


class TestMilvusClientStructArrayDml(StructArrayRowModelBase):
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
        name = self._create_contract_collection(client, pk_type=pk_type)
        rows = [_row(i, pk_type=pk_type) for i in range(24)]
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
                ids = deleted if operation == "reinsert" else existing + [_pk(1000 + step, pk_type)]
                if operation == "insert":
                    ids = [_pk(2000 + step, pk_type)]
                batch = []
                for pk in ids:
                    number = pk if pk_type == "int" else int(pk.rsplit("_", 1)[1])
                    row = _row(number, step + 1, pk_type)
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
                        self._assert_model(client, name, before, pk_type)
                        model = before
                        failure = {"request": history[-1], "failure": str(response)}
                        write_failures.append(failure)
            log.info(f"STRUCT_HISTORY seed={seed} pk={pk_type} initial={initial}: {json.dumps(history[-1])}")
            try:
                self._assert_model(client, name, model, pk_type)
                if step % 8 == 7:
                    self._reload(client, name)
                    self._assert_model(client, name, model, pk_type)
                    self._assert_predicates(client, name, model)
            except AssertionError as error:
                raise AssertionError(f"seed={seed}; history={json.dumps(history)}") from error
        assert not write_failures, json.dumps({"seed": seed, "failed_writes": write_failures})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("capacity", [1, 2, 128, 4096])
    def test_struct_array_capacity_boundary_and_integer_extremes(self, capacity):
        """
        target: retain exact INT64 and UTF-8 values at array capacity boundaries
        method: write NULL, empty, one, capacity-1 and capacity arrays and reload twice
        expected: no truncation, integer precision loss, array reordering or NULL/empty collapse
        """
        client = self._client()
        name = self._create_contract_collection(client, capacity=capacity, vectors=False)
        rows = [_row(i, capacity=capacity, vectors=False) for i in range(5)]
        for row, length in zip(rows, [None, 0, 1, max(0, capacity - 1), capacity]):
            row["clips"] = None if length is None else [_element(row["id"], j, vectors=False) for j in range(length)]
            for j, element in enumerate(row["clips"] or []):
                element["value"] = [-(2**63), 2**63 - 1, 0][j % 3]
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        self.insert(client, name, rows)
        self._assert_model(client, name, model)
        for _ in range(2):
            self._reload(client, name)
            self._assert_model(client, name, model)

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
        name = self._create_contract_collection(client)
        existing = [_row(i) for i in range(14)]
        model = {row["id"]: copy.deepcopy(row) for row in existing}
        self.insert(client, name, existing)
        self._reload(client, name)
        good = [_row(100 + i) for i in range(7)]
        for row in good:
            row["clips"] = [_element(row["id"], 0)]
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
        self._assert_model(client, name, model)
        self.insert(client, name, good)
        model.update({row["id"]: row for row in good})
        self._reload(client, name)
        self._assert_model(client, name, model)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("seed", [11, 37, 73])
    def test_struct_array_sibling_offsets_and_partial_update_are_independent(self, seed):
        """
        target: isolate offsets and row validity between two structs with identical child names
        method: use different NULL/length distributions and alternately replace one struct across reloads
        expected: omitted sibling fields remain bit-for-bit logically unchanged
        """
        client = self._client()
        name = self._create_contract_collection(client, second=True)
        rows = [_row(i) for i in range(48)]
        for row in rows:
            row["other"] = _profile(row["id"] + seed, capacity=3)
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        self.insert(client, name, rows)
        for revision in range(1, 7):
            field = "clips" if revision % 2 else "other"
            batch = []
            for pk in range(revision % 3, 48, 3):
                patch = {"id": pk, field: _profile(pk + seed, revision, capacity=revision)}
                batch.append(patch)
                model[pk].update(copy.deepcopy(patch))
            self.upsert(client, name, batch, partial_update=True)
            self._assert_model(client, name, model)
            self._reload(client, name)
            self._assert_model(client, name, model)
        self._assert_element_search(client, name, model, "L2", "FLAT", "clips")
        self._assert_element_search(client, name, model, "L2", "FLAT", "other")

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("pk_type", ["int", "str"])
    def test_struct_array_partition_iterator_after_delete_and_partial_update(self, pk_type):
        """
        target: isolate partition state and retain complete array projections in iterators
        method: delete and partially update one partition, then iterate both across reload
        expected: exact partition-specific PK maps with no duplicates or missing rows
        """
        client = self._client()
        name = self._create_contract_collection(client, pk_type=pk_type, second=True)
        models = {}
        for part, start in (("left_part", 0), ("right_part", 1000)):
            self.create_partition(client, name, part)
            rows = [_row(i, pk_type=pk_type) for i in range(start, start + 73)]
            for row in rows:
                number = row["id"] if pk_type == "int" else int(row["id"].rsplit("_", 1)[1])
                row["other"] = _profile(number + 3, capacity=2)
            self.insert(client, name, rows, partition_name=part)
            models[part] = {row["id"]: copy.deepcopy(row) for row in rows}
        self._reload(client, name)
        deleted = [_pk(i, pk_type) for i in range(0, 73, 5)]
        self.delete(client, name, ids=deleted, partition_name="left_part")
        for pk in deleted:
            del models["left_part"][pk]
        patches = []
        for number in range(1, 73, 5):
            patch = {"id": _pk(number, pk_type), "clips": _profile(number, 8), "revision": 8}
            patches.append(patch)
            models["left_part"][patch["id"]].update(copy.deepcopy(patch))
        self.upsert(client, name, patches, partial_update=True, partition_name="left_part")
        for phase in range(2):
            for part, model in models.items():
                self._assert_model(client, name, model, pk_type, [part])
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
                    _assert_value(seen[pk], expected)
            self._reload(client, name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("new_field_vectors", [False, True], ids=["scalar", "vector"])
    def test_struct_array_schema_evolution_mutation_and_compaction(self, new_field_vectors):
        """
        target: retain old-row backfill and independent offsets across schema changes and compaction
        method: add a struct to existing sealed rows, mix new/old-key partial writes, compact and reload
        expected: exact complete data and element search before and after a completed compaction
        """
        client = self._client()
        name = self._create_contract_collection(client)
        rows = [_row(i) for i in range(1200)]
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        for batch in (rows[:600], rows[600:]):
            self.insert(client, name, batch)
            self.flush(client, name)
        self.add_collection_struct_field(
            client,
            name,
            "added",
            self._struct_schema(client, vectors=new_field_vectors),
            CAPACITY,
        )
        for row in model.values():
            row["added"] = None
        if new_field_vectors:
            indexes, _ = self.prepare_index_params(client)
            indexes.add_index("added[emb]", index_type="FLAT", metric_type="L2")
            self.create_index(client, name, indexes)
        self.refresh_load(client, name)
        self._assert_model(client, name, model)
        patches = []
        for number in list(range(0, 1200, 3)) + list(range(1200, 1260)):
            source = _row(number, 4)
            patch = {key: source[key] for key in ("id", "revision", "bucket", "normal_vector")}
            patch["added"] = _profile(number + 2, 4, vectors=new_field_vectors)
            expected = copy.deepcopy(model.get(number, {**source, "note": None, "clips": None}))
            expected.update(copy.deepcopy(patch))
            model[number] = expected
            patches.append(patch)
        self.upsert(client, name, patches, partial_update=True)
        deleted = list(range(1, 1200, 7))
        self.delete(client, name, ids=deleted)
        for pk in deleted:
            del model[pk]
        self._reload(client, name)
        self._assert_model(client, name, model)
        self._assert_element_search(client, name, model, "L2", "FLAT")
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
        self._reload(client, name)
        self._assert_model(client, name, model)
        self._assert_predicates(client, name, model)
        self._assert_element_search(client, name, model, "L2", "FLAT")
        if new_field_vectors:
            self._assert_element_search(client, name, model, "L2", "FLAT", "added")
