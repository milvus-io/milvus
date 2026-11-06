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

"""Struct array concurrent mutations regression cases."""

import copy
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from common.common_type import CaseLabel
from common.struct_array_row_model import (
    StructArrayRowModelBase,
    _assert_value,
    _pk,
    _profile,
    _row,
)
from utils.util_log import test_log as log


class TestMilvusClientStructArrayConcurrentMutations(StructArrayRowModelBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("pk_type", ["int", "str"])
    def test_struct_array_concurrent_writers_preserve_row_atomicity(self, pk_type):
        """
        target: avoid torn struct/value pairs during concurrent independent-key writes
        method: four writers update disjoint keys while a reader validates whole-row invariants
        expected: every observed row is a legal complete revision; final model survives reload
        """
        client = self._client()
        name = self._create_contract_collection(client, pk_type=pk_type)
        rows = [_row(i, pk_type=pk_type) for i in range(40)]
        self.insert(client, name, rows)
        self._reload(client, name)
        barrier = threading.Barrier(5)
        done = threading.Event()

        def writer(worker):
            barrier.wait(timeout=30)
            result = {}
            for revision in range(1, 9):
                batch = [_row(i, revision, pk_type) for i in range(worker * 10, worker * 10 + 10)]
                self.upsert(client, name, batch)
                result = {row["id"]: row for row in batch}
                log.info(f"STRUCT_CONCURRENT worker={worker} revision={revision} ids={list(result)}")
            return result

        def reader():
            barrier.wait(timeout=30)
            reads = 0
            deadline = time.monotonic() + 120
            while not done.is_set() or reads < 3:
                assert time.monotonic() < deadline, "concurrent reader deadline"
                actual, _ = self.query(
                    client,
                    name,
                    filter="id >= 0" if pk_type == "int" else 'id != ""',
                    output_fields=["*"],
                    limit=100,
                    consistency_level="Strong",
                )
                assert len(actual) == 40
                for row in actual:
                    number = row["id"] if pk_type == "int" else int(row["id"].rsplit("_", 1)[1])
                    _assert_value(row, _row(number, row["revision"], pk_type))
                reads += 1
            return reads

        with ThreadPoolExecutor(max_workers=5) as pool:
            reading = pool.submit(reader)
            writers = [pool.submit(writer, worker) for worker in range(4)]
            try:
                model = {}
                for future in writers:
                    model.update(future.result(timeout=120))
            finally:
                done.set()
            assert reading.result(timeout=130) >= 3
        self._assert_model(client, name, model, pk_type)
        self._reload(client, name)
        self._assert_model(client, name, model, pk_type)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("pk_type", ["int", "str"])
    def test_struct_array_concurrent_partial_delete_reinsert(self, pk_type):
        """
        target: preserve retained fields and element coordinates during concurrent mixed mutations
        method: disjoint-key writers alternate full/partial updates, deletes and reinserts under a reader
        expected: every observed row is one complete legal revision and all final rows survive reload
        """
        client = self._client()
        name = self._create_contract_collection(client, pk_type=pk_type)
        states = {}
        for number in range(40):
            pk = _pk(number, pk_type)
            states[pk] = {0: _row(number, 0, pk_type), 1: _row(number, 1, pk_type)}
            states[pk][2] = {**copy.deepcopy(states[pk][1]), "revision": 2, "clips": _profile(number, 2)}
            states[pk][4] = _row(number, 4, pk_type)
            states[pk][5] = {
                **copy.deepcopy(states[pk][4]),
                "revision": 5,
                "normal_vector": _row(number, 5, pk_type)["normal_vector"],
            }
            states[pk][6] = _row(number, 6, pk_type)
        self.insert(client, name, [revisions[0] for revisions in states.values()])
        self._reload(client, name)
        barrier = threading.Barrier(5)
        done = threading.Event()

        def writer(worker):
            keys = [_pk(i, pk_type) for i in range(worker * 10, worker * 10 + 10)]
            barrier.wait(timeout=30)
            for revision in range(1, 7):
                if revision == 3:
                    self.delete(client, name, ids=keys)
                    remaining, _ = self.query(
                        client,
                        name,
                        filter=f"id in {json.dumps(keys)}",
                        output_fields=["id"],
                        consistency_level="Strong",
                    )
                    assert not remaining, "Deleted worker-owned keys must be absent before reinsertion"
                elif revision == 4:
                    self.insert(client, name, [states[pk][revision] for pk in keys])
                elif revision in (2, 5):
                    field = "clips" if revision == 2 else "normal_vector"
                    self.upsert(
                        client,
                        name,
                        [{"id": pk, "revision": revision, field: states[pk][revision][field]} for pk in keys],
                        partial_update=True,
                    )
                else:
                    self.upsert(client, name, [states[pk][revision] for pk in keys])
                log.info(f"STRUCT_CONCURRENT_MIX worker={worker} completed_revision={revision}")

        def reader():
            barrier.wait(timeout=30)
            reads = 0
            deadline = time.monotonic() + 120
            while not done.is_set() or reads < 3:
                assert time.monotonic() < deadline
                rows, _ = self.query(
                    client,
                    name,
                    filter="id >= 0" if pk_type == "int" else 'id != ""',
                    output_fields=["*"],
                    limit=100,
                    consistency_level="Strong",
                )
                keys = [row["id"] for row in rows]
                assert len(keys) == len(set(keys)) and set(keys).issubset(states)
                for row in rows:
                    assert row["revision"] in states[row["id"]]
                    _assert_value(row, states[row["id"]][row["revision"]])
                reads += 1
            return reads

        with ThreadPoolExecutor(max_workers=5) as pool:
            reading = pool.submit(reader)
            writing = [pool.submit(writer, worker) for worker in range(4)]
            try:
                for future in writing:
                    future.result(timeout=120)
            finally:
                done.set()
            assert reading.result(timeout=130) >= 3
        final = {pk: revisions[6] for pk, revisions in states.items()}
        self._assert_model(client, name, final, pk_type)
        self._reload(client, name)
        self._assert_model(client, name, final, pk_type)
        self._assert_element_search(client, name, final, "L2", "FLAT")
