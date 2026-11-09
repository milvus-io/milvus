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

"""Struct Array concurrent mutations E2E coverage."""

import copy
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import allure
import pytest
from base.struct_array import MetadataStructArrayTestBase, StructArrayTestBase
from common import common_func as cf
from common.common_type import CaseLabel
from common.struct_array_concurrent_workload import MixedHistory, WorkloadConfig
from common.struct_array_data import (
    assert_values_equal,
    child_schema,
    make_metadata_row,
    make_nullable_elements,
    make_nullable_row,
    make_primary_key,
    make_required_elements,
    nullable_children,
)
from pymilvus import DataType
from pymilvus.exceptions import MilvusException
from utils.util_log import test_log as log

SUBFIELD_DROP_ISSUE = "https://github.com/milvus-io/milvus/issues/55234"

SUBFIELD_DROP_MESSAGE = 'sub-field "added" does not exist in struct field "clips": invalid parameter'


class ConcurrentSubfieldDropRejection(AssertionError):
    """Insert or full upsert rejected a cached child during schema removal."""


def is_subfield_drop_rejection(code, message):
    return code == 1100 and message == SUBFIELD_DROP_MESSAGE


def is_verified_subfield_drop_failure(error, events, errors):
    """Accept only verified write rejections from the removed-child schema race."""
    if not isinstance(error, AssertionError) or not errors or str(error) != str(errors):
        return False
    if not all(e["epoch"] == 3 and is_subfield_drop_rejection(e.get("code"), e.get("message")) for e in errors):
        return False
    failed = [e for e in events if e["event"] == "end" and e["outcome"] not in ("ack", "rejected_schema")]
    if len(failed) != len(errors) or not all(
        e["epoch"] == 3
        and e["op"] in ("insert", "full_upsert")
        and e.get("write")
        and is_subfield_drop_rejection(e.get("error_code"), e.get("error_message"))
        for e in failed
    ):
        return False
    reconciled = [e for e in events if e["event"] == "uncertain_reconciliation"]
    if len(reconciled) != len(failed) or {e["entry"]["pk"] for e in reconciled} != {e["pk"] for e in failed}:
        return False
    if not all(e["matches"] == ["before"] for e in reconciled):
        return False
    if any(e["event"] in ("read_mismatch", "final_verification_failure") for e in events):
        return False
    summaries = [e for e in events if e["event"] == "summary"]
    if (
        len(summaries) != 1
        or not summaries[0]["read_write_overlap"]
        or not all(summaries[0]["ddl_write_overlap"].values())
        or not all(summaries[0]["ddl_read_overlap"].values())
    ):
        return False
    return any(e["event"] == "verified" and e["phase"] == "reloaded" for e in events)


class TestMilvusClientStructArrayConcurrentMutations(MetadataStructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("pk_type", ["int", "str"])
    def test_struct_array_concurrent_writers_preserve_row_atomicity(self, pk_type):
        """
        target: avoid torn struct/value pairs during concurrent independent-key writes
        method: four writers update disjoint keys while a reader validates whole-row invariants
        expected: every observed row is a legal complete revision; final model survives reload
        """
        client = self._client()
        name, schema = self.create_metadata_collection(client, pk_type=pk_type)
        revisions = {
            (number, revision): make_metadata_row(schema, number, revision)
            for number in range(40)
            for revision in range(9)
        }
        rows = [revisions[number, 0] for number in range(40)]
        self.insert(client, name, rows)
        self.reload_collection(client, name)
        barrier = threading.Barrier(5)
        done = threading.Event()

        def writer(worker):
            barrier.wait(timeout=30)
            result = {}
            for revision in range(1, 9):
                batch = [copy.deepcopy(revisions[i, revision]) for i in range(worker * 10, worker * 10 + 10)]
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
                    assert_values_equal(row, revisions[number, row["revision"]], exact_fields=False)
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
        self.assert_metadata_rows(client, name, model, pk_type)
        self.reload_collection(client, name)
        self.assert_metadata_rows(client, name, model, pk_type)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("pk_type", ["int", "str"])
    def test_struct_array_concurrent_partial_delete_reinsert(self, pk_type):
        """
        target: preserve retained fields and element coordinates during concurrent mixed mutations
        method: disjoint-key writers alternate full/partial updates, deletes and reinserts under a reader
        expected: every observed row is one complete legal revision and all final rows survive reload
        """
        client = self._client()
        name, schema = self.create_metadata_collection(client, pk_type=pk_type)
        states = {}
        for number in range(40):
            pk = make_primary_key(number, pk_type)
            states[pk] = {0: make_metadata_row(schema, number, 0), 1: make_metadata_row(schema, number, 1)}
            states[pk][2] = {
                **copy.deepcopy(states[pk][1]),
                "revision": 2,
                "clips": make_required_elements(schema.struct_fields[0], number, 2),
            }
            states[pk][4] = make_metadata_row(schema, number, 4)
            states[pk][5] = {
                **copy.deepcopy(states[pk][4]),
                "revision": 5,
                "normal_vector": make_metadata_row(schema, number, 5)["normal_vector"],
            }
            states[pk][6] = make_metadata_row(schema, number, 6)
        self.insert(client, name, [revisions[0] for revisions in states.values()])
        self.reload_collection(client, name)
        barrier = threading.Barrier(5)
        done = threading.Event()

        def writer(worker):
            keys = [make_primary_key(i, pk_type) for i in range(worker * 10, worker * 10 + 10)]
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
                    assert_values_equal(row, states[row["id"]][row["revision"]], exact_fields=False)
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
        self.assert_metadata_rows(client, name, final, pk_type)
        self.reload_collection(client, name)
        self.assert_metadata_rows(client, name, final, pk_type)
        self.assert_element_search(client, name, final, "L2", "FLAT")


class TestMilvusClientStructArrayConcurrentSchemaUpdates(StructArrayTestBase):
    skip_global_role_cleanup = True

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("kind", ["scalar", "nested", "vector"])
    @pytest.mark.parametrize("surface", ["query", "search"])
    def test_add_drop_readd_child_overlaps_queries_and_preserves_schema_snapshots(self, kind, surface):
        """
        target: exercise queries naming a child while it is added, dropped and recreated
        method: continuously read stable and changing projections; retain actual RPC intervals and row snapshots
        expected: each DDL overlaps both readers, valid schema snapshots, explicit missing-field errors, fresh IDs
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        rows = [make_nullable_row(schema, n, make_nullable_elements(schema.struct_fields[0], n)) for n in range(128)]
        model = self.insert_rows_with_model(client, name, rows)
        dtype, options = {
            "scalar": (DataType.INT64, {}),
            "nested": (DataType.ARRAY, {"element_type": DataType.INT64, "max_capacity": 4, "element_nullable": True}),
            "vector": (DataType.FLOAT_VECTOR, {"dim": 16}),
        }[kind]
        field = cf.gen_scalar_field(dtype, name="changing", **options)
        query_vector = rows[0]["normal_vector"]
        journal = []
        first_id = None
        for operation in ("add", "drop", "readd"):
            before = copy.deepcopy(model)
            after = copy.deepcopy(model)
            for row in after.values():
                for element in row["clips"] or []:
                    if operation == "drop":
                        del element["changing"]
                    else:
                        element["changing"] = None
            started = {target: threading.Event() for target in (False, True)}
            finished = threading.Event()
            observations = []
            lock = threading.Lock()
            null_test = "IS NOT NULL" if operation == "drop" else "IS NULL"
            target_expression = f"MATCH_ANY(clips, $[changing] {null_test})"

            def read(target):
                deadline = time.monotonic() + 90
                count = 0
                while not finished.is_set() or count < 3:
                    assert time.monotonic() < deadline, "DDL/query concurrency deadline"
                    record = {"target": target, "start": time.monotonic_ns()}
                    started[target].set()
                    try:
                        expression = target_expression if target else "id >= 0"
                        if surface == "query":
                            result = client.query(
                                name,
                                filter=expression,
                                output_fields=["id", "clips"],
                                limit=len(model) + 1,
                                consistency_level="Strong",
                                timeout=30,
                            )
                        else:
                            hits = client.search(
                                name,
                                [query_vector],
                                anns_field="normal_vector",
                                filter=expression,
                                output_fields=["clips"],
                                limit=len(model) + 1,
                                search_params={"metric_type": "L2"},
                                consistency_level="Strong",
                                timeout=30,
                            )
                            result = [{"id": hit["id"], "clips": hit["entity"]["clips"]} for hit in hits[0]]
                        record["rows"] = sorted(result, key=lambda row: row["id"])
                    except MilvusException as error:
                        record.update(code=error.code, message=error.message)
                    finally:
                        record["end"] = time.monotonic_ns()
                        with lock:
                            observations.append(record)
                    count += 1

            def evolve():
                assert all(event.wait(timeout=30) for event in started.values())
                begin = time.monotonic_ns()
                try:
                    if operation == "drop":
                        client.drop_collection_field(name, field_name="clips[changing]", timeout=60)
                    else:
                        client.add_collection_field(
                            name, "clips[changing]", dtype, nullable=True, timeout=60, **options
                        )
                    return begin, time.monotonic_ns()
                finally:
                    finished.set()

            with ThreadPoolExecutor(max_workers=3) as pool:
                readers = [pool.submit(read, target) for target in (False, True)]
                ddl = pool.submit(evolve)
                ddl_start, ddl_end = ddl.result(timeout=90)
                for future in readers:
                    future.result(timeout=100)
            overlaps = {
                target: sum(
                    r["target"] == target and r["start"] < ddl_end and ddl_start < r["end"] for r in observations
                )
                for target in (False, True)
            }
            journal.append(
                {
                    "operation": operation,
                    "surface": surface,
                    "ddl_start": ddl_start,
                    "ddl_end": ddl_end,
                    "overlaps": overlaps,
                    "reads": observations,
                }
            )
            allure.attach(
                json.dumps(journal),
                name=f"{operation} {surface} intervals",
                attachment_type=allure.attachment_type.JSON,
            )
            log.info(f"STRUCT_DDL_READ operation={operation} surface={surface} overlaps={overlaps}")
            assert all(overlaps.values()), journal[-1]
            for observation in observations:
                states = (
                    [before]
                    if observation["end"] < ddl_start
                    else [after]
                    if observation["start"] > ddl_end
                    else [before, after]
                )
                if "code" in observation:
                    assert observation["target"], observation
                    assert any(
                        not any("changing" in e for r in state.values() for e in r["clips"] or []) for state in states
                    ), observation
                    assert observation["code"] == 1100 and "changing" in observation["message"], observation
                    assert any(
                        word in observation["message"].lower() for word in ("not found", "not exist", "unknown")
                    ), observation
                    continue
                candidates = []
                for state in states:
                    if observation["target"]:
                        if not any("changing" in e for r in state.values() for e in r["clips"] or []):
                            continue
                        state = {
                            pk: row
                            for pk, row in state.items()
                            if any(
                                (e["changing"] is not None) if operation == "drop" else (e["changing"] is None)
                                for e in row["clips"] or []
                            )
                        }
                    candidates.append([{"id": pk, "clips": row["clips"]} for pk, row in sorted(state.items())])
                matched = False
                for candidate in candidates:
                    try:
                        assert_values_equal(observation["rows"], candidate)
                        matched = True
                    except AssertionError:
                        pass
                assert matched, observation
            model = after
            self.assert_rows(client, name, model)
            if operation == "add":
                first_id = self.get_child_id(client, name, "changing")
                # Seed non-null old-generation values so a reused field ID cannot hide stale data.
                for row in model.values():
                    for element in row["clips"] or []:
                        element["changing"] = cf.gen_data_by_collection_field(field)
                self.upsert(client, name, list(model.values()))
            elif operation == "readd":
                assert self.get_child_id(client, name, "changing") > first_id
            if kind == "vector" and operation != "drop":
                indexes, _ = self.prepare_index_params(client)
                indexes.add_index("clips[changing]", index_type="FLAT", metric_type="L2")
                self.create_index(client, name, indexes)
            self.reload_collection(client, name)
            self.assert_rows(client, name, model)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("kind", ["scalar", "nested", "vector"])
    def test_concurrent_writes_and_add_child(self, kind):
        """
        target: preserve acknowledged writes while adding a nullable child to mixed segments
        method: three finite writers and one DDL actor start together, then compare an independent model
        expected: every write survives, new child backfills NULL, indexed vector reload remains valid
        """
        client = self._client()
        name, schema = self.create_struct_collection(client, nullable_children())
        model = self.insert_rows_with_model(
            client,
            name,
            [make_nullable_row(schema, n, make_nullable_elements(schema.struct_fields[0], n)) for n in range(12)],
        )
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
                    make_nullable_row(
                        schema,
                        100 + worker * 100 + j,
                        make_nullable_elements(schema.struct_fields[0], j + worker, revision),
                        revision=revision,
                    )
                    for j in range(8)
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
        self.assert_rows(client, name, model)
        if kind == "vector":
            index = client.prepare_index_params()
            index.add_index("clips[added]", index_type="FLAT", metric_type="L2")
            client.create_index(name, index)
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)


class TestMilvusClientStructArrayConcurrentSubfieldDrop(StructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(strict=True, raises=ConcurrentSubfieldDropRejection, reason=SUBFIELD_DROP_ISSUE)
    def test_concurrent_drop_preserves_inserts_that_omit_removed_child(self):
        """
        target: keep inserts valid when they omit a concurrently removed nullable child
        method: warm SDK schema, race four disjoint-key writers against child removal
        expected: all inserts succeed and exact retained fields survive the schema change
        """
        client = self._client()
        name, schema = self.create_struct_collection(
            client, {"anchor": child_schema(DataType.INT64, False), "added": child_schema(DataType.INT64)}
        )
        seed = make_nullable_row(schema, 0, [{"anchor": 0}])
        self.insert(client, name, [seed])
        barrier = threading.Barrier(5)

        def writer(worker):
            rows, errors = [], []
            barrier.wait(timeout=30)
            for step in range(12):
                row = make_nullable_row(schema, 1 + worker * 100 + step, [{"anchor": step}])
                try:
                    result = client.insert(name, [row], timeout=30)
                    assert result["insert_count"] == 1, result
                    rows.append(row)
                except MilvusException as exc:
                    errors.append({"row": row, "code": exc.code, "message": exc.message})
            return rows, errors

        def drop():
            barrier.wait(timeout=30)
            client.drop_collection_field(name, field_name="clips[added]", timeout=60)

        with ThreadPoolExecutor(max_workers=5) as pool:
            writers = [pool.submit(writer, worker) for worker in range(4)]
            ddl = pool.submit(drop)
            results = [future.result(timeout=180) for future in writers]
            ddl.result(timeout=90)
        acknowledged = [seed] + [row for rows, _ in results for row in rows]
        errors = [error for _, failures in results for error in failures]
        model = {row["id"]: copy.deepcopy(row) for row in acknowledged}
        self.assert_rows(client, name, model)
        self.reload_collection(client, name)
        self.assert_rows(client, name, model)
        allure.attach(
            str(errors), name="insert rejections during child drop", attachment_type=allure.attachment_type.TEXT
        )
        if errors:
            assert all(is_subfield_drop_rejection(e["code"], e["message"]) for e in errors), errors
            raise ConcurrentSubfieldDropRejection(f"{SUBFIELD_DROP_ISSUE}: {errors}")


class TestMilvusClientStructArrayConcurrentReadWrite(StructArrayTestBase):
    skip_global_role_cleanup = True

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(strict=True, raises=ConcurrentSubfieldDropRejection, reason=SUBFIELD_DROP_ISSUE)
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
        except AssertionError as error:
            if is_verified_subfield_drop_failure(error, history.events, history.errors):
                raise ConcurrentSubfieldDropRejection(f"{SUBFIELD_DROP_ISSUE}: {history.errors}") from error
            raise
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
