import json
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass

import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from milvus_client import test_milvus_client_data_integrity as integrity
from pymilvus import BulkInsertState, DataType
from pymilvus.bulk_writer import BulkFileType, RemoteBulkWriter
from utils.etcd_config import MilvusEtcdConfigController

CONTINUOUS_EPOCHS = 4
CONTINUOUS_ROWS_PER_REQUEST = 100
CONTINUOUS_FIRST_INSERTS = 2200
CONTINUOUS_UPSERTS = 400
CONTINUOUS_DELETES = 200
CONTINUOUS_REINSERTS = 100
CONTINUOUS_DUPLICATE_INSERTS = 100
CONTINUOUS_MUTATIONS_PER_EPOCH = 3000
CONTINUOUS_IMPORT_ROWS_PER_EPOCH = 3000
CONTINUOUS_SEQUENCE_SPAN_PER_EPOCH = 9000
CONTINUOUS_EXPECTED_LIVE_ROWS_PER_EPOCH = 7200
CONTINUOUS_SEED_ROWS = 200
CONTINUOUS_REQUEST_INTERVAL = float(os.getenv("MILVUS_CONTINUOUS_INTEGRITY_REQUEST_INTERVAL", "2"))
CONTINUOUS_CASE_TIMEOUT = int(os.getenv("MILVUS_CONTINUOUS_INTEGRITY_TIMEOUT", "1200"))
CONTINUOUS_FINAL_CHECKPOINT_TIMEOUT = int(os.getenv("MILVUS_CONTINUOUS_INTEGRITY_FINAL_CHECKPOINT_TIMEOUT", "300"))
CONTINUOUS_IMPORT_VISIBILITY_TIMEOUT = int(os.getenv("MILVUS_CONTINUOUS_INTEGRITY_IMPORT_VISIBILITY_TIMEOUT", "120"))
CONTINUOUS_RUNTIME_CONFIG = {
    "dataNode.segment.syncPeriod": "10",
    "dataCoord.segment.maxSize": "64",
    "dataCoord.segment.maxLife": "60",
    "streaming.flush.l0.maxLifetime": "30s",
    "dataCoord.compaction.levelzero.forceTrigger.deltalogMinNum": "4",
}
CONTINUOUS_LANES = {"mutation_a", "mutation_b", "import"}


@dataclass(frozen=True)
class _ContinuousAction:
    sequence: int
    lane: str
    epoch: int
    operation: str
    primary_key: str
    expected: dict | None


class _ContinuousOracle:
    def __init__(self):
        self._lock = threading.RLock()
        self._actions = {}
        self._poison_reason = None

    def poison(self, reason):
        with self._lock:
            if self._poison_reason is None:
                self._poison_reason = str(reason)

    def commit(self, actions):
        actions = list(actions)
        assert actions, "oracle receipt cannot be empty"
        sequences = [action.sequence for action in actions]
        assert len(sequences) == len(set(sequences)), f"receipt contains duplicate sequences: {sequences}"
        with self._lock:
            if self._poison_reason is not None:
                raise AssertionError(f"collection is indeterminate: {self._poison_reason}")
            duplicates = set(sequences).intersection(self._actions)
            assert not duplicates, f"logical sequences committed twice: {sorted(duplicates)}"
            self._actions.update((action.sequence, action) for action in actions)

    @property
    def max_committed_sequence(self):
        with self._lock:
            return max(self._actions, default=0)

    @property
    def action_count(self):
        with self._lock:
            return len(self._actions)

    def snapshot(self, boundary):
        with self._lock:
            if self._poison_reason is not None:
                raise AssertionError(f"collection is indeterminate: {self._poison_reason}")
            actions = [self._actions[sequence] for sequence in sorted(self._actions) if sequence <= boundary]
        committed_sequences = {action.sequence for action in actions}
        missing_sequences = set(range(1, boundary + 1)) - committed_sequences
        assert not missing_sequences, (
            f"prefix boundary {boundary} contains uncommitted actions: "
            f"sample={sorted(missing_sequences)[:20]} count={len(missing_sequences)}"
        )
        expected_by_pk = {}
        deleted_primary_keys = set()
        for action in actions:
            if action.operation == "delete":
                expected_by_pk.pop(action.primary_key, None)
                deleted_primary_keys.add(action.primary_key)
            else:
                assert action.expected is not None
                expected_by_pk[action.primary_key] = action.expected
                deleted_primary_keys.discard(action.primary_key)
        return expected_by_pk, deleted_primary_keys


class _ContinuousEpochTracker:
    def __init__(self, checkpoint_queue):
        self._lock = threading.Lock()
        self._completed = {epoch: set() for epoch in range(CONTINUOUS_EPOCHS)}
        self._checkpoint_queue = checkpoint_queue

    def complete(self, lane, epoch):
        with self._lock:
            completed = self._completed[epoch]
            assert lane not in completed, f"lane {lane} completed epoch {epoch + 1} twice"
            completed.add(lane)
            if completed == CONTINUOUS_LANES and epoch < CONTINUOUS_EPOCHS - 1:
                self._checkpoint_queue.put(epoch)


class _ContinuousLifecycleObserver:
    def __init__(
        self,
        client,
        collection_name,
        oracle,
        ingress_done,
        stop_event,
        checkpoint_queue,
        config_committed_at,
    ):
        self.client = client
        self.collection_name = collection_name
        self.oracle = oracle
        self.ingress_done = ingress_done
        self.stop_event = stop_event
        self.checkpoint_queue = checkpoint_queue
        self.config_committed_at = config_committed_at
        self.segment_history = {}
        self.task_history = {}
        self.mix_closures = {}
        self.l0_closures = {}
        self.first_natural_flush_at = None
        self.error = None
        self._lock = threading.Lock()

    def capture(self):
        all_segments = integrity._snapshot_compaction_integrity_segments(
            self.client.list_segments(self.collection_name)
        )
        active_segments = {
            segment_id: segment
            for segment_id, segment in all_segments.items()
            if segment["state"] in integrity.COMPACTION_INTEGRITY_ACTIVE_STATES and segment["num_rows"] != 0
        }
        serving_segments = {
            segment_id: segment
            for segment_id, segment in integrity._snapshot_compaction_integrity_segments(
                self.client.list_loaded_segments(self.collection_name)
            ).items()
            if segment["num_rows"] != 0
        }
        tasks = integrity._snapshot_compaction_integrity_tasks(self.client.list_compaction_tasks(self.collection_name))
        checkpoint = {
            "all": all_segments,
            "active": active_segments,
            "serving": serving_segments,
            "tasks": tasks,
            "storage_versions": {segment["storage_version"] for segment in active_segments.values()},
        }
        self._assert_graph_sane(checkpoint)
        return checkpoint

    def _assert_graph_sane(self, checkpoint):
        with self._lock:
            combined = {**self.segment_history, **checkpoint["all"]}
        children = {}
        for target_id, target in combined.items():
            for source_id in target["compaction_from"]:
                assert source_id != target_id, f"segment {target_id} lists itself as a compaction source"
                children.setdefault(source_id, set()).add(target_id)
        for root_id in children:
            integrity._compaction_integrity_descendants(root_id, children)

    def _descendants(self, segment_id):
        children = {}
        for target_id, target in self.segment_history.items():
            for source_id in target["compaction_from"]:
                children.setdefault(source_id, set()).add(target_id)
        return integrity._compaction_integrity_descendants(segment_id, children)

    def _observe_closures(self, checkpoint):
        active_ids = set(checkpoint["active"])
        serving_ids = set(checkpoint["serving"])
        active_and_serving = active_ids.intersection(serving_ids)
        ingress_active = not self.ingress_done.is_set()
        for task_id, task in self.task_history.items():
            if task["state"] not in integrity.COMPACTION_INTEGRITY_SUCCESS_TASK_STATES or task["failure_reason"]:
                continue
            if task["type"] == "MixCompaction" and task_id not in self.mix_closures:
                sources_retired = bool(task["sources"]) and all(
                    self.segment_history.get(source_id, {}).get("state") == "Dropped" for source_id in task["sources"]
                )
                frontier = set(task["targets"])
                for target_id in task["targets"]:
                    frontier.update(self._descendants(target_id))
                adopted = {
                    segment_id
                    for segment_id in frontier.intersection(active_and_serving)
                    if checkpoint["active"][segment_id]["state"] == "Flushed"
                }
                if sources_retired and adopted:
                    self.mix_closures[task_id] = {
                        "task": task,
                        "adopted_segment_ids": sorted(adopted),
                        "ingress_active": ingress_active,
                        "committed_sequence": self.oracle.max_committed_sequence,
                        "committed_action_count": self.oracle.action_count,
                        "elapsed_since_config_seconds": round(time.time() - self.config_committed_at, 3),
                    }
                    integrity._log_compaction_integrity_evidence(
                        "continuous_mix_compaction_closed",
                        collection=self.collection_name,
                        **self.mix_closures[task_id],
                    )
            if task["type"] == "Level0DeleteCompaction" and task_id not in self.l0_closures:
                l0_sources = {
                    source_id
                    for source_id in task["sources"]
                    if self.segment_history.get(source_id, {}).get("level") == "L0"
                }
                sources_retired = bool(l0_sources) and all(
                    self.segment_history.get(source_id, {}).get("state") == "Dropped" for source_id in l0_sources
                )
                if sources_retired:
                    self.l0_closures[task_id] = {
                        "task": task,
                        "l0_source_ids": sorted(l0_sources),
                        "ingress_active": ingress_active,
                        "committed_sequence": self.oracle.max_committed_sequence,
                        "committed_action_count": self.oracle.action_count,
                        "elapsed_since_config_seconds": round(time.time() - self.config_committed_at, 3),
                    }
                    integrity._log_compaction_integrity_evidence(
                        "continuous_l0_compaction_closed",
                        collection=self.collection_name,
                        **self.l0_closures[task_id],
                    )

    def observe_once(self):
        checkpoint = self.capture()
        with self._lock:
            self.segment_history.update(checkpoint["all"])
            self.task_history.update(checkpoint["tasks"])
            if self.first_natural_flush_at is None and any(
                segment["state"] == "Flushed" for segment in checkpoint["active"].values()
            ):
                self.first_natural_flush_at = time.time()
                integrity._log_compaction_integrity_evidence(
                    "continuous_natural_flush_observed",
                    collection=self.collection_name,
                    active_segment_ids=sorted(checkpoint["active"]),
                    elapsed_since_config_seconds=round(time.time() - self.config_committed_at, 3),
                )
            nonzero_versions = {version for version in checkpoint["storage_versions"] if version != 0}
            assert nonzero_versions.issubset({3}), f"continuous V3 case observed storage versions {nonzero_versions}"
            self._observe_closures(checkpoint)
        return checkpoint

    def run(self, observer_stop):
        try:
            while not observer_stop.is_set():
                self.observe_once()
                observer_stop.wait(2)
        except Exception as error:  # The main test re-raises the original observer failure.
            self.error = error
            self.oracle.poison(f"lifecycle observer failed: {error}")
            self.stop_event.set()
            self.checkpoint_queue.put(None)

    def assert_active_writing_coverage(self):
        final_action_count = self.oracle.action_count
        with self._lock:
            active_mix = [closure for closure in self.mix_closures.values() if closure["ingress_active"]]
            active_l0 = [closure for closure in self.l0_closures.values() if closure["ingress_active"]]
        assert active_mix, "coverage_not_reached: no MixCompaction closure completed during active ingress"
        assert active_l0, "coverage_not_reached: no L0Compaction closure completed during active ingress"
        assert any(final_action_count > closure["committed_action_count"] for closure in active_mix), (
            "coverage_not_reached: no ingress progress followed the active MixCompaction closure"
        )
        assert any(final_action_count > closure["committed_action_count"] for closure in active_l0), (
            "coverage_not_reached: no ingress progress followed the active L0Compaction closure"
        )


def _continuous_lane_sequence(epoch, lane_index, operation_index):
    return (
        epoch * CONTINUOUS_SEQUENCE_SPAN_PER_EPOCH + lane_index * CONTINUOUS_MUTATIONS_PER_EPOCH + operation_index + 1
    )


def _continuous_logical_pk(epoch, lane_index, row_index):
    return epoch * 8000 + lane_index * 2200 + row_index


def _continuous_import_logical_pk(epoch, row_index):
    return epoch * 8000 + 4400 + row_index


def _continuous_row(collection_name, logical_pk, sequence, output_fields):
    row, expected = integrity._build_compaction_integrity_row(
        collection_name,
        logical_pk,
        sequence,
        DataType.VARCHAR,
        include_text=True,
        include_struct_array=True,
        test_sequence_id=sequence,
    )
    return row, integrity._canonical_compaction_integrity_row(expected, output_fields, DataType.VARCHAR)


def _continuous_import_row(row):
    writer_row = dict(row)
    for field_name in ("int8_vector", "nullable_int8_vector"):
        value = writer_row.get(field_name)
        if isinstance(value, bytes | bytearray):
            writer_row[field_name] = np.frombuffer(value, dtype=np.int8).copy()
    return writer_row


def _continuous_wait_for_import_visibility(client, collection_name, sequence_start, sequence_end, expected_rows):
    deadline = time.time() + CONTINUOUS_IMPORT_VISIBILITY_TIMEOUT
    query_filter = f"test_sequence_id >= {sequence_start} && test_sequence_id <= {sequence_end}"
    observed = 0
    while time.time() < deadline:
        iterator = client.query_iterator(
            collection_name,
            batch_size=integrity.COMPACTION_INTEGRITY_QUERY_BATCH_SIZE,
            filter=query_filter,
            output_fields=["id"],
            consistency_level="Strong",
        )
        try:
            observed = 0
            while True:
                batch = iterator.next()
                if not batch:
                    break
                observed += len(batch)
        finally:
            iterator.close()
        if observed == expected_rows:
            return
        time.sleep(2)
    raise AssertionError(
        f"import rows did not become visible: sequence=[{sequence_start},{sequence_end}] "
        f"actual={observed} expected={expected_rows}"
    )


@pytest.mark.xdist_group("TestMilvusClientCompactionDataIntegrity")
@pytest.mark.compaction_data_integrity_serial
class TestMilvusClientContinuousStreamingDataIntegrity(TestMilvusClientV2Base):
    """Continuous mixed-ingress correctness under automatic segment lifecycle transitions."""

    _create_compaction_integrity_collection = (
        integrity.TestMilvusClientCompactionDataIntegrity._create_compaction_integrity_collection
    )
    _ensure_compaction_integrity_utility_connection = (
        integrity.TestMilvusClientCompactionDataIntegrity._ensure_compaction_integrity_utility_connection
    )

    def _commit_row_request(self, client, collection_name, oracle, actions, rows, operation):
        if operation == "upsert":
            result = client.upsert(collection_name, rows)
            count_key = "upsert_count"
        else:
            result = client.insert(collection_name, rows)
            count_key = "insert_count"
        assert result[count_key] == len(rows), (
            f"{operation} receipt count mismatch: actual={result[count_key]} expected={len(rows)}"
        )
        oracle.commit(actions)
        integrity._log_compaction_integrity_evidence(
            "continuous_mutation_committed",
            collection=collection_name,
            lane=actions[0].lane,
            epoch=actions[0].epoch,
            operation=operation,
            sequence_start=actions[0].sequence,
            sequence_end=actions[-1].sequence,
            row_count=len(actions),
            oracle_action_count=oracle.action_count,
        )

    def _commit_delete_request(self, client, collection_name, oracle, actions):
        primary_keys = [action.primary_key for action in actions]
        result = client.delete(collection_name, filter=f"id in {json.dumps(primary_keys)}")
        assert result["delete_count"] == len(actions), (
            f"delete receipt count mismatch: actual={result['delete_count']} expected={len(actions)}"
        )
        oracle.commit(actions)
        integrity._log_compaction_integrity_evidence(
            "continuous_mutation_committed",
            collection=collection_name,
            lane=actions[0].lane,
            epoch=actions[0].epoch,
            operation="delete",
            sequence_start=actions[0].sequence,
            sequence_end=actions[-1].sequence,
            row_count=len(actions),
            oracle_action_count=oracle.action_count,
        )

    def _seed_l0_delete_candidates(self, client, collection_name, output_fields, oracle):
        for batch_start in range(0, CONTINUOUS_SEED_ROWS, CONTINUOUS_ROWS_PER_REQUEST):
            actions = []
            rows = []
            for row_index in range(batch_start, batch_start + CONTINUOUS_ROWS_PER_REQUEST):
                sequence = _continuous_lane_sequence(0, 0, row_index)
                logical_pk = _continuous_logical_pk(0, 0, row_index)
                row, expected = _continuous_row(collection_name, logical_pk, sequence, output_fields)
                rows.append(row)
                actions.append(_ContinuousAction(sequence, "mutation_a", 1, "insert", row["id"], expected))
            self._commit_row_request(client, collection_name, oracle, actions, rows, "insert")
            time.sleep(CONTINUOUS_REQUEST_INTERVAL)

        empty_checkpoint = {"all": {}, "active": {}, "serving": {}, "tasks": {}, "storage_versions": set()}
        seed_checkpoint = integrity._wait_for_compaction_integrity_checkpoint(
            client,
            collection_name,
            expected_rows=CONTINUOUS_SEED_ROWS,
            before_checkpoint=empty_checkpoint,
            require_new_inputs=False,
            transition_policy="stable",
            expected_storage_version=3,
            timeout=120,
        )
        integrity._assert_compaction_integrity_dataset(
            client,
            collection_name,
            oracle.snapshot(CONTINUOUS_SEED_ROWS)[0],
            output_fields,
            DataType.VARCHAR,
            query_filter=f"test_sequence_id <= {CONTINUOUS_SEED_ROWS}",
        )
        integrity._log_compaction_integrity_evidence(
            "continuous_l0_candidates_persisted",
            collection=collection_name,
            candidate_primary_keys=sorted(oracle.snapshot(CONTINUOUS_SEED_ROWS)[0]),
            **integrity._compaction_integrity_checkpoint_audit(seed_checkpoint),
        )

    def _run_mutation_lane(
        self,
        client,
        collection_name,
        output_fields,
        oracle,
        tracker,
        stop_event,
        checkpoint_queue,
        lane_index,
    ):
        lane = f"mutation_{'a' if lane_index == 0 else 'b'}"
        try:
            for epoch in range(CONTINUOUS_EPOCHS):
                if stop_event.is_set():
                    return
                insert_start = CONTINUOUS_SEED_ROWS if lane_index == 0 and epoch == 0 else 0
                for batch_start in range(insert_start, CONTINUOUS_FIRST_INSERTS, CONTINUOUS_ROWS_PER_REQUEST):
                    if stop_event.is_set():
                        return
                    actions = []
                    rows = []
                    for row_index in range(batch_start, batch_start + CONTINUOUS_ROWS_PER_REQUEST):
                        sequence = _continuous_lane_sequence(epoch, lane_index, row_index)
                        logical_pk = _continuous_logical_pk(epoch, lane_index, row_index)
                        row, expected = _continuous_row(collection_name, logical_pk, sequence, output_fields)
                        rows.append(row)
                        actions.append(_ContinuousAction(sequence, lane, epoch + 1, "insert", row["id"], expected))
                    self._commit_row_request(client, collection_name, oracle, actions, rows, "insert")
                    if stop_event.wait(CONTINUOUS_REQUEST_INTERVAL):
                        return

                operation_specs = (
                    ("upsert", CONTINUOUS_FIRST_INSERTS, CONTINUOUS_UPSERTS, 200),
                    (
                        "delete",
                        CONTINUOUS_FIRST_INSERTS + CONTINUOUS_UPSERTS,
                        CONTINUOUS_DELETES,
                        0,
                    ),
                    (
                        "reinsert",
                        CONTINUOUS_FIRST_INSERTS + CONTINUOUS_UPSERTS + CONTINUOUS_DELETES,
                        CONTINUOUS_REINSERTS,
                        0,
                    ),
                    (
                        "duplicate_insert",
                        CONTINUOUS_FIRST_INSERTS + CONTINUOUS_UPSERTS + CONTINUOUS_DELETES + CONTINUOUS_REINSERTS,
                        CONTINUOUS_DUPLICATE_INSERTS,
                        600,
                    ),
                )
                for operation, operation_offset, count, pk_offset in operation_specs:
                    for batch_offset in range(0, count, CONTINUOUS_ROWS_PER_REQUEST):
                        if stop_event.is_set():
                            return
                        actions = []
                        rows = []
                        for item_offset in range(batch_offset, batch_offset + CONTINUOUS_ROWS_PER_REQUEST):
                            operation_index = operation_offset + item_offset
                            sequence = _continuous_lane_sequence(epoch, lane_index, operation_index)
                            logical_pk = _continuous_logical_pk(epoch, lane_index, pk_offset + item_offset)
                            primary_key = integrity._compaction_integrity_physical_pk(logical_pk, DataType.VARCHAR)
                            if operation == "delete":
                                actions.append(
                                    _ContinuousAction(sequence, lane, epoch + 1, operation, primary_key, None)
                                )
                            else:
                                row, expected = _continuous_row(
                                    collection_name,
                                    logical_pk,
                                    sequence,
                                    output_fields,
                                )
                                rows.append(row)
                                actions.append(
                                    _ContinuousAction(sequence, lane, epoch + 1, operation, primary_key, expected)
                                )
                        if operation == "delete":
                            self._commit_delete_request(client, collection_name, oracle, actions)
                        else:
                            rpc_operation = "upsert" if operation == "upsert" else "insert"
                            self._commit_row_request(
                                client,
                                collection_name,
                                oracle,
                                actions,
                                rows,
                                rpc_operation,
                            )
                        if stop_event.wait(CONTINUOUS_REQUEST_INTERVAL):
                            return
                tracker.complete(lane, epoch)
        except Exception as error:
            oracle.poison(f"{lane} failed: {error}")
            stop_event.set()
            checkpoint_queue.put(None)
            raise

    def _run_import_lane(
        self,
        client,
        collection_name,
        schema,
        output_fields,
        oracle,
        tracker,
        stop_event,
        checkpoint_queue,
        minio_host,
        minio_bucket,
    ):
        lane = "import"
        try:
            self._ensure_compaction_integrity_utility_connection()
            for epoch in range(CONTINUOUS_EPOCHS):
                if stop_event.is_set():
                    return
                actions = []
                sequence_start = _continuous_lane_sequence(epoch, 2, 0)
                sequence_end = _continuous_lane_sequence(epoch, 2, CONTINUOUS_IMPORT_ROWS_PER_EPOCH - 1)
                with RemoteBulkWriter(
                    schema=schema,
                    remote_path=f"continuous_integrity/{collection_name}/epoch_{epoch + 1}",
                    connect_param=RemoteBulkWriter.ConnectParam(
                        bucket_name=minio_bucket,
                        endpoint=f"{minio_host}:9000",
                        access_key="minioadmin",
                        secret_key="minioadmin",
                    ),
                    file_type=BulkFileType.PARQUET,
                ) as remote_writer:
                    for row_index in range(CONTINUOUS_IMPORT_ROWS_PER_EPOCH):
                        sequence = _continuous_lane_sequence(epoch, 2, row_index)
                        logical_pk = _continuous_import_logical_pk(epoch, row_index)
                        row, expected = _continuous_row(collection_name, logical_pk, sequence, output_fields)
                        remote_writer.append_row(_continuous_import_row(row))
                        actions.append(_ContinuousAction(sequence, lane, epoch + 1, "import", row["id"], expected))
                    remote_writer.commit()
                    batch_files = remote_writer.batch_files
                assert len(batch_files) == 1, f"ImportIngress expected one file group, got {batch_files}"
                task_id, _ = self.utility_wrap.do_bulk_insert(
                    collection_name=collection_name,
                    files=batch_files[0],
                )
                completed, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
                    task_ids=[task_id],
                    timeout=600,
                )
                state = states.get(task_id)
                assert completed and state is not None, f"import job {task_id} did not complete: {states}"
                assert state.state == BulkInsertState.ImportCompleted, (
                    f"import job {task_id} reached {state.state_name}: {state.failed_reason}"
                )
                assert state.row_count == CONTINUOUS_IMPORT_ROWS_PER_EPOCH
                assert state.progress == 100
                _continuous_wait_for_import_visibility(
                    client,
                    collection_name,
                    sequence_start,
                    sequence_end,
                    CONTINUOUS_IMPORT_ROWS_PER_EPOCH,
                )
                oracle.commit(actions)
                integrity._log_compaction_integrity_evidence(
                    "continuous_import_committed",
                    collection=collection_name,
                    lane=lane,
                    epoch=epoch + 1,
                    sequence_start=sequence_start,
                    sequence_end=sequence_end,
                    row_count=len(actions),
                    job_id=task_id,
                    files=batch_files[0],
                    oracle_action_count=oracle.action_count,
                )
                tracker.complete(lane, epoch)
        except Exception as error:
            oracle.poison(f"{lane} failed: {error}")
            stop_event.set()
            checkpoint_queue.put(None)
            raise

    def _validate_prefixes(
        self,
        client,
        collection_name,
        output_fields,
        oracle,
        checkpoint_queue,
        observer,
    ):
        validated = []
        while len(validated) < CONTINUOUS_EPOCHS - 1:
            epoch = checkpoint_queue.get(timeout=CONTINUOUS_CASE_TIMEOUT)
            assert epoch is not None, "prefix validation stopped because an ingress lane failed"
            boundary = (epoch + 1) * CONTINUOUS_SEQUENCE_SPAN_PER_EPOCH
            expected_by_pk, deleted_primary_keys = oracle.snapshot(boundary)
            assert len(expected_by_pk) == (epoch + 1) * CONTINUOUS_EXPECTED_LIVE_ROWS_PER_EPOCH
            before = observer.capture()
            progress_sequence_before = oracle.max_committed_sequence
            progress_count_before = oracle.action_count
            started_at = time.time()
            validation = integrity._assert_compaction_integrity_dataset(
                client,
                collection_name,
                expected_by_pk,
                output_fields,
                DataType.VARCHAR,
                query_filter=f"test_sequence_id <= {boundary}",
            )
            after = observer.capture()
            progress_sequence_after = oracle.max_committed_sequence
            progress_count_after = oracle.action_count
            assert progress_count_after > progress_count_before and progress_sequence_after > boundary, (
                f"prefix {epoch + 1} was not validated under active ingress: "
                f"action_count={progress_count_before}->{progress_count_after} "
                f"max_sequence={progress_sequence_before}->{progress_sequence_after}"
            )
            integrity._log_compaction_integrity_evidence(
                "continuous_prefix_validated",
                collection=collection_name,
                prefix=epoch + 1,
                boundary=boundary,
                expected_live_rows=len(expected_by_pk),
                deleted_primary_key_count=len(deleted_primary_keys),
                expected_pk_digest=integrity._compaction_integrity_pk_digest(expected_by_pk, DataType.VARCHAR),
                progress_sequence_before=progress_sequence_before,
                progress_sequence_after=progress_sequence_after,
                progress_count_before=progress_count_before,
                progress_count_after=progress_count_after,
                duration_seconds=round(time.time() - started_at, 3),
                data_validation=validation,
                before_frontier=integrity._compaction_integrity_checkpoint_audit(before),
                after_frontier=integrity._compaction_integrity_checkpoint_audit(after),
            )
            validated.append(epoch)
        return validated

    def _run_prefix_validator(
        self,
        client,
        collection_name,
        output_fields,
        oracle,
        checkpoint_queue,
        observer,
        stop_event,
    ):
        try:
            return self._validate_prefixes(
                client,
                collection_name,
                output_fields,
                oracle,
                checkpoint_queue,
                observer,
            )
        except Exception as error:
            oracle.poison(f"prefix validator failed: {error}")
            stop_event.set()
            checkpoint_queue.put(None)
            raise

    @pytest.mark.tags(CaseLabel.L3)
    def test_continuous_mixed_ingress_auto_compaction_preserves_all_rows_and_fields(
        self,
        minio_host,
        minio_bucket,
        etcd_host,
        etcd_port,
        etcd_root_path,
        etcd_user,
        etcd_password,
    ):
        client = self._client()
        collection_name = cf.gen_unique_str("continuous_data_integrity")
        deadline = time.time() + CONTINUOUS_CASE_TIMEOUT
        oracle = _ContinuousOracle()
        stop_event = threading.Event()
        ingress_done = threading.Event()
        observer_stop = threading.Event()
        checkpoint_queue = queue.Queue()
        tracker = _ContinuousEpochTracker(checkpoint_queue)
        integrity._validate_compaction_integrity_float32_range(32400, CONTINUOUS_EPOCHS * 9000)

        with MilvusEtcdConfigController(
            host=etcd_host,
            port=etcd_port,
            root_path=etcd_root_path,
            user=etcd_user,
            password=etcd_password,
        ) as config_controller:
            originals = {key: config_controller.read_config(key) for key in CONTINUOUS_RUNTIME_CONFIG}
            with ExitStack() as config_restore:
                for key in CONTINUOUS_RUNTIME_CONFIG:
                    config_restore.enter_context(config_controller.preserve_config(key))
                revisions = {}
                for key, value in CONTINUOUS_RUNTIME_CONFIG.items():
                    configured = config_controller.set_config(key, value)
                    revisions[key] = configured.mod_revision
                config_committed_at = time.time()
                integrity._log_compaction_integrity_evidence(
                    "continuous_runtime_config_committed",
                    collection=collection_name,
                    etcd_endpoint=config_controller.endpoint,
                    etcd_root_path=etcd_root_path,
                    original_values={
                        key: None if original.value is None else original.value.decode()
                        for key, original in originals.items()
                    },
                    desired_values=CONTINUOUS_RUNTIME_CONFIG,
                    revisions=revisions,
                    settle_seconds=10,
                )
                time.sleep(10)

                output_fields, schema = self._create_compaction_integrity_collection(
                    client,
                    collection_name,
                    DataType.VARCHAR,
                    include_text=True,
                    include_struct_array=True,
                    include_test_sequence_id=True,
                )
                integrity._log_compaction_integrity_evidence(
                    "continuous_case_started",
                    collection=collection_name,
                    storage_version=3,
                    epochs=CONTINUOUS_EPOCHS,
                    lanes=sorted(CONTINUOUS_LANES),
                    mutation_operations_per_epoch=CONTINUOUS_MUTATIONS_PER_EPOCH,
                    import_rows_per_epoch=CONTINUOUS_IMPORT_ROWS_PER_EPOCH,
                    request_rows=CONTINUOUS_ROWS_PER_REQUEST,
                    request_interval_seconds=CONTINUOUS_REQUEST_INTERVAL,
                    output_fields=output_fields,
                )

                self._seed_l0_delete_candidates(client, collection_name, output_fields, oracle)
                observer = _ContinuousLifecycleObserver(
                    client,
                    collection_name,
                    oracle,
                    ingress_done,
                    stop_event,
                    checkpoint_queue,
                    config_committed_at,
                )
                observer_thread = threading.Thread(
                    target=observer.run,
                    args=(observer_stop,),
                    name="continuous-integrity-observer",
                    daemon=True,
                )
                observer_thread.start()
                try:
                    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="continuous-integrity") as executor:
                        producer_futures = [
                            executor.submit(
                                self._run_mutation_lane,
                                client,
                                collection_name,
                                output_fields,
                                oracle,
                                tracker,
                                stop_event,
                                checkpoint_queue,
                                lane_index,
                            )
                            for lane_index in (0, 1)
                        ]
                        producer_futures.append(
                            executor.submit(
                                self._run_import_lane,
                                client,
                                collection_name,
                                schema,
                                output_fields,
                                oracle,
                                tracker,
                                stop_event,
                                checkpoint_queue,
                                minio_host,
                                minio_bucket,
                            )
                        )
                        validator_future = executor.submit(
                            self._run_prefix_validator,
                            client,
                            collection_name,
                            output_fields,
                            oracle,
                            checkpoint_queue,
                            observer,
                            stop_event,
                        )
                        for future in producer_futures:
                            future.result(timeout=max(1, deadline - time.time()))
                        ingress_done.set()
                        validated_prefixes = validator_future.result(timeout=max(1, deadline - time.time()))
                    assert validated_prefixes == [0, 1, 2]
                    assert oracle.action_count == CONTINUOUS_EPOCHS * CONTINUOUS_SEQUENCE_SPAN_PER_EPOCH

                    final_boundary = CONTINUOUS_EPOCHS * CONTINUOUS_SEQUENCE_SPAN_PER_EPOCH
                    expected_by_pk, deleted_primary_keys = oracle.snapshot(final_boundary)
                    assert len(expected_by_pk) == CONTINUOUS_EPOCHS * CONTINUOUS_EXPECTED_LIVE_ROWS_PER_EPOCH
                    empty_checkpoint = {
                        "all": {},
                        "active": {},
                        "serving": {},
                        "tasks": {},
                        "storage_versions": set(),
                    }
                    final_checkpoint = integrity._wait_for_compaction_integrity_checkpoint(
                        client,
                        collection_name,
                        expected_rows=len(expected_by_pk),
                        before_checkpoint=empty_checkpoint,
                        require_new_inputs=False,
                        transition_policy="stable",
                        expected_storage_version=3,
                        timeout=min(CONTINUOUS_FINAL_CHECKPOINT_TIMEOUT, max(1, deadline - time.time())),
                    )
                    final_checkpoint, _, final_validation = integrity._assert_compaction_integrity_fenced_dataset(
                        client,
                        collection_name,
                        expected_by_pk,
                        output_fields,
                        DataType.VARCHAR,
                        expected_storage_version=3,
                        timeout=min(CONTINUOUS_FINAL_CHECKPOINT_TIMEOUT, max(1, deadline - time.time())),
                    )

                    client.release_collection(collection_name)
                    client.load_collection(collection_name)
                    reload_checkpoint, _, reload_validation = integrity._assert_compaction_integrity_fenced_dataset(
                        client,
                        collection_name,
                        expected_by_pk,
                        output_fields,
                        DataType.VARCHAR,
                        expected_storage_version=3,
                        timeout=min(CONTINUOUS_FINAL_CHECKPOINT_TIMEOUT, max(1, deadline - time.time())),
                    )
                    observer.observe_once()
                    observer_stop.set()
                    observer_thread.join(timeout=10)
                    if observer.error is not None:
                        raise observer.error
                    observer.assert_active_writing_coverage()
                    integrity._log_compaction_integrity_evidence(
                        "continuous_case_passed",
                        collection=collection_name,
                        final_boundary=final_boundary,
                        expected_live_rows=len(expected_by_pk),
                        deleted_primary_key_count=len(deleted_primary_keys),
                        expected_pk_digest=integrity._compaction_integrity_pk_digest(
                            expected_by_pk,
                            DataType.VARCHAR,
                        ),
                        final_validation=final_validation,
                        reload_validation=reload_validation,
                        mix_closures=list(observer.mix_closures.values()),
                        l0_closures=list(observer.l0_closures.values()),
                        final_checkpoint=integrity._compaction_integrity_checkpoint_audit(final_checkpoint),
                        reload_checkpoint=integrity._compaction_integrity_checkpoint_audit(reload_checkpoint),
                    )
                finally:
                    ingress_done.set()
                    stop_event.set()
                    observer_stop.set()
                    observer_thread.join(timeout=10)
