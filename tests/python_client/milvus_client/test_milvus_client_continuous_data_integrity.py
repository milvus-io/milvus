import json
import os
import queue
import sys
import tempfile
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import ExitStack, contextmanager, nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace

import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from common.data_integrity_diagnostics import emit_data_integrity_event, exception_diagnostics
from milvus_client import test_milvus_client_data_integrity as integrity
from pymilvus import BulkInsertState, DataType, utility
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
    "common.storage.useLoonFFI": "true",
    "dataNode.segment.syncPeriod": "10",
    "dataCoord.segment.maxSize": "64",
    "dataCoord.segment.maxLife": "60",
    "streaming.flush.l0.maxLifetime": "30s",
    "dataCoord.compaction.levelzero.forceTrigger.deltalogMinNum": "4",
}
CONTINUOUS_LANES = {"mutation_a", "mutation_b", "import"}


class _ContinuousBudget:
    """Cooperative case deadline, not a watchdog for uninterruptible SDK calls."""

    def __init__(self, timeout, stop_event):
        self.deadline = time.monotonic() + timeout
        self.stop_event = stop_event

    def remaining(self, cap=None):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("continuous integrity case deadline exceeded")
        if self.stop_event.is_set():
            raise RuntimeError("continuous integrity case stopped")
        return remaining if cap is None else min(cap, remaining)

    def wait(self, seconds):
        self.stop_event.wait(self.remaining(seconds))
        self.remaining()


@contextmanager
def _continuous_executor(stop_event, checkpoint_queue, failure_recorder):
    # Signal cancellation BEFORE ThreadPoolExecutor.__exit__ waits for workers.
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="continuous-integrity") as executor:
        try:
            yield executor
        except BaseException as error:
            # Claim the coordinator failure before cancellation lets a worker
            # observe a stopped budget and before executor shutdown waits for it.
            failure_recorder.record(error, "coordinator", phase="await_workers")
            stop_event.set()
            checkpoint_queue.put(None)
            raise


def _continuous_wait_for_import(task_id, budget):
    deadline = time.monotonic() + budget.remaining(600)
    while True:
        remaining = min(deadline - time.monotonic(), budget.remaining())
        if remaining <= 0:
            raise TimeoutError(f"import job {task_id} did not complete within its budget")
        state = utility.get_bulk_insert_state(task_id, timeout=remaining)
        budget.remaining()
        if state.state == BulkInsertState.ImportCompleted:
            return state
        assert state.state in {
            BulkInsertState.ImportPending,
            BulkInsertState.ImportStarted,
            BulkInsertState.ImportPersisted,
        }, f"import job {task_id} reached {state.state_name}: {state.failed_reason}"
        budget.wait(min(2, max(0, deadline - time.monotonic())))


def _continuous_wait_for_futures(futures, budget, failure_recorder):
    """Poll worker Futures so an already-recorded first error is surfaced promptly."""
    pending = set(futures)
    while pending:
        if failure_recorder.primary() is not None:
            failure_recorder.raise_primary(TimeoutError("continuous worker failed"))
        remaining = budget.deadline - time.monotonic()
        if remaining <= 0:
            timeout = TimeoutError("continuous integrity case deadline exceeded while awaiting workers")
            failure_recorder.record(timeout, "coordinator", phase="deadline")
            failure_recorder.raise_primary(timeout)
        done, pending = wait(pending, timeout=min(0.5, remaining), return_when=FIRST_COMPLETED)
        for future in done:
            try:
                future.result()
            except BaseException as error:
                failure_recorder.record(error, "coordinator", phase="await_workers")
                failure_recorder.raise_primary(error)


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
        budget,
        failure_recorder,
    ):
        self.client = client
        self.collection_name = collection_name
        self.oracle = oracle
        self.ingress_done = ingress_done
        self.stop_event = stop_event
        self.checkpoint_queue = checkpoint_queue
        self.config_committed_at = config_committed_at
        self.budget = budget
        self.failure_recorder = failure_recorder
        self.segment_history = {}
        self.task_history = {}
        self.mix_closures = {}
        self.l0_closures = {}
        self.first_natural_flush_at = None
        self.error = None
        self._lock = threading.Lock()

    def capture(self):
        all_segments = integrity._snapshot_compaction_integrity_segments(
            self.client.list_segments(self.collection_name, timeout=self.budget.remaining(5))
        )
        active_segments = {
            segment_id: segment
            for segment_id, segment in all_segments.items()
            if segment["state"] in integrity.COMPACTION_INTEGRITY_ACTIVE_STATES and segment["num_rows"] != 0
        }
        serving_segments = {
            segment_id: segment
            for segment_id, segment in integrity._snapshot_compaction_integrity_segments(
                self.client.list_loaded_segments(self.collection_name, timeout=self.budget.remaining(5))
            ).items()
            if segment["num_rows"] != 0
        }
        tasks = integrity._snapshot_compaction_integrity_tasks(
            self.client.list_compaction_tasks(self.collection_name, timeout=self.budget.remaining(5))
        )
        self.budget.remaining()
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
            self.failure_recorder.record(error, "observer", phase="lifecycle_observation")
            self.oracle.poison(f"lifecycle observer failed: {type(error).__name__}")
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


def _continuous_import_row(row, raw_expected, output_fields):
    """Convert physical INT8 input only; preserve all logical values and the oracle.

    Unlike the regular compaction workload, the continuous design requires null
    equivalence across both ingress paths. Unsupported SDK/import null handling
    must fail visibly instead of changing nulls into empty or populated values.
    """
    writer_row = dict(row)
    for field_name in ("int8_vector", "nullable_int8_vector"):
        value = writer_row.get(field_name)
        if isinstance(value, bytes | bytearray):
            writer_row[field_name] = np.frombuffer(value, dtype=np.int8).copy()
    canonical_expected = integrity._canonical_compaction_integrity_row(raw_expected, output_fields, DataType.VARCHAR)
    return writer_row, canonical_expected


class _ContinuousFailureRecorder:
    """Thread-safe first-error ownership for continuous lanes and their coordinator."""

    def __init__(self, nodeid):
        self.nodeid = nodeid
        self._lock = threading.Lock()
        self._first = None
        self._recorded_errors = []

    def record(self, error, lane, epoch=None, phase="worker"):
        with self._lock:
            if any(error is recorded for recorded in self._recorded_errors):
                return False
            primary = self._first is None
            # Keep at most eight distinct secondary failures and omit expected
            # cooperative cancellation once the underlying failure is recorded.
            if not primary and (
                len(self._recorded_errors) >= 9
                or (
                    type(error) is RuntimeError
                    and len(error.args) == 1
                    and type(error.args[0]) is str
                    and error.args[0] == "continuous integrity case stopped"
                )
            ):
                return False
            self._recorded_errors.append(error)
            if primary:
                self._first = (error, error.__traceback__, lane, epoch, phase, time.monotonic())
        try:
            emit_data_integrity_event(
                {
                    "event": "continuous_primary_failure" if primary else "continuous_secondary_failure",
                    "nodeid": self.nodeid,
                    "lane": lane,
                    "epoch": epoch,
                    "phase": phase,
                    **exception_diagnostics(error),
                },
                primary_failure=primary,
            )
        except BaseException:
            pass
        return primary

    def primary(self):
        with self._lock:
            return self._first

    def raise_primary(self, fallback):
        first = self.primary()
        if first is None:
            raise fallback
        error, original_traceback, *_ = first
        raise error.with_traceback(original_traceback)


@contextmanager
def _continuous_writer_context(writer, failure_recorder, lane, epoch):
    """Record writer-body errors before RemoteBulkWriter.__exit__ runs."""
    writer_entered = False
    try:
        with writer as active_writer:
            writer_entered = True
            try:
                yield active_writer
            except BaseException as error:
                failure_recorder.record(error, lane, epoch, phase="writer_body")
                raise
    except BaseException as error:
        failure_recorder.record(error, lane, epoch, phase="writer_exit" if writer_entered else "writer_setup")
        raise


class _ContinuousProgress:
    """Bound continuous progress events to one heartbeat per lane per 30 seconds."""

    def __init__(self, nodeid):
        self.nodeid = nodeid
        self.started = time.monotonic()
        self._lock = threading.Lock()
        self._last_heartbeat = {}
        self._active = {}

    def set_active(self, lane, epoch, phase, budget, oracle):
        with self._lock:
            self._active[lane] = (epoch, phase, budget, oracle)

    def finish(self, lane, epoch, phase, budget, oracle, outcome):
        self.emit(lane, epoch, phase, budget, oracle, event="continuous_case_completion", outcome=outcome)
        with self._lock:
            self._active.pop(lane, None)

    def heartbeat_loop(self, stop_event):
        while not stop_event.wait(30):
            with self._lock:
                active = list(self._active.items())
            for lane, (epoch, phase, budget, oracle) in active:
                self.emit(lane, epoch, phase, budget, oracle, event="continuous_heartbeat", force=True)

    def emit(self, lane, epoch, phase, budget, oracle, *, event="continuous_phase", force=False, **details):
        now = time.monotonic()
        with self._lock:
            if event in {"continuous_phase", "continuous_case_start"}:
                self._active[lane] = (epoch, phase, budget, oracle)
            last = self._last_heartbeat.get(lane)
            if event == "continuous_heartbeat" and not force and last is not None and now - last < 30:
                return
            if event == "continuous_heartbeat":
                self._last_heartbeat[lane] = now
        remaining = max(0.0, budget.deadline - now)
        emit_data_integrity_event(
            {
                "event": event,
                "nodeid": self.nodeid,
                "lane": lane,
                "epoch": epoch,
                "phase": phase,
                "timestamp_utc": datetime.now(UTC).isoformat(),
                "elapsed_seconds": round(now - self.started, 3),
                "remaining_budget_seconds": round(remaining, 3),
                "committed_action_count": oracle.action_count,
                **details,
            }
        )


def _continuous_wait_for_import_visibility(
    client, collection_name, sequence_start, sequence_end, expected_rows, budget
):
    deadline = time.monotonic() + budget.remaining(CONTINUOUS_IMPORT_VISIBILITY_TIMEOUT)
    query_filter = f"test_sequence_id >= {sequence_start} && test_sequence_id <= {sequence_end}"
    observed = 0
    while time.monotonic() < deadline:
        iterator = client.query_iterator(
            collection_name,
            batch_size=integrity.COMPACTION_INTEGRITY_QUERY_BATCH_SIZE,
            filter=query_filter,
            output_fields=["id"],
            consistency_level="Strong",
            timeout=budget.remaining(min(30, deadline - time.monotonic())),
        )
        try:
            observed = 0
            while True:
                budget.remaining()
                batch = iterator.next()
                budget.remaining()
                if not batch:
                    break
                observed += len(batch)
        finally:
            iterator.close()
        if observed == expected_rows:
            return
        budget.wait(min(2, max(0, deadline - time.monotonic())))
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

    def _commit_row_request(self, client, collection_name, oracle, actions, rows, operation, budget):
        if operation == "upsert":
            result = client.upsert(collection_name, rows, timeout=budget.remaining())
            count_key = "upsert_count"
        else:
            result = client.insert(collection_name, rows, timeout=budget.remaining())
            count_key = "insert_count"
        budget.remaining()
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

    def _commit_delete_request(self, client, collection_name, oracle, actions, budget):
        primary_keys = [action.primary_key for action in actions]
        result = client.delete(collection_name, filter=f"id in {json.dumps(primary_keys)}", timeout=budget.remaining())
        budget.remaining()
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

    def _seed_l0_delete_candidates(self, client, collection_name, output_fields, oracle, budget):
        for batch_start in range(0, CONTINUOUS_SEED_ROWS, CONTINUOUS_ROWS_PER_REQUEST):
            actions = []
            rows = []
            for row_index in range(batch_start, batch_start + CONTINUOUS_ROWS_PER_REQUEST):
                sequence = _continuous_lane_sequence(0, 0, row_index)
                logical_pk = _continuous_logical_pk(0, 0, row_index)
                row, expected = _continuous_row(collection_name, logical_pk, sequence, output_fields)
                rows.append(row)
                actions.append(_ContinuousAction(sequence, "mutation_a", 1, "insert", row["id"], expected))
            self._commit_row_request(client, collection_name, oracle, actions, rows, "insert", budget)
            budget.wait(CONTINUOUS_REQUEST_INTERVAL)

        empty_checkpoint = {"all": {}, "active": {}, "serving": {}, "tasks": {}, "storage_versions": set()}
        seed_checkpoint = integrity._wait_for_compaction_integrity_checkpoint(
            client,
            collection_name,
            expected_rows=CONTINUOUS_SEED_ROWS,
            before_checkpoint=empty_checkpoint,
            require_new_inputs=False,
            transition_policy="stable",
            expected_storage_version=3,
            timeout=budget.remaining(180),
            remaining_timeout=budget.remaining,
        )
        integrity._assert_compaction_integrity_dataset(
            client,
            collection_name,
            oracle.snapshot(CONTINUOUS_SEED_ROWS)[0],
            output_fields,
            DataType.VARCHAR,
            query_filter=f"test_sequence_id <= {CONTINUOUS_SEED_ROWS}",
            remaining_timeout=budget.remaining,
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
        budget,
        failure_recorder,
        progress,
    ):
        lane = f"mutation_{'a' if lane_index == 0 else 'b'}"
        progress.emit(lane, 0, "started", budget, oracle, event="continuous_case_start")
        try:
            for epoch in range(CONTINUOUS_EPOCHS):
                progress.emit(lane, epoch + 1, "insert", budget, oracle)
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
                    self._commit_row_request(client, collection_name, oracle, actions, rows, "insert", budget)
                    budget.wait(CONTINUOUS_REQUEST_INTERVAL)

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
                    progress.emit(lane, epoch + 1, operation, budget, oracle)
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
                            self._commit_delete_request(client, collection_name, oracle, actions, budget)
                        else:
                            rpc_operation = "upsert" if operation == "upsert" else "insert"
                            self._commit_row_request(
                                client,
                                collection_name,
                                oracle,
                                actions,
                                rows,
                                rpc_operation,
                                budget,
                            )
                        budget.wait(CONTINUOUS_REQUEST_INTERVAL)
                tracker.complete(lane, epoch)
                progress.emit(lane, epoch + 1, "epoch_complete", budget, oracle)
            progress.finish(lane, CONTINUOUS_EPOCHS, "complete", budget, oracle, "passed")
        except Exception as error:
            failure_recorder.record(error, lane, phase="mutation")
            oracle.poison(f"{lane} failed: {type(error).__name__}")
            stop_event.set()
            checkpoint_queue.put(None)
            progress.finish(lane, 0, "failed", budget, oracle, "failed")
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
        budget,
        failure_recorder,
        progress,
    ):
        lane = "import"
        progress.emit(lane, 0, "started", budget, oracle, event="continuous_case_start")
        try:
            self._ensure_compaction_integrity_utility_connection()
            for epoch in range(CONTINUOUS_EPOCHS):
                if stop_event.is_set():
                    return
                actions = []
                sequence_start = _continuous_lane_sequence(epoch, 2, 0)
                sequence_end = _continuous_lane_sequence(epoch, 2, CONTINUOUS_IMPORT_ROWS_PER_EPOCH - 1)
                progress.emit(lane, epoch + 1, "payload_construction", budget, oracle)
                with _continuous_writer_context(
                    RemoteBulkWriter(
                        local_path=tempfile.mkdtemp(prefix="bulk_writer_"),
                        schema=schema,
                        remote_path=f"continuous_integrity/{collection_name}/epoch_{epoch + 1}",
                        connect_param=RemoteBulkWriter.ConnectParam(
                            bucket_name=minio_bucket,
                            endpoint=f"{minio_host}:9000",
                            access_key="minioadmin",
                            secret_key="minioadmin",
                        ),
                        file_type=BulkFileType.PARQUET,
                    ),
                    failure_recorder,
                    lane,
                    epoch + 1,
                ) as remote_writer:
                    for row_index in range(CONTINUOUS_IMPORT_ROWS_PER_EPOCH):
                        budget.remaining()
                        sequence = _continuous_lane_sequence(epoch, 2, row_index)
                        logical_pk = _continuous_import_logical_pk(epoch, row_index)
                        row, raw_expected = integrity._build_compaction_integrity_row(
                            collection_name,
                            logical_pk,
                            sequence,
                            DataType.VARCHAR,
                            include_text=True,
                            include_struct_array=True,
                            test_sequence_id=sequence,
                        )
                        writer_row, expected = _continuous_import_row(row, raw_expected, output_fields)
                        remote_writer.append_row(writer_row)
                        actions.append(_ContinuousAction(sequence, lane, epoch + 1, "import", row["id"], expected))
                    progress.emit(
                        lane,
                        epoch + 1,
                        "append_row_batch",
                        budget,
                        oracle,
                        event="continuous_phase",
                        batch_rows=len(actions),
                    )
                    progress.emit(
                        lane,
                        epoch + 1,
                        "writer_commit",
                        budget,
                        oracle,
                        event="continuous_phase",
                        batch_rows=len(actions),
                    )
                    remote_writer.commit()
                    batch_files = remote_writer.batch_files
                budget.remaining()
                assert len(batch_files) == 1, f"ImportIngress expected one file group, got {batch_files}"
                # The generic wrapper performs extra unbounded list/poll calls.
                progress.emit(lane, epoch + 1, "import_rpc_attempt", budget, oracle)
                task_id = utility.do_bulk_insert(
                    collection_name=collection_name,
                    files=batch_files[0],
                    timeout=budget.remaining(),
                )
                progress.emit(lane, epoch + 1, "import_rpc_acknowledged", budget, oracle, job_id=task_id)
                state = _continuous_wait_for_import(task_id, budget)
                progress.emit(lane, epoch + 1, "import_job_completed", budget, oracle, job_id=task_id)
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
                    budget,
                )
                progress.emit(lane, epoch + 1, "visibility_confirmed", budget, oracle, job_id=task_id)
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
                progress.emit(lane, epoch + 1, "epoch_complete", budget, oracle)
            progress.finish(lane, CONTINUOUS_EPOCHS, "complete", budget, oracle, "passed")
        except Exception as error:
            failure_recorder.record(error, lane, phase="import")
            oracle.poison(f"{lane} failed: {type(error).__name__}")
            stop_event.set()
            checkpoint_queue.put(None)
            progress.finish(lane, 0, "failed", budget, oracle, "failed")
            raise

    def _validate_prefixes(
        self,
        client,
        collection_name,
        output_fields,
        oracle,
        checkpoint_queue,
        observer,
        budget,
    ):
        validated = []
        while len(validated) < CONTINUOUS_EPOCHS - 1:
            epoch = checkpoint_queue.get(timeout=budget.remaining())
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
                remaining_timeout=budget.remaining,
            )
            after = observer.capture()
            progress_sequence_after = oracle.max_committed_sequence
            progress_count_after = oracle.action_count
            has_suffix_progress = progress_count_after > progress_count_before and progress_sequence_after > boundary
            integrity._log_compaction_integrity_evidence(
                "continuous_prefix_validated" if has_suffix_progress else "continuous_prefix_coverage_not_reached",
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
            assert has_suffix_progress, (
                f"coverage_not_reached: prefix {epoch + 1} was not validated under active ingress: "
                f"action_count={progress_count_before}->{progress_count_after} "
                f"max_sequence={progress_sequence_before}->{progress_sequence_after}"
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
        budget,
        failure_recorder,
    ):
        try:
            return self._validate_prefixes(
                client,
                collection_name,
                output_fields,
                oracle,
                checkpoint_queue,
                observer,
                budget,
            )
        except (Exception, pytest.fail.Exception) as error:
            failure_recorder.record(error, "validator", phase="prefix_validation")
            oracle.poison(f"prefix validator failed: {type(error).__name__}")
            stop_event.set()
            checkpoint_queue.put(None)
            raise

    def _verify_post_ingress_compaction(
        self, client, collection_name, expected_by_pk, output_fields, before_checkpoint, observer, budget
    ):
        # Automatic coverage is mandatory BEFORE the manual request; it cannot
        # be rescued by a post-ingress rewrite (the observer is already stopped).
        observer.assert_active_writing_coverage()
        compact_id = client.compact(collection_name, timeout=budget.remaining())
        budget.remaining()
        integrity._log_compaction_integrity_evidence(
            "continuous_post_ingress_compaction_requested",
            collection=collection_name,
            manual_job=compact_id,
            before_checkpoint=integrity._compaction_integrity_checkpoint_audit(before_checkpoint),
        )
        integrity._wait_for_compaction_integrity_checkpoint(
            client,
            collection_name,
            expected_rows=len(expected_by_pk),
            before_checkpoint=before_checkpoint,
            require_new_inputs=False,
            transition_policy="lineage",
            required_task_types={"MixCompaction"},
            expected_storage_version=3,
            timeout=budget.remaining(CONTINUOUS_FINAL_CHECKPOINT_TIMEOUT),
            require_exact_segment_rows=False,
            remaining_timeout=budget.remaining,
        )
        checkpoint, _, validation = integrity._assert_compaction_integrity_fenced_dataset(
            client,
            collection_name,
            expected_by_pk,
            output_fields,
            DataType.VARCHAR,
            expected_storage_version=3,
            timeout=budget.remaining(CONTINUOUS_FINAL_CHECKPOINT_TIMEOUT),
            require_exact_segment_rows=False,
            remaining_timeout=budget.remaining,
        )
        edges = integrity._assert_compaction_integrity_graph_transition(
            before_checkpoint, checkpoint, require_new_inputs=False
        )
        integrity._log_compaction_integrity_evidence(
            "continuous_post_ingress_compaction_validated",
            collection=collection_name,
            manual_job=compact_id,
            round_edges=[{"source": source, "target": target} for source, target in sorted(edges)],
            data_validation=validation,
            **integrity._compaction_integrity_checkpoint_audit(checkpoint),
        )

        client.release_collection(collection_name, timeout=budget.remaining())
        client.load_collection(collection_name, timeout=budget.remaining())
        reload_checkpoint, _, reload_validation = integrity._assert_compaction_integrity_fenced_dataset(
            client,
            collection_name,
            expected_by_pk,
            output_fields,
            DataType.VARCHAR,
            expected_storage_version=3,
            timeout=budget.remaining(CONTINUOUS_FINAL_CHECKPOINT_TIMEOUT),
            require_exact_segment_rows=False,
            remaining_timeout=budget.remaining,
        )
        return checkpoint, validation, reload_checkpoint, reload_validation

    @pytest.mark.tags(CaseLabel.L3)
    def test_continuous_mixed_ingress_auto_compaction_preserves_all_rows_and_fields(
        self,
        request,
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
        oracle = _ContinuousOracle()
        stop_event = threading.Event()
        budget = _ContinuousBudget(CONTINUOUS_CASE_TIMEOUT, stop_event)
        failure_recorder = _ContinuousFailureRecorder(request.node.nodeid)
        progress = _ContinuousProgress(request.node.nodeid)
        heartbeat_stop = threading.Event()
        heartbeat_thread = threading.Thread(
            target=progress.heartbeat_loop,
            args=(heartbeat_stop,),
            name="continuous-integrity-heartbeats",
            daemon=True,
        )
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
                budget.wait(10)

                output_fields, schema = self._create_compaction_integrity_collection(
                    client,
                    collection_name,
                    DataType.VARCHAR,
                    include_text=True,
                    include_struct_array=True,
                    include_test_sequence_id=True,
                    remaining_timeout=budget.remaining,
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

                self._seed_l0_delete_candidates(client, collection_name, output_fields, oracle, budget)
                observer = _ContinuousLifecycleObserver(
                    client,
                    collection_name,
                    oracle,
                    ingress_done,
                    stop_event,
                    checkpoint_queue,
                    config_committed_at,
                    budget,
                    failure_recorder,
                )
                observer_thread = threading.Thread(
                    target=observer.run,
                    args=(observer_stop,),
                    name="continuous-integrity-observer",
                    daemon=True,
                )
                observer_thread.start()
                heartbeat_thread.start()
                failure_phase = "await_workers"
                try:
                    with _continuous_executor(stop_event, checkpoint_queue, failure_recorder) as executor:
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
                                budget,
                                failure_recorder,
                                progress,
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
                                budget,
                                failure_recorder,
                                progress,
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
                            budget,
                            failure_recorder,
                        )
                        _continuous_wait_for_futures(producer_futures, budget, failure_recorder)
                        ingress_done.set()
                        _continuous_wait_for_futures([validator_future], budget, failure_recorder)
                        validated_prefixes = validator_future.result()
                    failure_phase = "final_validation"
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
                        timeout=budget.remaining(CONTINUOUS_FINAL_CHECKPOINT_TIMEOUT),
                        require_exact_segment_rows=False,
                        remaining_timeout=budget.remaining,
                    )
                    final_checkpoint, _, final_validation = integrity._assert_compaction_integrity_fenced_dataset(
                        client,
                        collection_name,
                        expected_by_pk,
                        output_fields,
                        DataType.VARCHAR,
                        expected_storage_version=3,
                        timeout=budget.remaining(CONTINUOUS_FINAL_CHECKPOINT_TIMEOUT),
                        require_exact_segment_rows=False,
                        remaining_timeout=budget.remaining,
                    )

                    observer.observe_once()
                    observer_stop.set()
                    observer_thread.join(timeout=10)
                    if observer.error is not None:
                        raise observer.error
                    assert not observer_thread.is_alive(), "lifecycle observer did not stop"
                    failure_phase = "post_ingress_validation"
                    post_checkpoint, post_validation, reload_checkpoint, reload_validation = (
                        self._verify_post_ingress_compaction(
                            client, collection_name, expected_by_pk, output_fields, final_checkpoint, observer, budget
                        )
                    )
                    budget.remaining()
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
                        post_compaction_validation=post_validation,
                        reload_validation=reload_validation,
                        mix_closures=list(observer.mix_closures.values()),
                        l0_closures=list(observer.l0_closures.values()),
                        final_checkpoint=integrity._compaction_integrity_checkpoint_audit(final_checkpoint),
                        post_compaction_checkpoint=integrity._compaction_integrity_checkpoint_audit(post_checkpoint),
                        reload_checkpoint=integrity._compaction_integrity_checkpoint_audit(reload_checkpoint),
                    )
                except BaseException as error:
                    # The observer remains active through final validation. A
                    # stopped budget must not replace its recorded first error.
                    failure_recorder.record(error, "coordinator", phase=failure_phase)
                    failure_recorder.raise_primary(error)
                finally:
                    heartbeat_stop.set()
                    heartbeat_thread.join(timeout=2)
                    ingress_done.set()
                    stop_event.set()
                    observer_stop.set()
                    observer_thread.join(timeout=10)


def _continuous_import_test_fields():
    return integrity._compaction_integrity_output_fields(
        include_struct_array=True,
        include_text=True,
        include_bm25_control=False,
        include_test_sequence_id=True,
    )


@pytest.mark.tags("CompactionIntegrityUnit")
def test_continuous_integrity_unit_import_adapter_preserves_oracle_boundary():
    output_fields = _continuous_import_test_fields()
    samples = {}
    for logical_pk in range(1, 1000):
        sequence = logical_pk + 1000
        row, raw_expected = integrity._build_compaction_integrity_row(
            "continuous_import_adapter_unit",
            logical_pk,
            sequence,
            DataType.VARCHAR,
            include_text=True,
            include_struct_array=True,
            test_sequence_id=sequence,
        )
        struct_value = row[integrity.COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD]
        sparse_value = row["nullable_sparse_vector"]
        if struct_value is None:
            samples.setdefault("null_struct", (logical_pk, sequence, row, raw_expected))
        elif struct_value:
            samples.setdefault("populated_struct", (logical_pk, sequence, row, raw_expected))
        if sparse_value is None:
            samples.setdefault("null_sparse", (logical_pk, sequence, row, raw_expected))
        else:
            samples.setdefault("populated_sparse", (logical_pk, sequence, row, raw_expected))
        if len(samples) == 4:
            break

    assert set(samples) == {"null_struct", "populated_struct", "null_sparse", "populated_sparse"}
    for logical_pk, sequence, row, raw_expected in samples.values():
        original_row = integrity._canonical_compaction_integrity_row(row, output_fields, DataType.VARCHAR)
        original_expected = integrity._canonical_compaction_integrity_row(raw_expected, output_fields, DataType.VARCHAR)
        writer_row, canonical_expected = _continuous_import_row(row, raw_expected, output_fields)
        _, mutation_expected = _continuous_row("continuous_import_adapter_unit", logical_pk, sequence, output_fields)

        assert integrity._canonical_compaction_integrity_row(row, output_fields, DataType.VARCHAR) == original_row
        assert (
            integrity._canonical_compaction_integrity_row(raw_expected, output_fields, DataType.VARCHAR)
            == original_expected
        )
        assert canonical_expected == mutation_expected == original_expected
        assert (
            integrity._canonical_compaction_integrity_row(writer_row, output_fields, DataType.VARCHAR) == original_row
        )
        for field_name in (integrity.COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD, "nullable_sparse_vector"):
            if row[field_name] is None:
                assert writer_row[field_name] is None
        action = _ContinuousAction(1, "import", 1, "import", row["id"], canonical_expected)
        oracle = _ContinuousOracle()
        oracle.commit([action])
        assert oracle.snapshot(1)[0][row["id"]] == canonical_expected


@pytest.mark.tags("CompactionIntegrityUnit")
def test_continuous_integrity_unit_import_adapter_converts_int8_and_keeps_inputs():
    output_fields = integrity._compaction_integrity_output_fields(
        include_struct_array=False,
        include_text=True,
        include_bm25_control=False,
        include_test_sequence_id=True,
    )
    logical_pk = 3
    sequence = 1003
    row, raw_expected = integrity._build_compaction_integrity_row(
        "continuous_import_int8_unit",
        logical_pk,
        sequence,
        DataType.VARCHAR,
        include_text=True,
        include_struct_array=False,
        test_sequence_id=sequence,
    )
    before = integrity._canonical_compaction_integrity_row(row, output_fields, DataType.VARCHAR)
    writer_row, canonical_expected = _continuous_import_row(row, raw_expected, output_fields)
    assert isinstance(writer_row["int8_vector"], np.ndarray)
    expected_vector = np.frombuffer(
        integrity._compaction_integrity_int8_vector(logical_pk, sequence, 17), dtype=np.int8
    ).tolist()
    assert writer_row["int8_vector"].tolist() == expected_vector
    assert integrity._canonical_compaction_integrity_row(row, output_fields, DataType.VARCHAR) == before
    assert len(canonical_expected) == len(output_fields)


@pytest.mark.tags("CompactionIntegrityUnit")
def test_continuous_integrity_unit_coordinator_failure_precedes_executor_cancellation():
    stop_event = threading.Event()
    checkpoint_queue = queue.Queue()
    budget = _ContinuousBudget(10, stop_event)
    recorder = _ContinuousFailureRecorder("coordinator-order-unit")
    worker_started = threading.Event()
    cancellation_seen = threading.Event()
    original = ValueError("safe coordinator failure")

    def worker():
        worker_started.set()
        assert stop_event.wait(5)
        try:
            budget.remaining()
        except RuntimeError as error:
            cancellation_seen.set()
            recorder.record(error, "mutation_a", phase="mutation")

    with pytest.raises(ValueError) as raised:
        with _continuous_executor(stop_event, checkpoint_queue, recorder) as executor:
            future = executor.submit(worker)
            assert worker_started.wait(5)
            raise original
    future.result()
    assert raised.value is original
    assert cancellation_seen.is_set()
    assert recorder.primary()[0] is original
    assert recorder.primary()[2] == "coordinator"


@pytest.mark.tags("CompactionIntegrityUnit")
@pytest.mark.parametrize("stage", ["checkpoint", "dataset"])
def test_continuous_integrity_unit_final_validation_preserves_observer_failure(monkeypatch, stage):
    module = sys.modules[__name__]
    original = ValueError("safe observer failure during final validation")
    trigger = threading.Event()
    observer_ready = threading.Event()
    observers = []
    config = SimpleNamespace(
        endpoint="unit",
        read_config=lambda _key: SimpleNamespace(value=None),
        preserve_config=lambda _key: nullcontext(),
        set_config=lambda *_args: SimpleNamespace(mod_revision=1),
    )
    monkeypatch.setattr(module, "MilvusEtcdConfigController", lambda **_kwargs: nullcontext(config))
    monkeypatch.setattr(_ContinuousBudget, "wait", lambda *_args: None)
    monkeypatch.setattr(integrity, "_log_compaction_integrity_evidence", lambda *_args, **_kwargs: None)
    expected = dict.fromkeys(range(CONTINUOUS_EPOCHS * CONTINUOUS_EXPECTED_LIVE_ROWS_PER_EPOCH), {})
    monkeypatch.setattr(
        _ContinuousOracle,
        "action_count",
        property(lambda _self: CONTINUOUS_EPOCHS * CONTINUOUS_SEQUENCE_SPAN_PER_EPOCH),
    )
    monkeypatch.setattr(_ContinuousOracle, "snapshot", lambda *_args: (expected, set()))

    def observe(observer):
        observers.append(observer)
        observer_ready.set()
        assert trigger.wait(5)
        raise original

    # Exercise the real observer.run: it records, poisons, and cancels the budget.
    monkeypatch.setattr(_ContinuousLifecycleObserver, "observe_once", observe)

    def final_validation(current_stage, **kwargs):
        if current_stage == stage:
            assert observer_ready.wait(5)
            trigger.set()
            assert observers[0].stop_event.wait(5)
            kwargs["remaining_timeout"]()
            pytest.fail("the final validator must observe cancellation")
        return {}

    monkeypatch.setattr(
        integrity,
        "_wait_for_compaction_integrity_checkpoint",
        lambda *_args, **kwargs: final_validation("checkpoint", **kwargs),
    )
    monkeypatch.setattr(
        integrity,
        "_assert_compaction_integrity_fenced_dataset",
        lambda *_args, **kwargs: final_validation("dataset", **kwargs),
    )
    case = TestMilvusClientContinuousStreamingDataIntegrity()
    monkeypatch.setattr(case, "_client", lambda: object())
    monkeypatch.setattr(case, "_create_compaction_integrity_collection", lambda *_args, **_kwargs: ([], object()))
    monkeypatch.setattr(case, "_seed_l0_delete_candidates", lambda *_args: None)
    monkeypatch.setattr(case, "_run_mutation_lane", lambda *_args: None)
    monkeypatch.setattr(case, "_run_import_lane", lambda *_args: None)
    monkeypatch.setattr(case, "_run_prefix_validator", lambda *_args: [0, 1, 2])
    with pytest.raises(ValueError) as raised:
        case.test_continuous_mixed_ingress_auto_compaction_preserves_all_rows_and_fields(
            SimpleNamespace(node=SimpleNamespace(nodeid="observer-final-unit")),
            "unit-minio",
            "unit-bucket",
            "unit-etcd",
            2379,
            "unit-root",
            "",
            "",
        )
    assert raised.value is original
    assert observers[0].error is original
    assert observers[0].failure_recorder.primary()[0] is original


@pytest.mark.tags("CompactionIntegrityUnit")
def test_continuous_integrity_unit_first_worker_failure_wins_and_reraises_original():
    recorder = _ContinuousFailureRecorder("continuous-case-unit")
    original = RuntimeError("safe client failure detail")
    secondary = TimeoutError("coordinator observed cancellation")

    assert recorder.record(original, "import", epoch=2, phase="writer_body")
    assert not recorder.record(secondary, "coordinator", phase="await_workers")
    with pytest.raises(RuntimeError, match="safe client failure detail") as raised:
        recorder.raise_primary(secondary)
    assert raised.value is original


@pytest.mark.tags("CompactionIntegrityUnit")
def test_continuous_integrity_unit_worker_polling_surfaces_recorded_failure_without_waiting():
    recorder = _ContinuousFailureRecorder("continuous-case-unit")
    original = RuntimeError("safe worker failure")
    recorder.record(original, "import", epoch=1, phase="import")
    pending = Future()
    budget = _ContinuousBudget(60, threading.Event())

    with pytest.raises(RuntimeError, match="safe worker failure") as raised:
        _continuous_wait_for_futures([pending], budget, recorder)

    assert raised.value is original


@pytest.mark.tags("CompactionIntegrityUnit")
def test_continuous_integrity_unit_writer_exit_failure_does_not_replace_body_failure(tmp_path, monkeypatch):
    event_path = tmp_path / "events.jsonl"
    monkeypatch.setenv("DATA_INTEGRITY_DIAGNOSTICS_PATH", str(event_path))
    recorder = _ContinuousFailureRecorder("continuous-case-unit")
    body_error = ValueError("safe append failure detail")
    exit_error = RuntimeError("safe writer exit failure detail")

    class BrokenWriter:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            # Evidence must exist before SDK cleanup can fail or hang.
            primary = json.loads(event_path.read_text().splitlines()[0])
            assert primary["phase"] == "writer_body"
            assert primary["error_reason"] == "invalid value"
            assert primary["error_stack"][-1]["function"] == (
                "test_continuous_integrity_unit_writer_exit_failure_does_not_replace_body_failure"
            )
            raise exit_error

    with pytest.raises(RuntimeError) as cleanup:
        with _continuous_writer_context(BrokenWriter(), recorder, "import", 1):
            raise body_error
    assert cleanup.value is exit_error
    # The outer handler sees the same exception again; do not duplicate it.
    recorder.record(exit_error, "import", epoch=1, phase="worker")
    with pytest.raises(ValueError, match="safe append failure detail") as raised:
        recorder.raise_primary(exit_error)
    assert raised.value is body_error
    events = [json.loads(line) for line in event_path.read_text().splitlines()]
    assert [(event["event"], event["phase"]) for event in events] == [
        ("continuous_primary_failure", "writer_body"),
        ("continuous_secondary_failure", "writer_exit"),
    ]


@pytest.mark.tags("CompactionIntegrityUnit")
def test_continuous_integrity_unit_secondary_errors_are_bounded(tmp_path, monkeypatch):
    event_path = tmp_path / "events.jsonl"
    monkeypatch.setenv("DATA_INTEGRITY_DIAGNOSTICS_PATH", str(event_path))
    recorder = _ContinuousFailureRecorder("continuous-case-unit")
    primary = ValueError("primary")
    recorder.record(primary, "import")
    recorder.record(RuntimeError("continuous integrity case stopped"), "mutation_a")
    for _ in range(20):
        recorder.record(RuntimeError("secondary"), "import", phase="writer_exit")
    events = [json.loads(line) for line in event_path.read_text().splitlines()]
    assert len(events) == 9
    assert sum(event["event"] == "continuous_primary_failure" for event in events) == 1
    assert recorder.primary()[0] is primary


@pytest.mark.tags("CompactionIntegrityUnit")
def test_continuous_integrity_unit_emits_writer_body_error_before_exit():
    recorder = _ContinuousFailureRecorder("continuous-case-unit")

    class FakeWriter:
        primary_phase_during_exit = None

        def __enter__(self):
            return self

        def __exit__(self, _exc_type, _exc, _traceback):
            primary = recorder.primary()
            self.primary_phase_during_exit = primary[4] if primary else None
            return False

    writer = FakeWriter()
    original = RuntimeError("safe append failure")
    with pytest.raises(RuntimeError, match="safe append failure") as raised:
        with _continuous_writer_context(writer, recorder, "import", 1):
            raise original
    assert writer.primary_phase_during_exit == "writer_body"
    with pytest.raises(RuntimeError, match="safe append failure") as reraised:
        recorder.raise_primary(RuntimeError("coordinator cancellation"))
    assert reraised.value is original


@pytest.mark.tags("CompactionIntegrityUnit")
def test_continuous_integrity_unit_heartbeat_is_throttled_and_safe_after_deadline(monkeypatch):
    events = []
    monkeypatch.setattr(
        sys.modules[__name__], "emit_data_integrity_event", lambda event, **kwargs: events.append(event)
    )

    class ExpiredBudget:
        deadline = time.monotonic() - 1

        def remaining(self, *_args, **_kwargs):
            raise RuntimeError("budget method must not be called from diagnostics")

    progress = _ContinuousProgress("continuous-case-unit")
    oracle = _ContinuousOracle()
    progress.emit("import", 1, "writer", ExpiredBudget(), oracle, event="continuous_heartbeat")
    progress.emit("import", 1, "writer", ExpiredBudget(), oracle, event="continuous_heartbeat")
    assert len(events) == 1
    assert events[0]["remaining_budget_seconds"] == 0
