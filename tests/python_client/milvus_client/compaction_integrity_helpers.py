import json
import struct
from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from milvus_client import test_milvus_client_data_integrity as integrity
from pymilvus import CompactionTaskState, CompactionType, MilvusClient, MilvusException, SegmentState
from pymilvus.client.types import Plan, SegmentInfo

# Requires the observability SDK; collected only by the explicit unit entry.
pytestmark = pytest.mark.tags("CompactionIntegrityUnit")


@pytest.mark.parametrize(
    "state,expected_state,category",
    [
        (CompactionTaskState.Unknown, "unknown", "unknown"),
        (CompactionTaskState.Executing, "executing", "running"),
        (CompactionTaskState.Pipelining, "pipelining", "running"),
        (CompactionTaskState.Completed, "completed", "success"),
        (CompactionTaskState.Failed, "failed", "failed"),
        (CompactionTaskState.Timeout, "timeout", "failed"),
        (CompactionTaskState.Analyzing, "analyzing", "running"),
        (CompactionTaskState.Indexing, "indexing", "running"),
        (CompactionTaskState.Cleaned, "cleaned", "success"),
        (CompactionTaskState.MetaSaved, "meta_saved", "running"),
        (CompactionTaskState.Statistic, "statistic", "running"),
    ],
)
def test_task_snapshot_normalizes_sdk_enum_states(state, expected_state, category):
    task = Plan(
        [10, 11],
        20,
        plan_id=1,
        trigger_id=2,
        targets=[20, 21],
        compaction_type=CompactionType.MixCompaction,
        state=state,
        failure_reason="rewrite failed" if category == "failed" else "",
    )
    snapshot = integrity._snapshot_compaction_integrity_tasks(SimpleNamespace(plans=[task]))
    assert snapshot == {
        1: {
            "task_id": 1,
            "trigger_id": 2,
            "type": "MixCompaction",
            "state": expected_state,
            "failure_reason": task.failure_reason,
            "sources": (10, 11),
            "targets": (20, 21),
        }
    }
    if category == "unknown":
        with pytest.raises(AssertionError, match="unknown states"):
            integrity._assert_compaction_integrity_task_snapshot(snapshot)
        return
    integrity._assert_compaction_integrity_task_snapshot(snapshot)
    assert (expected_state in integrity.COMPACTION_INTEGRITY_RUNNING_TASK_STATES) == (category == "running")
    assert bool(integrity._compaction_integrity_failed_tasks(snapshot)) == (category == "failed")
    assert bool(integrity._compaction_integrity_successful_new_tasks(None, {"tasks": snapshot})) == (
        category == "success"
    )


@pytest.mark.parametrize("compaction_type", list(CompactionType))
def test_task_snapshot_keeps_sdk_enum_type_names_in_audit(compaction_type):
    task = Plan([10], 20, plan_id=1, compaction_type=compaction_type, state=CompactionTaskState.Completed)
    snapshot = integrity._snapshot_compaction_integrity_tasks(SimpleNamespace(plans=[task]))
    assert json.loads(json.dumps(snapshot))["1"]["type"] == compaction_type.name
    assert (
        integrity._compaction_integrity_successful_new_tasks(
            None, {"tasks": snapshot}, task_types={compaction_type.name}
        )
        == snapshot
    )
    assert (snapshot[1]["type"] == integrity.COMPACTION_INTEGRITY_BUMP_TASK_TYPE) == (
        compaction_type == CompactionType.BumpSchemaVersionCompaction
    )


def test_checkpoint_waits_for_sdk_enum_tasks_and_accepts_recovery(monkeypatch):
    source = SegmentInfo(10, 1, "c", 100, True, SegmentState.Dropped, 1, 2)
    target = SegmentInfo(20, 1, "c", 100, True, SegmentState.Flushed, 1, 2, compaction_from=[10])
    serving = SegmentInfo(20, 1, "c", 100, True, SegmentState.Sealed, 1, 2)
    failed = Plan(
        [10],
        -1,
        plan_id=1,
        compaction_type=CompactionType.MixCompaction,
        state=CompactionTaskState.Cleaned,
        failure_reason="previous attempt failed",
    )
    states = [
        CompactionTaskState.Pipelining,
        CompactionTaskState.Executing,
        CompactionTaskState.Analyzing,
        CompactionTaskState.Indexing,
        CompactionTaskState.MetaSaved,
        CompactionTaskState.Statistic,
        *([CompactionTaskState.Completed] * 3),
    ]
    client = Mock()
    client.list_segments.return_value = [source, target]
    client.list_loaded_segments.return_value = [serving]
    client.list_compaction_tasks.side_effect = [
        SimpleNamespace(
            plans=[failed, Plan([10], 20, plan_id=2, compaction_type=CompactionType.MixCompaction, state=state)]
        )
        for state in states
    ]
    monkeypatch.setattr(integrity, "time", SimpleNamespace(time=lambda: 0, sleep=Mock()))
    checkpoint = integrity._wait_for_compaction_integrity_checkpoint(
        client,
        "c",
        expected_rows=100,
        before_checkpoint={"all": {}, "active": {}, "tasks": {}},
        required_task_types={"MixCompaction"},
        expected_storage_version=2,
    )
    assert client.list_compaction_tasks.call_count == len(states)
    assert set(checkpoint["active"]) == set(checkpoint["serving"]) == {20}
    assert set(integrity._compaction_integrity_failed_tasks(checkpoint["tasks"])) == {1}
    assert set(integrity._compaction_integrity_successful_new_tasks(None, checkpoint)) == {2}


@pytest.mark.parametrize("pk,ts", [(1000, 1), (30999, 30), (32768, 3), (39999, 3)])
def test_float_fingerprints_remain_distinct_after_float32_encoding(pk, ts):
    # Cover array, both top-level vector profiles, and every nested element offset.
    for field_id, count in [(10, 4), (12, 16), (23, 16), (34, 4), (36, 64)]:
        values = [integrity._compaction_integrity_float32(pk, ts, field_id, index) for index in range(count)]
        encoded = [struct.pack("<f", value) for value in values]
        assert len(set(encoded)) == count
        assert [struct.unpack("<f", value)[0] for value in encoded] == values
        swapped = list(encoded)
        swapped[0], swapped[1] = swapped[1], swapped[0]
        assert b"".join(encoded) != b"".join(swapped)
    integrity._validate_compaction_integrity_float32_range(pk, ts)


def test_float_fingerprint_bounds_are_explicit():
    assert integrity._compaction_integrity_float32(0, 0, 0) == 0.0
    assert integrity._compaction_integrity_float32(65535, 0, 255) == 2**24 - 1
    for args in [(65536, 0, 0), (65535, 32, 0), (65535, 0, 256), (65535, 0, 255, 1), (-1, 0, 0)]:
        with pytest.raises(AssertionError, match="exact integer range"):
            integrity._compaction_integrity_float32(*args)


@pytest.mark.parametrize("ingress_type", ["insert", "import"])
def test_round_range_preflight_happens_before_any_ingress(ingress_type):
    client, ingress = Mock(), Mock()
    case = integrity.TestMilvusClientCompactionDataIntegrity()
    with pytest.raises(AssertionError, match="exact integer range"):
        case._run_compaction_integrity_rounds(client, "c", [], integrity.DataType.INT64, 2, 100, ingress_type, ingress)
    client.assert_not_called()
    assert not client.mock_calls
    ingress.assert_not_called()


@pytest.mark.parametrize("bad_score", [float("nan"), float("inf"), -float("inf")])
@pytest.mark.parametrize("source", ["actual", "baseline", "initial", "cross_field"])
def test_bm25_non_finite_scores_are_reported_after_all_queries(monkeypatch, bad_score, source):
    tokens = {"one": 1, "two": 2}
    baseline = (
        None
        if source in {"initial", "cross_field"}
        else {"one": (1, bad_score if source == "baseline" else 1.0), "two": (2, 1.0)}
    )
    fields = ["base", "added"] if source == "cross_field" else ["base"]
    client = Mock()

    def search(**kwargs):
        return [
            [
                {
                    "id": tokens[token],
                    "distance": bad_score if token == "one" and source != "baseline" else 1.0,
                }
            ]
            for token in kwargs["data"]
        ]

    client.search.side_effect = search
    monkeypatch.setattr(integrity, "COMPACTION_INTEGRITY_BM25_QUERY_BATCH_SIZE", 1)
    evidence = Mock()
    monkeypatch.setattr(integrity, "_log_compaction_integrity_evidence", evidence)
    with pytest.raises(pytest.fail.Exception, match="non_finite"):
        integrity._assert_compaction_integrity_bm25(client, "c", tokens, fields, baseline)
    assert client.search.call_count == len(tokens) * len(fields)
    summary = evidence.call_args.kwargs
    assert summary["score_delta_summary"] == {}
    assert all(sample["token"] == "one" for sample in summary["samples"])


def test_bm25_finite_scores_keep_existing_tolerance(monkeypatch):
    client = Mock()
    client.search.return_value = [[{"id": 1, "distance": 1.0 + 0.5e-6}]]
    monkeypatch.setattr(integrity, "_log_compaction_integrity_evidence", Mock())
    baseline = {"one": (1, 1.0)}
    integrity._assert_compaction_integrity_bm25(client, "c", {"one": 1}, ["base", "added"], baseline)
    client.search.return_value = [[{"id": 1, "distance": 1.0 + 2e-6}]]
    with pytest.raises(pytest.fail.Exception, match="baseline_score_mismatch"):
        integrity._assert_compaction_integrity_bm25(client, "c", {"one": 1}, ["base"], baseline)


@pytest.mark.parametrize("failure", [pytest.fail.Exception("bm25"), AssertionError("cell")])
@pytest.mark.parametrize("handoff", [False, True])
def test_fence_checks_frontier_after_validation_failure(monkeypatch, failure, handoff):
    checkpoints = [1, 2, 2, 2] if handoff else [1, 1]
    wait = Mock(side_effect=checkpoints)
    monkeypatch.setattr(integrity, "_wait_for_compaction_integrity_checkpoint", wait)
    monkeypatch.setattr(integrity, "_compaction_integrity_checkpoint_audit", lambda checkpoint: {})
    monkeypatch.setattr(integrity, "_log_compaction_integrity_evidence", Mock())
    monkeypatch.setattr(integrity, "_assert_compaction_integrity_dataset", Mock(return_value={}))
    # Keep actual checkpoint shapes while isolating the polling from the server.
    states = [{"active": {}, "serving": {}, "signature": value} for value in checkpoints]
    wait.side_effect = states
    monkeypatch.setattr(
        integrity, "_compaction_integrity_frontier_signature", lambda checkpoint: checkpoint["signature"]
    )
    validator = Mock(side_effect=[failure, "ok"])
    if handoff:
        _, result, _ = integrity._assert_compaction_integrity_fenced_dataset(
            Mock(), "c", {}, [], integrity.DataType.INT64, 2, additional_validator=validator
        )
        assert result == "ok"
        assert wait.call_count == 4
    else:
        with pytest.raises(type(failure)) as caught:
            integrity._assert_compaction_integrity_fenced_dataset(
                Mock(), "c", {}, [], integrity.DataType.INT64, 2, additional_validator=validator
            )
        assert caught.value is failure
        assert wait.call_count == 2


@pytest.mark.parametrize("failure", [KeyboardInterrupt(), SystemExit(), pytest.skip.Exception("skip")])
def test_fence_does_not_swallow_control_flow(monkeypatch, failure):
    wait = Mock(return_value={})
    monkeypatch.setattr(integrity, "_wait_for_compaction_integrity_checkpoint", wait)
    monkeypatch.setattr(integrity, "_assert_compaction_integrity_dataset", Mock(side_effect=failure))
    with pytest.raises(type(failure)):
        integrity._assert_compaction_integrity_fenced_dataset(Mock(), "c", {}, [], integrity.DataType.INT64, 2)
    assert wait.call_count == 1


@pytest.mark.parametrize(
    "field_name,valid_value,invalid_value,error_type",
    [
        ("int8_value", 1, 128, "error"),
        ("int16_value", 1, 32768, "error"),
        ("int32_value", 1, 2**31, "error"),
        ("varchar_payload", "valid", 123, "AttributeError"),
        ("binary_vector", b"\x01\x02", [b"\x01", b"\x02"], "AssertionError"),
        ("float_array", [1.0], ["invalid"], "ValueError"),
        ("sparse_vector", {1: 1.0}, [1.0], "AttributeError"),
    ],
)
def test_unencodable_cells_do_not_abort_dataset_audit(monkeypatch, field_name, valid_value, invalid_value, error_type):
    """Malformed cells must not hide other damaged fields, rows, or later batches."""
    fields = ["id", field_name, "int64_value"]
    rows = [{"id": pk, field_name: valid_value, "int64_value": 10} for pk in range(1, 5)]
    expected = {
        row["id"]: integrity._canonical_compaction_integrity_row(row, fields, integrity.DataType.INT64) for row in rows
    }
    client = Mock()
    iterator = client.query_iterator.return_value
    iterator.next.side_effect = [
        [dict(rows[0], **{field_name: invalid_value, "int64_value": 11}), rows[1]],
        [dict(rows[2], **{field_name: invalid_value}), dict(rows[3], int64_value=12)],
        [],
    ]
    evidence = Mock()
    monkeypatch.setattr(integrity, "_log_compaction_integrity_evidence", evidence)
    with pytest.raises(AssertionError, match="data corruption detected in complete dataset"):
        integrity._assert_compaction_integrity_dataset(client, "c", expected, fields, integrity.DataType.INT64)
    assert iterator.next.call_count == 3
    iterator.close.assert_called_once_with()
    assert evidence.call_args.args == ("data_integrity_dataset_corruption",)
    summary = evidence.call_args.kwargs
    assert summary["retrieved_rows"] == 4
    assert summary["validated_cell_count"] == 12
    assert summary["query_batch_count"] == summary["corrupted_batch_count"] == 2
    assert summary["corrupted_row_count"] == summary["affected_primary_key_count"] == 3
    assert summary["corrupted_cell_count"] == 4
    assert summary["issue_counts"] == {"canonical_encoding_error": 2, "canonical_value_mismatch": 2}
    assert summary["field_mismatch_counts"] == {field_name: 2, "int64_value": 2}
    assert summary["expected_pk_digest"] == summary["retrieved_pk_digest"]
    errors = [sample for sample in summary["samples"] if sample["issue"] == "canonical_encoding_error"]
    assert [sample["pk"] for sample in errors] == [1, 3]
    assert all(sample["error_type"] == error_type for sample in errors)
    assert all(sample["actual_type"] == type(invalid_value).__name__ for sample in errors)


def test_encoding_error_samples_and_messages_are_bounded(monkeypatch):
    fields = ["id", "float_array"]
    expected = {
        pk: integrity._canonical_compaction_integrity_row(
            {"id": pk, "float_array": [1.0]}, fields, integrity.DataType.INT64
        )
        for pk in range(4)
    }
    monkeypatch.setattr(integrity, "COMPACTION_INTEGRITY_CORRUPTION_SAMPLE_LIMIT", 2)
    summary = integrity._compaction_integrity_batch_corruption_summary(
        1,
        [{"id": pk, "float_array": ["x" * 1000]} for pk in expected],
        expected,
        fields,
        integrity.DataType.INT64,
        set(),
    )
    assert summary["issue_counts"] == {"canonical_encoding_error": 4}
    assert summary["corrupted_cell_count"] == 4
    assert summary["sample_count"] == summary["samples_truncated"] == 2
    assert all(len(sample["error_message"]) <= 512 for sample in summary["samples"])


@pytest.mark.parametrize("failure", [KeyboardInterrupt(), SystemExit(), pytest.skip.Exception("skip")])
def test_cell_encoding_does_not_swallow_control_flow(monkeypatch, failure):
    monkeypatch.setattr(integrity, "_canonical_compaction_integrity_cell", Mock(side_effect=failure))
    with pytest.raises(type(failure)):
        integrity._compaction_integrity_batch_corruption_summary(
            1, [{"id": 1}], {1: {"id": b"expected"}}, ["id"], integrity.DataType.INT64, set()
        )


@pytest.mark.parametrize("failure", [AssertionError("cell corruption"), pytest.fail.Exception("bm25"), None])
def test_post_checkpoint_failure_preserves_validation_error(monkeypatch, failure):
    before = {"active": {}, "serving": {}}
    checkpoint_error = AssertionError("checkpoint timed out")
    wait = Mock(side_effect=[before, checkpoint_error])
    evidence = Mock()
    monkeypatch.setattr(integrity, "_wait_for_compaction_integrity_checkpoint", wait)
    monkeypatch.setattr(integrity, "_compaction_integrity_checkpoint_audit", lambda checkpoint: checkpoint)
    monkeypatch.setattr(integrity, "_log_compaction_integrity_evidence", evidence)
    monkeypatch.setattr(integrity, "_assert_compaction_integrity_dataset", Mock(side_effect=failure, return_value={}))
    expected_error = checkpoint_error if failure is None else failure
    with pytest.raises(type(expected_error)) as caught:
        integrity._assert_compaction_integrity_fenced_dataset(Mock(), "c", {}, [], integrity.DataType.INT64, 2)
    assert caught.value is expected_error
    if failure is not None:
        assert caught.value.__cause__ is checkpoint_error
    assert wait.call_count == 2
    assert evidence.call_args.args == ("fenced_validation_checkpoint_failed",)
    assert evidence.call_args.kwargs["validation_error"] == (None if failure is None else repr(failure))
    assert evidence.call_args.kwargs["checkpoint_error"] == repr(checkpoint_error)
    assert evidence.call_args.kwargs["frontier_verified"] is False


@pytest.fixture
def collection_creation(monkeypatch):
    case = integrity.TestMilvusClientCompactionDataIntegrity()
    case.tear_down_collection_names = []
    case.create_schema = Mock(return_value=(Mock(), True))
    case.create_struct_field_schema = Mock(return_value=(Mock(), True))
    client = Mock()
    client.prepare_index_params.return_value = Mock()
    for method in ("prepare_index_params", "create_collection", "create_index", "load_collection"):
        getattr(client, method).__qualname__ = method
    clock = SimpleNamespace(now=0.0)

    def advance(seconds):
        clock.now += seconds

    monkeypatch.setattr(integrity, "time", SimpleNamespace(monotonic=lambda: clock.now, sleep=advance))
    monkeypatch.setattr(integrity, "_log_compaction_integrity_evidence", Mock())
    return case, client, clock


@pytest.mark.parametrize("rejections", [0, 2])
@pytest.mark.parametrize("include_struct_array", [False, True])
def test_v3_adoption_budget_does_not_limit_index_or_load(collection_creation, rejections, include_struct_array):
    case, client, clock = collection_creation
    client.create_collection.side_effect = [
        MilvusException(code=1100, message="TEXT field requires StorageV3") for _ in range(rejections)
    ] + [None]

    def slow_phase(*args, **kwargs):
        assert kwargs["timeout"] > 5
        assert case.tear_down_collection_names == ["c"]
        clock.now += integrity.COMPACTION_INTEGRITY_V3_ADOPTION_TIMEOUT + 1

    client.create_index.side_effect = slow_phase
    client.load_collection.side_effect = slow_phase
    case._create_compaction_integrity_collection(
        client, "c", integrity.DataType.INT64, True, include_struct_array=include_struct_array
    )
    assert client.create_collection.call_count == rejections + 1
    for call in client.create_collection.call_args_list:
        assert 0 < call.args[7] <= 5  # Test wrapper forwards timeout positionally.
        assert call.args[9] is None  # SDK must not implicitly index/load inside the retry.
    client.create_index.assert_called_once()
    client.load_collection.assert_called_once()


@pytest.mark.parametrize("stage", ["create_collection", "create_index", "load_collection"])
def test_creation_failures_are_not_retried_as_config_adoption(collection_creation, stage):
    case, client, _ = collection_creation
    getattr(client, stage).side_effect = MilvusException(code=1, message="unrelated operation failure")
    with pytest.raises(AssertionError):
        case._create_compaction_integrity_collection(client, "c", integrity.DataType.INT64, True)
    assert client.create_collection.call_count == 1
    assert client.create_index.call_count == (stage != "create_collection")
    assert client.load_collection.call_count == (stage == "load_collection")
    assert case.tear_down_collection_names == ([] if stage == "create_collection" else ["c"])


def test_v3_adoption_still_expires_without_index_or_load(collection_creation):
    case, client, clock = collection_creation
    client.create_collection.side_effect = MilvusException(code=1100, message="TEXT field requires StorageV3")
    with pytest.raises(AssertionError, match="Proxy did not adopt StorageV3 within 30s"):
        case._create_compaction_integrity_collection(client, "c", integrity.DataType.INT64, True)
    assert clock.now == integrity.COMPACTION_INTEGRITY_V3_ADOPTION_TIMEOUT
    client.create_index.assert_not_called()
    client.load_collection.assert_not_called()
    assert not case.tear_down_collection_names


def test_non_text_creation_keeps_combined_sdk_path(collection_creation):
    case, client, _ = collection_creation
    case._create_compaction_integrity_collection(client, "c", integrity.DataType.INT64, False)
    client.create_collection.assert_called_once()
    assert client.create_collection.call_args.args[7] > 5
    assert client.create_collection.call_args.args[9] is client.prepare_index_params.return_value
    client.create_index.assert_not_called()
    client.load_collection.assert_not_called()


def v2_ddl_checkpoint(stage):
    segments = {
        segment_id: {
            "segment_id": segment_id,
            "state": "Flushed" if segment_id == stage + 1 else "Dropped",
            "compaction_from": () if segment_id == 1 else (segment_id - 1,),
            "storage_version": 2,
            "num_rows": 3,
            "is_sorted": True,
        }
        for segment_id in range(1, stage + 2)
    }
    active = {stage + 1: segments[stage + 1]}
    return {
        "all": segments,
        "active": active,
        "serving": {stage + 1: dict(segments[stage + 1], state="Sealed")},
        "tasks": {
            task_id: {
                "task_id": task_id,
                "type": "MixCompaction",
                "state": "cleaned",
                "failure_reason": "",
                "sources": (task_id,),
                "targets": (task_id + 1,),
            }
            for task_id in range(1, stage + 1)
        },
        "storage_versions": {2},
    }


@pytest.mark.parametrize("dim", [32, 4096])
def test_v2_projection_dataset_is_deterministic_exact_and_case_local(dim):
    rng_state = integrity.np.random.get_state()
    build = integrity._build_compaction_integrity_v2_projection_row
    row = build("run", 8193, 2, dim)
    repeat = build("run", 8193, 2, dim)
    other = build("run", 8194, 2, dim)

    def canonical(value):
        return integrity._canonical_compaction_integrity_row(value, list(value), integrity.DataType.VARCHAR)

    assert canonical(row) == canonical(repeat)
    assert canonical(row)["float16_vector"] != canonical(other)["float16_vector"]
    assert canonical(row)["float_vector"] != canonical(other)["float_vector"]
    assert len(row["float16_vector"]) == 16 and len(row["binary_vector"]) == 2
    assert len(row["float_vector"]) == dim
    assert row["float_vector"][:3] == [8193.0, 2.0, 7.0]
    assert integrity.np.isfinite(row["float_vector"]).all()
    assert integrity.np.array_equal(row["float_vector"], integrity.np.asarray(row["float_vector"], dtype="float32"))
    after = integrity.np.random.get_state()
    assert rng_state[0] == after[0] and rng_state[2:] == after[2:]
    assert integrity.np.array_equal(rng_state[1], after[1])
    assert integrity.COMPACTION_INTEGRITY_VECTOR_DIM == 16
    assert integrity.COMPACTION_INTEGRITY_DDL_ROWS_PER_BATCH > 0


@pytest.mark.parametrize("pk,dim", [(-1, 32), (2**20, 32), (0, 16), (0, 32769)])
def test_v2_projection_generator_rejects_ambiguous_fingerprints_or_dimensions(pk, dim):
    with pytest.raises(AssertionError):
        integrity._build_compaction_integrity_v2_projection_row("run", pk, 1, dim)


@pytest.mark.parametrize("fault", [None, "narrow_replay", "wide_replay", "last_coordinate"])
def test_v2_projection_verifier_detects_row_preserving_damage_and_scans_every_batch(monkeypatch, fault):
    rows = [integrity._build_compaction_integrity_v2_projection_row("run", pk, 1, 32) for pk in range(8)]
    fields = [name for name in rows[0] if name != integrity.COMPACTION_INTEGRITY_V2_PROJECTION_DROP_FIELD]
    expected = {
        row["id"]: integrity._canonical_compaction_integrity_row(row, fields, integrity.DataType.VARCHAR)
        for row in rows
    }
    for row in rows:
        row.pop(integrity.COMPACTION_INTEGRITY_V2_PROJECTION_DROP_FIELD)
    if fault in {"narrow_replay", "wide_replay"}:
        field = "float16_vector" if fault == "narrow_replay" else "float_vector"
        for index in range(4, 8):
            rows[index][field] = deepcopy(rows[index - 4][field])
    elif fault == "last_coordinate":
        field = "float_vector"
        rows[-1][field][-1] += 1
    client, iterator, evidence = Mock(), Mock(), Mock()
    iterator.next.side_effect = [rows[:2], rows[2:4], rows[4:6], rows[6:], []]
    client.query_iterator.return_value = iterator
    monkeypatch.setattr(integrity, "_log_compaction_integrity_evidence", evidence)

    def verify():
        return integrity._assert_compaction_integrity_dataset(client, "c", expected, fields, integrity.DataType.VARCHAR)

    if fault is None:
        verify()
    else:
        with pytest.raises(AssertionError, match="data corruption detected in complete dataset"):
            verify()
        summary = next(
            call.kwargs for call in evidence.call_args_list if call.args[0] == "data_integrity_dataset_corruption"
        )
        assert summary["corrupted_row_count"] == (1 if fault == "last_coordinate" else 4)
        assert summary["field_mismatch_counts"] == {field: summary["corrupted_row_count"]}
    assert iterator.next.call_count == 5
    iterator.close.assert_called_once()
    assert len(expected) == len(rows) == len({row["id"] for row in rows})


@pytest.mark.parametrize("original_value", [None, b"true"])
@pytest.mark.parametrize(
    "failure_at", [None, "insert", "insert_count", "flush", "baseline", "drop", "mix_wait", "retained_data", "index"]
)
def test_v2_projection_workload_orders_verification_and_restores_config(monkeypatch, original_value, failure_at):
    case, client = integrity.TestMilvusClientCompactionDataIntegrity(), Mock()
    case._client = Mock(return_value=client)
    schema = MilvusClient.create_schema(auto_id=False, enable_dynamic_field=False)
    case.create_schema = Mock(return_value=(schema, True))
    case.prepare_index_params = Mock(return_value=(MilvusClient.prepare_index_params(), True))
    case.create_collection, case.create_index, case.load_collection, case.drop_collection = (
        Mock(),
        Mock(),
        Mock(),
        Mock(),
    )
    trace, stored, iterators = [], [], []
    dropped_field = integrity.COMPACTION_INTEGRITY_V2_PROJECTION_DROP_FIELD
    dropped = False
    mix_ready = False
    config_key = integrity.COMPACTION_INTEGRITY_BUMP_SCHEMA_VERSION_CONFIG

    def fail(where):
        if failure_at == where:
            raise RuntimeError(f"injected {where}")

    class ConfigController:
        value = original_value

        def read_config(self, key):
            assert key == config_key
            return SimpleNamespace(value=self.value, mod_revision=2)

        def set_config(self, key, value, *, settle_after_write):
            assert key == config_key and value == "false" and settle_after_write is True
            self.value = b"false"
            return self.read_config(key)

        @contextmanager
        def preserve_config(self, key):
            try:
                yield self
            finally:
                self.value = original_value

    controller = ConfigController()

    def insert(_client, _collection, rows):
        fail("insert")
        stored.extend(deepcopy(rows))
        trace.append("insert")
        return {"insert_count": len(rows) - (failure_at == "insert_count")}, True

    def flush(*args):
        fail("flush")
        assert len(stored) == 5
        trace.append("flush")

    def drop(*args, **kwargs):
        nonlocal dropped
        fail("drop")
        assert trace[-1] == "checkpoint_before"
        assert trace.count("scan_before") == 1
        assert kwargs["field_name"] == dropped_field
        dropped = True
        trace.append("drop")

    def compact(*args):
        trace.append("compact")
        return -1, True  # A later successful automatic Mix is also accepted.

    def mix_wait(*args, **kwargs):
        nonlocal mix_ready
        fail("mix_wait")
        assert dropped and kwargs["required_task_types"] == {"MixCompaction"}
        assert kwargs["expected_storage_version"] == 2 and kwargs["transition_policy"] == "lineage"
        mix_ready = True
        trace.append("mix_ready")

    def checkpoint(*args, **kwargs):
        assert controller.value == b"false" and kwargs["expected_storage_version"] == 2
        if dropped:
            assert mix_ready
        trace.append("checkpoint_after" if dropped else "checkpoint_before")
        result = v2_ddl_checkpoint(int(dropped))
        for segment in [*result["all"].values(), *result["serving"].values()]:
            segment.update(num_rows=5, partition_id=10, insert_channel="projection-channel")
        return result

    def query_iterator(*args, **kwargs):
        fail("retained_data" if dropped else "baseline")
        assert trace[-1] == ("checkpoint_after" if dropped else "checkpoint_before")
        trace.append("scan_after" if dropped else "scan_before")
        assert (dropped_field in kwargs["output_fields"]) is not dropped
        rows = [{name: row[name] for name in kwargs["output_fields"]} for row in stored]
        iterator = Mock()
        iterator.next.side_effect = [rows[:2], rows[2:4], rows[4:], []]
        iterators.append(iterator)
        return iterator

    def snapshot(*args):
        return {
            "schema_version": int(dropped),
            "fields": {field.name: {} for field in schema.fields if not dropped or field.name != dropped_field},
        }

    case.insert, case.flush = Mock(side_effect=insert), Mock(side_effect=flush)
    case.drop_collection_field, case.compact = Mock(side_effect=drop), Mock(side_effect=compact)
    case._wait_for_ddl_schema_transition = Mock(side_effect=mix_wait)
    client.query_iterator.side_effect = query_iterator
    client.list_indexes.return_value = ["float16_vector", "float_vector"] + (
        [dropped_field] if failure_at == "index" else []
    )
    evidence = Mock()
    monkeypatch.setattr(integrity, "_wait_for_compaction_integrity_checkpoint", checkpoint)
    monkeypatch.setattr(integrity, "_compaction_integrity_schema_snapshot", snapshot)
    monkeypatch.setattr(integrity, "_log_compaction_integrity_evidence", evidence)
    monkeypatch.setattr(integrity, "_log_compaction_integrity_checkpoint", Mock())
    monkeypatch.setattr(integrity, "COMPACTION_INTEGRITY_V2_PROJECTION_ROWS", 5)
    monkeypatch.setattr(integrity, "COMPACTION_INTEGRITY_V2_PROJECTION_DIM", 32)
    monkeypatch.setattr(integrity, "COMPACTION_INTEGRITY_V2_PROJECTION_INSERT_BATCH", 2)
    monkeypatch.setattr(integrity, "COMPACTION_INTEGRITY_KEEP_DDL_COLLECTION", False)
    config = {"controller": controller, "storage_version": 2}

    def run():
        case.test_v2_drop_vector_mix_compaction_preserves_projected_rows(config)

    if failure_at in {"insert_count", "index"}:
        with pytest.raises(AssertionError):
            run()
    elif failure_at:
        with pytest.raises(RuntimeError, match=f"injected {failure_at}"):
            run()
    else:
        run()
        assert trace == [
            "insert",
            "insert",
            "insert",
            "flush",
            "checkpoint_before",
            "scan_before",
            "checkpoint_before",
            "drop",
            "compact",
            "mix_ready",
            "checkpoint_after",
            "scan_after",
            "checkpoint_after",
        ]
        assert [row["explicit_test_ts"] for row in stored] == [1, 1, 2, 2, 3]
        committed = [
            call.kwargs["expected_total"]
            for call in evidence.call_args_list
            if call.args[0] == "v2_projection_ingress_mutation_committed"
        ]
        assert committed == [2, 4, 5]
        for iterator in iterators:
            assert iterator.next.call_count == 4
            iterator.close.assert_called_once()
    if failure_at in {"insert", "insert_count"}:
        assert not any(call.args[0] == "v2_projection_ingress_mutation_committed" for call in evidence.call_args_list)
        case.flush.assert_not_called()
    fields = {field.name: field for field in schema.fields}
    assert fields["float_vector"].params["dim"] == 32
    assert fields["float16_vector"].params["dim"] == 16
    assert not schema.enable_dynamic_field
    assert list(fields)[-3:] == [dropped_field, "float16_vector", "float_vector"]
    assert controller.value == original_value
    case.drop_collection.assert_called_once()


@pytest.mark.parametrize(
    "defect,message",
    [
        (None, None),
        ("bump", "bump disabled"),
        ("v3", "non-V2"),
        ("unchanged_source", "rewrite all"),
        ("failed_mix", "successful new MixCompaction"),
        ("old_mix", "successful new MixCompaction"),
        ("sort_only", "successful new MixCompaction"),
        ("unadopted_mix", "successful new MixCompaction"),
        ("no_lineage", "no observable compaction transition"),
    ],
)
def test_v2_ddl_requires_actual_adopted_mix_rewrite_without_bump(defect, message):
    before, after = v2_ddl_checkpoint(0), v2_ddl_checkpoint(1)
    if defect == "bump":
        after["tasks"][1]["type"] = integrity.COMPACTION_INTEGRITY_BUMP_TASK_TYPE
    elif defect == "v3":
        after["storage_versions"] = {3}
    elif defect == "unchanged_source":
        after["active"][1] = before["active"][1]
    elif defect == "failed_mix":
        after["tasks"][1]["failure_reason"] = "failed before cleanup"
    elif defect == "old_mix":
        before["tasks"] = deepcopy(after["tasks"])
    elif defect == "sort_only":
        after["tasks"][1]["type"] = "SortCompaction"
    elif defect == "unadopted_mix":
        after["tasks"][1]["targets"] = (999,)
    elif defect == "no_lineage":
        after["all"][2]["compaction_from"] = ()
    if message:
        with pytest.raises(AssertionError, match=message):
            integrity._assert_compaction_integrity_v2_ddl_checkpoint(after, before)
    else:
        assert integrity._assert_compaction_integrity_v2_ddl_checkpoint(before) == set()
        assert integrity._assert_compaction_integrity_v2_ddl_checkpoint(after, before) == {(1, 2)}


@pytest.mark.parametrize("storage_version", [2, 3])
def test_ddl_schema_wait_keeps_v3_defaults_and_supports_v2_mix(monkeypatch, storage_version):
    case, client = integrity.TestMilvusClientCompactionDataIntegrity(), Mock()
    case.wait_for_schema_version_consistency = Mock(return_value=True)
    client.describe_collection.return_value = {"schema_version": 1}
    wait = Mock(return_value="checkpoint")
    monkeypatch.setattr(integrity, "_wait_for_compaction_integrity_checkpoint", wait)
    extra = {"expected_storage_version": 2, "required_task_types": {"MixCompaction"}} if storage_version == 2 else {}
    assert case._wait_for_ddl_schema_transition(client, "c", 3, {}, "lineage", 1, timeout=17, **extra) == "checkpoint"
    case.wait_for_schema_version_consistency.assert_called_once_with(client, "c", timeout=17)
    assert wait.call_args.kwargs["expected_storage_version"] == storage_version
    assert wait.call_args.kwargs["required_task_types"] == (
        {"MixCompaction"} if storage_version == 2 else integrity.COMPACTION_INTEGRITY_SCHEMA_REWRITE_TASK_TYPES
    )
    assert wait.call_args.kwargs["timeout"] == 17


@pytest.mark.parametrize("v2_profile", [False, True])
def test_ddl_ingress_v2_profile_omits_v3_fields_without_changing_v3_defaults(monkeypatch, v2_profile):
    case = integrity.TestMilvusClientCompactionDataIntegrity()
    case.insert = Mock(return_value=({"insert_count": 3}, True))
    case.flush = Mock()
    monkeypatch.setattr(integrity, "COMPACTION_INTEGRITY_DDL_BATCHES", 1)
    monkeypatch.setattr(integrity, "COMPACTION_INTEGRITY_DDL_ROWS_PER_BATCH", 3)
    fields = integrity._compaction_integrity_output_fields(False, not v2_profile, not v2_profile)
    kwargs = {"include_text": False, "include_bm25_control": False} if v2_profile else {}
    expected, tokens = case._ingest_ddl_compaction_integrity_dataset(
        Mock(), "c", fields, integrity.DataType.VARCHAR, **kwargs
    )
    assert len(expected) == 3
    assert len(tokens) == (0 if v2_profile else 3)
    for row in case.insert.call_args.args[2]:
        assert (integrity.COMPACTION_INTEGRITY_TEXT_FIELD in row) is not v2_profile
        assert (integrity.COMPACTION_INTEGRITY_BM25_TEXT_FIELD in row) is not v2_profile
        assert integrity.COMPACTION_INTEGRITY_STRUCT_ARRAY_FIELD not in row
        assert all(field in row for field in integrity.COMPACTION_INTEGRITY_TOP_LEVEL_VECTOR_FIELDS)
    case.flush.assert_called_once()


def test_v2_ddl_drop_selection_is_reproducible_and_does_not_change_global_rng():
    candidates = integrity.COMPACTION_INTEGRITY_V2_DDL_DROP_CANDIDATES
    fields = [
        "id",
        "explicit_test_ts",
        "dynamic_payload",
        integrity.COMPACTION_INTEGRITY_ADDED_DEFAULT_FIELD,
        *integrity.COMPACTION_INTEGRITY_TOP_LEVEL_VECTOR_FIELDS,
        *candidates,
    ]
    state = integrity.random.getstate()
    selections = {
        seed: integrity._select_compaction_integrity_v2_ddl_drop_field(fields, fields, seed)
        for seed in map(str, range(64))
    }
    assert integrity.random.getstate() == state
    assert set(selections.values()) == set(candidates)
    for seed, selected in selections.items():
        assert integrity._select_compaction_integrity_v2_ddl_drop_field(fields[::-1], fields[::-1], seed) == selected


@pytest.mark.parametrize("field_name", integrity.COMPACTION_INTEGRITY_V2_DDL_DROP_CANDIDATES)
def test_v2_ddl_drop_selection_requires_a_schema_and_oracle_field(field_name):
    fields = ["id", integrity.COMPACTION_INTEGRITY_ADDED_DEFAULT_FIELD, field_name]
    select = integrity._select_compaction_integrity_v2_ddl_drop_field
    assert select(fields, fields, "seed") == field_name
    for schema, output in [(fields[:-1], fields), (fields, fields[:-1])]:
        with pytest.raises(AssertionError, match="existing data-bearing ordinary field"):
            select(schema, output, "seed")


@pytest.mark.parametrize("original_value", [None, b"true"])
@pytest.mark.parametrize(
    "failure_at",
    [
        None,
        "config",
        "create",
        "add",
        "C1_add_field",
        "drop_existing",
        "C2_drop_existing_field",
        "drop_added",
        "C3_drop_added_field",
        "readable_y_at_c2",
        "readable_y_at_c3",
        "readable_x_at_c3",
        "missing_x_at_c2",
    ],
)
def test_v2_ddl_workload_validates_each_stage_and_restores_bump_config(monkeypatch, original_value, failure_at):
    case, client = integrity.TestMilvusClientCompactionDataIntegrity(), Mock()
    case._client = Mock(return_value=client)
    stage = 0
    operations, validations, waits = [], [], []
    field_name = integrity.COMPACTION_INTEGRITY_ADDED_DEFAULT_FIELD
    fields = ["id", "int64_value", "varchar_payload"]
    expected = {"pk": {"id": b"pk", "int64_value": b"integer", "varchar_payload": b"string"}}
    original_expected = deepcopy(expected)
    drop_seed = "v2-ddl-unit"
    existing_field = integrity._select_compaction_integrity_v2_ddl_drop_field(fields, fields, drop_seed)
    stage_names = ["C0_initial", "C1_add_field", "C2_drop_existing_field", "C3_drop_added_field"]
    monkeypatch.setenv("MILVUS_COMPACTION_INTEGRITY_DDL_DROP_SEED", drop_seed)

    def fail(where):
        if failure_at == where:
            raise RuntimeError(f"injected {where}")

    class ConfigController:
        value = original_value
        revision = 1

        def read_config(self, key):
            assert key == integrity.COMPACTION_INTEGRITY_BUMP_SCHEMA_VERSION_CONFIG
            return SimpleNamespace(value=self.value, mod_revision=self.revision)

        def set_config(self, key, value, *, settle_after_write):
            assert value == "false" and settle_after_write is True
            self.value, self.revision = b"false", 2
            fail("config")
            return self.read_config(key)

        @contextmanager
        def preserve_config(self, key):
            try:
                yield self
            finally:
                self.value = original_value

    controller = ConfigController()

    def create(*args, **kwargs):
        fail("create")
        assert controller.value == b"false"
        assert kwargs == {"include_text": False, "include_bm25_control": False, "include_struct_array": False}
        return fields, None

    case._create_compaction_integrity_collection = Mock(side_effect=create)
    case._ingest_ddl_compaction_integrity_dataset = Mock(return_value=(expected, {}))
    case.drop_collection = Mock()
    case.compact = Mock(return_value=(-1, True))  # Automatic Mix is valid even if manual returns zero plans.
    case.wait_for_schema_version_consistency = Mock(return_value=True)

    def add(*args, **kwargs):
        nonlocal stage
        fail("add")
        assert args[2] == field_name
        operations.append(("add", field_name))
        assert kwargs == {"nullable": True, "default_value": integrity.COMPACTION_INTEGRITY_ADDED_DEFAULT_VALUE}
        stage = 1

    def drop(*args, **kwargs):
        nonlocal stage
        fail("drop_existing" if stage == 1 else "drop_added")
        assert kwargs["field_name"] == (existing_field if stage == 1 else field_name)
        operations.append(("drop", kwargs["field_name"]))
        stage += 1

    case.add_collection_field, case.drop_collection_field = Mock(side_effect=add), Mock(side_effect=drop)
    client.describe_collection.side_effect = lambda _: {"schema_version": stage}

    def query(*args, **kwargs):
        visible = (
            (failure_at == "readable_y_at_c2" and stage == 2 and kwargs["filter"] == f"exists {existing_field}")
            or (failure_at == "readable_y_at_c3" and stage == 3 and kwargs["filter"] == f"exists {existing_field}")
            or (failure_at == "readable_x_at_c3" and stage == 3 and kwargs["filter"] == f"exists {field_name}")
        )
        return [{"count(*)": 1 if visible else 0}]

    client.query.side_effect = query

    def schema_snapshot(*args):
        current_fields = [name for name in fields if stage < 2 or name != existing_field]
        if stage in (1, 2) and not (failure_at == "missing_x_at_c2" and stage == 2):
            current_fields.append(field_name)
        return {"schema_version": stage, "fields": dict.fromkeys(current_fields)}

    def wait(*args, **kwargs):
        assert kwargs["expected_storage_version"] == 2
        waits.append(kwargs)
        if stage:
            assert kwargs["required_task_types"] == {"MixCompaction"}
            assert kwargs["transition_policy"] == "lineage"
            fail(stage_names[stage])
        return v2_ddl_checkpoint(stage)

    def verify(_client, _collection, oracle, output_fields, _pk_type, **kwargs):
        assert kwargs["expected_storage_version"] == 2
        assert controller.value == b"false"
        validations.append((stage, deepcopy(oracle), list(output_fields)))
        if kwargs.get("additional_validator"):
            kwargs["additional_validator"]()
        return v2_ddl_checkpoint(stage), None, {}

    monkeypatch.setattr(integrity, "COMPACTION_INTEGRITY_DDL_BATCHES", 1)
    monkeypatch.setattr(integrity, "COMPACTION_INTEGRITY_DDL_ROWS_PER_BATCH", 1)
    monkeypatch.setattr(integrity, "COMPACTION_INTEGRITY_KEEP_DDL_COLLECTION", False)
    monkeypatch.setattr(integrity, "_compaction_integrity_schema_snapshot", schema_snapshot)
    monkeypatch.setattr(integrity, "_wait_for_compaction_integrity_checkpoint", wait)
    monkeypatch.setattr(integrity, "_assert_compaction_integrity_fenced_dataset", verify)
    checkpoint_log = Mock()
    monkeypatch.setattr(integrity, "_log_compaction_integrity_checkpoint", checkpoint_log)
    config = {"controller": controller, "storage_version": 2}
    if failure_at and failure_at.startswith("readable_"):
        with pytest.raises(AssertionError, match="still has readable values"):
            case.test_v2_add_drop_field_mix_compaction_preserves_all_rows_and_fields(config)
    elif failure_at == "missing_x_at_c2":
        with pytest.raises(AssertionError):
            case.test_v2_add_drop_field_mix_compaction_preserves_all_rows_and_fields(config)
    elif failure_at:
        with pytest.raises(RuntimeError, match=f"injected {failure_at}"):
            case.test_v2_add_drop_field_mix_compaction_preserves_all_rows_and_fields(config)
    else:
        case.test_v2_add_drop_field_mix_compaction_preserves_all_rows_and_fields(config)
        assert operations == [("add", field_name), ("drop", existing_field), ("drop", field_name)]
        assert [item[0] for item in validations] == [0, 1, 2, 3]
        default_bytes = integrity._canonical_compaction_integrity_cell(
            field_name, integrity.COMPACTION_INTEGRITY_ADDED_DEFAULT_VALUE, integrity.DataType.VARCHAR
        )
        remaining = {name: value for name, value in original_expected["pk"].items() if name != existing_field}
        assert [item[1] for item in validations] == [
            original_expected,
            {"pk": {**original_expected["pk"], field_name: default_bytes}},
            {"pk": {**remaining, field_name: default_bytes}},
            {"pk": remaining},
        ]
        retained_fields = [name for name in fields if name != existing_field]
        assert [item[2] for item in validations] == [
            fields,
            fields + [field_name],
            retained_fields + [field_name],
            retained_fields,
        ]
        assert case.compact.call_count == 3
        assert len(waits) == 4
        assert [call.kwargs["filter"] for call in client.query.call_args_list] == [
            f"exists {existing_field}",
            f"exists {existing_field}",
            f"exists {field_name}",
        ]
        assert [call.args[0] for call in checkpoint_log.call_args_list] == stage_names
        for call in checkpoint_log.call_args_list:
            assert call.kwargs["drop_seed"] == drop_seed
            assert call.kwargs["selected_existing_field"] == existing_field
        assert checkpoint_log.call_args_list[2].kwargs["dropped_fields"] == [existing_field]
        assert checkpoint_log.call_args_list[3].kwargs["dropped_fields"] == [existing_field, field_name]
    assert controller.value == original_value
    assert case.drop_collection.call_count == (failure_at not in {"config", "create"})
