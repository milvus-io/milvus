import struct
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from milvus_client import test_milvus_client_data_integrity as integrity
from pymilvus import MilvusException


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
