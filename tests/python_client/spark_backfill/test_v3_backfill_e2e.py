import copy
import json
import time

import pyarrow as pa
import pytest
from common.common_type import CaseLabel
from pymilvus import DataType

from spark_backfill.backfill_helpers import (
    BackfillContractError,
    FunctionOutputIndexNotReadyError,
    StaleSchemaFenceMissingError,
    assert_commit_succeeded,
    assert_stale_schema_commit_rejected,
    compute_minhash_signatures,
    inspect_result_artifacts,
    make_backfill_rows,
    validate_v3_result,
    wait_for_index_ready,
    wait_for_visible_rows,
)
from spark_backfill.contracts import build_ground_truth

pytestmark = [
    pytest.mark.tags(CaseLabel.SparkBackfill),
    pytest.mark.spark_e2e,
    pytest.mark.spark_backfill_v3,
    pytest.mark.spark_backfill_core,
]
SOURCE_FIELDS = ("base_int", "base_float", "text", "vector")
TARGET_FIELDS = ("bf_score", "bf_label", "bf_vector")
VISIBLE_FIELDS = (*SOURCE_FIELDS, *TARGET_FIELDS)


def _source_by_pk(case):
    return {row["id"]: row for row in case.source_rows}


def _parquet_by_pk(rows):
    return {row["pk"]: row for row in rows}


def _visible_ground_truth(case, parquet_rows, mode, target_fields=TARGET_FIELDS):
    source = _source_by_pk(case)
    targets = build_ground_truth(source, _parquet_by_pk(parquet_rows), target_fields, mode)
    return {
        primary_key: {
            **{field: row[field] for field in SOURCE_FIELDS},
            **targets[primary_key],
        }
        for primary_key, row in source.items()
    }


def _validate_result_artifacts(
    case,
    job_result,
    result_uri,
    parquet_rows,
    matched_rows=None,
    *,
    target_fields=TARGET_FIELDS,
):
    assert job_result.succeeded, job_result.logs
    result = case.read_result(result_uri)
    case.write_local_evidence(job_result, "snapshot.json", case.snapshot.raw)
    case.write_local_evidence(job_result, "backfill-result.json", result)
    case.write_local_evidence(job_result, "objects.json", case.list_result_objects(result_uri))
    case.write_local_evidence(
        job_result,
        "artifacts.json",
        inspect_result_artifacts(case.minio_client, case.settings.minio_bucket, result),
    )
    validate_v3_result(
        result,
        collection_id=case.snapshot.collection_id,
        schema_version=case.snapshot.schema_version,
        source_rows=len(case.source_rows),
        backfill_rows=len(parquet_rows),
        matched_rows=len(parquet_rows) if matched_rows is None else matched_rows,
        target_fields=set(target_fields),
        current_manifest_versions=case.snapshot.manifest_versions,
    )
    return result


def _validate_commit_and_visibility(
    case,
    job_result,
    result_uri,
    parquet_rows,
    mode,
    matched_rows=None,
    *,
    target_fields=TARGET_FIELDS,
    expected_parquet_rows=None,
):
    result = _validate_result_artifacts(
        case,
        job_result,
        result_uri,
        parquet_rows,
        matched_rows,
        target_fields=target_fields,
    )
    status, commit = case.commit(result_uri)
    case.write_local_evidence(job_result, "commit-response.json", commit)
    assert status == 200, commit
    assert_commit_succeeded(commit, expected_segments=set(case.snapshot.segment_ids), expected_kind="v3")
    case.drop_snapshots_and_refresh()
    expected_rows = parquet_rows if expected_parquet_rows is None else expected_parquet_rows
    expected = _visible_ground_truth(case, expected_rows, mode, target_fields)
    visible_fields = (*SOURCE_FIELDS, *target_fields)
    wait_for_visible_rows(case.client, case.collection_name, expected, visible_fields)
    return result, commit


def _assert_manifest_commit_rejected(status, commit, expected_segments):
    assert status == 500
    assert commit["committed_segments"] == 0
    assert commit["failed_segments"] == len(expected_segments)
    statuses = commit["segment_statuses"]
    assert {int(item["segment_id"]) for item in statuses} == expected_segments
    assert all(not item.get("ok", False) for item in statuses)
    assert all("not greater than current" in item["reason"] for item in statuses)


def _run_stamped_backfill(case, case_id):
    parquet_rows = make_backfill_rows()
    parquet_uri = case.upload_parquet(case_id, parquet_rows)
    job_result, result_uri = case.run_backfill(case_id=case_id, parquet_uri=parquet_uri, mode="coalesce")
    result = _validate_result_artifacts(case, job_result, result_uri, parquet_rows, matched_rows=18)
    result_version = int(result["schemaVersion"])
    assert result_version == case.snapshot.schema_version
    return job_result, result_uri, result_version


def _assert_stale_schema_commit(case, job_result, result_uri, result_version):
    current_version = int(case.client.describe_collection(case.collection_name)["schema_version"])
    assert current_version > result_version

    status, commit = case.commit(result_uri)
    case.write_local_evidence(job_result, "commit-response.json", commit)
    assert_stale_schema_commit_rejected(
        status,
        commit,
        result_version=result_version,
        current_version=current_version,
    )


def _run_stale_schema_commit_case(case, case_id):
    job_result, result_uri, result_version = _run_stamped_backfill(case, case_id)

    case.client.add_collection_field(
        case.collection_name,
        f"{case_id.replace('-', '_')}_post_spark_field",
        DataType.FLOAT,
        nullable=True,
    )
    _assert_stale_schema_commit(case, job_result, result_uri, result_version)


def _wait_for_compaction(case, job_result, timeout=180):
    compaction_id = case.client.compact(case.collection_name)
    deadline = time.monotonic() + timeout
    state = ""
    while time.monotonic() < deadline:
        state = str(case.client.get_compaction_state(compaction_id))
        if "Completed" in state:
            break
        if "Failed" in state:
            pytest.fail(f"compaction {compaction_id} failed: {state}")
        time.sleep(2)
    else:
        pytest.fail(f"compaction {compaction_id} did not complete: {state}")
    case.write_local_evidence(job_result, "compaction.json", {"job_id": compaction_id, "state": state})
    return compaction_id, state


@pytest.mark.parametrize("mode", ["coalesce", "overwrite", "replace"])
def test_v3_backfill_modes_publish_and_become_visible(backfill_case_factory, mode):
    case = backfill_case_factory()
    parquet_rows = make_backfill_rows()
    parquet_uri = case.upload_parquet(mode, parquet_rows)

    job_result, result_uri = case.run_backfill(case_id=mode, parquet_uri=parquet_uri, mode=mode)

    _validate_commit_and_visibility(case, job_result, result_uri, parquet_rows, mode, matched_rows=18)


@pytest.mark.parametrize("mode", ["coalesce", "overwrite", "replace"])
def test_v3_explicit_null_semantics(backfill_case_factory, mode):
    case = backfill_case_factory()
    parquet_rows = make_backfill_rows(explicit_null_pk=0)
    parquet_uri = case.upload_parquet(f"explicit-null-{mode}", parquet_rows)

    job_result, result_uri = case.run_backfill(
        case_id=f"explicit-null-{mode}",
        parquet_uri=parquet_uri,
        mode=mode,
    )

    _validate_commit_and_visibility(case, job_result, result_uri, parquet_rows, mode, matched_rows=18)


def test_v3_online_minhash_function_output_builds_index_and_searches(backfill_case_factory):
    row_count = 3000
    num_hashes = 128
    field_name = "bf_minhash"
    case = backfill_case_factory(
        source_row_count=row_count,
        flush_batch_size=row_count,
        online_minhash_field=field_name,
    )
    assert case.snapshot.schema_version > 0

    signatures = compute_minhash_signatures(
        case.client,
        case.source_rows,
        field_name,
        num_hashes=num_hashes,
    )

    parquet_rows = [
        {
            "pk": primary_key,
            field_name: signatures[primary_key],
        }
        for primary_key in range(row_count)
    ]
    parquet_uri = case.upload_parquet(
        "online-minhash-function-output",
        parquet_rows,
        target_fields=(field_name,),
        target_field_types={field_name: pa.binary()},
    )
    job_result, result_uri = case.run_backfill(
        case_id="online-minhash-function-output",
        parquet_uri=parquet_uri,
        mode="replace",
    )
    _validate_result_artifacts(
        case,
        job_result,
        result_uri,
        parquet_rows,
        matched_rows=row_count,
        target_fields=(field_name,),
    )

    status, commit = case.commit(result_uri)
    case.write_local_evidence(job_result, "commit-response.json", commit)
    assert status == 200, commit
    assert_commit_succeeded(commit, expected_segments=set(case.snapshot.segment_ids), expected_kind="v3")
    case.drop_snapshots_and_refresh()

    index_info = wait_for_index_ready(
        case.client,
        case.collection_name,
        field_name,
        expected_rows=row_count,
    )
    case.write_local_evidence(job_result, "function-output-index.json", index_info)

    case.client.release_collection(case.collection_name)
    case.client.load_collection(case.collection_name)

    text_by_id = {int(row["id"]): row["text"] for row in case.source_rows}
    for expected_id in (0, row_count // 2, row_count - 1):
        hits = case.client.search(
            case.collection_name,
            data=[text_by_id[expected_id]],
            anns_field=field_name,
            limit=1,
            output_fields=["id"],
            search_params={"metric_type": "MHJACCARD", "params": {}},
        )[0]
        assert [hit["id"] for hit in hits] == [expected_id]
        assert hits[0]["distance"] == pytest.approx(1.0)


@pytest.mark.spark_backfill_known_gap
@pytest.mark.xfail(
    strict=True,
    raises=BackfillContractError,
    reason=(
        "spark-milvus MilvusScan read path does not support the 'minio://' scheme "
        "returned in the client snapshot s3_location; only s3:// / s3a:// are handled"
    ),
)
def test_v3_read_probe_reports_live_rows_and_search(backfill_case_factory):
    case = backfill_case_factory()

    parquet_rows = make_backfill_rows()
    parquet_uri = case.upload_parquet("read", parquet_rows)
    backfill_job, result_uri = case.run_backfill(case_id="read", parquet_uri=parquet_uri, mode="coalesce")
    assert backfill_job.succeeded, backfill_job.logs

    status, commit = case.commit(result_uri)
    case.write_local_evidence(backfill_job, "commit-response.json", commit)
    assert status == 200, commit
    assert_commit_succeeded(commit, expected_segments=set(case.snapshot.segment_ids), expected_kind="v3")

    case.drop_snapshots_and_refresh()

    job_result, probe = case.run_read_probe()
    assert job_result.succeeded, job_result.logs
    case.write_local_evidence(job_result, "read-probe.json", probe)

    expected_ids = sorted(row["id"] for row in case.source_rows)
    assert probe["count"] == len(expected_ids)
    assert probe["primaryKeys"] == expected_ids
    assert {"id", "base_int", "base_float", "text", "vector"} <= set(probe["schemaFields"])

    assert probe["projection"]["count"] == len(expected_ids)
    assert set(probe["projection"]["fields"]) == {"id", "base_float"}

    assert len(probe["sqlRows"]) == 1
    assert probe["sqlRows"][0]["total"] == len(expected_ids)
    assert probe["sqlRows"][0]["avg_float"] == pytest.approx(14.5)

    assert len(probe["topK"]) == 5
    assert [int(row["id"]) for row in probe["topK"]] == [0, 1, 2, 3, 4]


def test_parquet_extra_primary_key_does_not_expand_snapshot(backfill_case_factory):
    case = backfill_case_factory()
    parquet_rows = [
        {
            "pk": primary_key,
            "bf_score": float(primary_key),
            "bf_label": f"backfill-{primary_key}",
            "bf_vector": [float(primary_key)] * 4,
        }
        for primary_key in range(30)
    ]
    parquet_rows.append({"pk": 100, "bf_score": 100.0, "bf_label": "outside-snapshot", "bf_vector": [100.0] * 4})
    parquet_uri = case.upload_parquet("extra-pk", parquet_rows)

    job_result, result_uri = case.run_backfill(
        case_id="extra-pk",
        parquet_uri=parquet_uri,
        mode="overwrite",
    )

    result, _ = _validate_commit_and_visibility(
        case,
        job_result,
        result_uri,
        parquet_rows,
        "overwrite",
        matched_rows=30,
    )
    assert result["totalBackfillDataRows"] == 31
    assert result["totalMatchedRows"] == 30
    assert case.client.query(case.collection_name, filter="id == 100", output_fields=["id"]) == []


def test_rows_inserted_after_snapshot_are_not_backfilled(backfill_case_factory):
    case = backfill_case_factory()
    new_rows = [
        {
            "id": primary_key,
            "base_int": primary_key,
            "base_float": float(primary_key),
            "text": f"row-{primary_key}",
            "vector": [float(primary_key) + offset / 10.0 for offset in range(4)],
            "bf_score": -1000.0 - primary_key,
            "bf_label": f"inserted-after-snapshot-{primary_key}",
            "bf_vector": [float(primary_key) + 100.0] * 4,
        }
        for primary_key in range(30, 33)
    ]
    case.client.insert(case.collection_name, new_rows)
    case.client.flush(case.collection_name)
    parquet_rows = [
        {
            "pk": primary_key,
            "bf_score": float(primary_key),
            "bf_label": f"backfill-{primary_key}",
            "bf_vector": [float(primary_key)] * 4,
        }
        for primary_key in range(33)
    ]
    parquet_uri = case.upload_parquet("insert-after-snapshot", parquet_rows)

    job_result, result_uri = case.run_backfill(
        case_id="insert-after-snapshot",
        parquet_uri=parquet_uri,
        mode="overwrite",
    )

    _validate_result_artifacts(case, job_result, result_uri, parquet_rows, matched_rows=30)
    status, commit = case.commit(result_uri)
    case.write_local_evidence(job_result, "commit-response.json", commit)
    assert status == 200, commit
    assert_commit_succeeded(commit, expected_segments=set(case.snapshot.segment_ids), expected_kind="v3")
    case.drop_snapshots_and_refresh()
    expected = _visible_ground_truth(case, parquet_rows, "overwrite")
    for new_row in new_rows:
        expected[new_row["id"]] = {field: new_row[field] for field in VISIBLE_FIELDS}
    wait_for_visible_rows(case.client, case.collection_name, expected, VISIBLE_FIELDS)


def test_rows_deleted_after_snapshot_remain_deleted_after_backfill(backfill_case_factory):
    case = backfill_case_factory()
    deleted_primary_keys = {0, 10, 21}
    case.client.delete(
        case.collection_name,
        filter=f"id in {sorted(deleted_primary_keys)}",
    )
    case.client.flush(case.collection_name)
    assert (
        case.client.query(
            case.collection_name,
            filter=f"id in {sorted(deleted_primary_keys)}",
            output_fields=["id"],
        )
        == []
    )

    parquet_rows = make_backfill_rows()
    parquet_uri = case.upload_parquet("delete-after-snapshot", parquet_rows)
    job_result, result_uri = case.run_backfill(
        case_id="delete-after-snapshot",
        parquet_uri=parquet_uri,
        mode="overwrite",
    )

    _validate_result_artifacts(case, job_result, result_uri, parquet_rows, matched_rows=18)
    status, commit = case.commit(result_uri)
    case.write_local_evidence(job_result, "commit-response.json", commit)
    assert status == 200, commit
    assert_commit_succeeded(commit, expected_segments=set(case.snapshot.segment_ids), expected_kind="v3")
    case.drop_snapshots_and_refresh()
    expected = _visible_ground_truth(case, parquet_rows, "overwrite")
    for primary_key in deleted_primary_keys:
        expected.pop(primary_key)
    wait_for_visible_rows(case.client, case.collection_name, expected, VISIBLE_FIELDS)
    assert (
        case.client.query(
            case.collection_name,
            filter=f"id in {sorted(deleted_primary_keys)}",
            output_fields=["id"],
        )
        == []
    )


@pytest.mark.parametrize(
    ("case_id", "vector_type", "encode_as_json_string"),
    [
        ("vector-list-float32", pa.list_(pa.float32()), False),
        ("vector-json-array-string", pa.string(), True),
    ],
    ids=["list-float32", "json-array-string"],
)
def test_float_vector_supported_parquet_formats_publish_exact_values(
    backfill_case_factory,
    case_id,
    vector_type,
    encode_as_json_string,
):
    case = backfill_case_factory()
    parquet_rows = make_backfill_rows()
    expected_rows = copy.deepcopy(parquet_rows)
    if encode_as_json_string:
        for row in parquet_rows:
            row["bf_vector"] = json.dumps(row["bf_vector"])
    parquet_uri = case.upload_parquet(
        case_id,
        parquet_rows,
        vector_type=vector_type,
        target_fields=("bf_vector",),
    )

    job_result, result_uri = case.run_backfill(
        case_id=case_id,
        parquet_uri=parquet_uri,
        mode="overwrite",
    )

    _validate_commit_and_visibility(
        case,
        job_result,
        result_uri,
        parquet_rows,
        "overwrite",
        matched_rows=18,
        target_fields=("bf_vector",),
        expected_parquet_rows=expected_rows,
    )


def test_snapshot_after_add_field_rejects_new_target_field(backfill_case_factory):
    case = backfill_case_factory()
    case.client.add_collection_field(case.collection_name, "bf_new", DataType.FLOAT, nullable=True)
    rows = [{"pk": primary_key, "bf_new": float(primary_key)} for primary_key in range(30)]
    parquet_uri = case.upload_parquet("field-after-snapshot", rows, target_fields=("bf_new",))

    job_result, result_uri = case.run_backfill(
        case_id="field-after-snapshot",
        parquet_uri=parquet_uri,
        mode="replace",
    )

    assert not job_result.succeeded
    assert job_result.exit_code not in (None, 0)
    assert "not found in snapshot schema" in job_result.logs.lower()
    result_key = result_uri.split(f"s3a://{case.settings.minio_bucket}/", 1)[-1]
    objects = case.list_result_objects(result_uri)
    case.write_local_evidence(job_result, "snapshot.json", case.snapshot.raw)
    case.write_local_evidence(job_result, "objects.json", objects)
    assert result_key not in objects


def test_add_field_after_spark_rejects_nonzero_stale_schema_result(backfill_case_factory):
    case = backfill_case_factory()
    initial_version = int(case.client.describe_collection(case.collection_name)["schema_version"])
    case.client.add_collection_field(
        case.collection_name,
        "nonzero_pre_spark_field",
        DataType.FLOAT,
        nullable=True,
    )
    baseline_version = int(case.client.describe_collection(case.collection_name)["schema_version"])
    assert baseline_version > initial_version
    snapshot = case.create_snapshot()
    assert snapshot.schema_version == baseline_version
    assert snapshot.schema_version > 0

    _run_stale_schema_commit_case(case, "nonzero-schema-gap")


@pytest.mark.spark_backfill_known_gap
@pytest.mark.xfail(
    strict=True,
    raises=StaleSchemaFenceMissingError,
    reason=(
        "https://github.com/milvus-io/milvus/issues/51318: "
        "schema fencing is not in place for schema version = 0"
    ),
)
def test_add_field_after_spark_rejects_zero_version_stale_schema_result(backfill_case_factory):
    case = backfill_case_factory()
    live_version = int(case.client.describe_collection(case.collection_name)["schema_version"])
    assert case.snapshot.schema_version == 0
    assert live_version == case.snapshot.schema_version

    _run_stale_schema_commit_case(case, "zero-schema-gap")


@pytest.mark.parametrize("schema_mutation", ["add", "drop"])
def test_schema_change_after_snapshot_before_spark_rejects_stale_result(
    backfill_case_factory,
    schema_mutation,
):
    case = backfill_case_factory()
    initial_version = int(case.client.describe_collection(case.collection_name)["schema_version"])
    case.client.add_collection_field(
        case.collection_name,
        "schema_baseline_field",
        DataType.FLOAT,
        nullable=True,
    )
    baseline_version = int(case.client.describe_collection(case.collection_name)["schema_version"])
    assert baseline_version > initial_version
    snapshot = case.create_snapshot()
    assert snapshot.schema_version == baseline_version
    assert snapshot.schema_version > 0

    if schema_mutation == "add":
        case.client.add_collection_field(
            case.collection_name,
            "field_added_after_snapshot",
            DataType.FLOAT,
            nullable=True,
        )
    else:
        case.client.drop_collection_field(case.collection_name, "schema_baseline_field")

    job_result, result_uri, result_version = _run_stamped_backfill(
        case,
        f"{schema_mutation}-after-snapshot",
    )
    _assert_stale_schema_commit(case, job_result, result_uri, result_version)


def test_v3_rejects_duplicate_or_stale_manifest_commit(backfill_case_factory):
    case = backfill_case_factory()
    parquet_rows = make_backfill_rows()
    parquet_uri = case.upload_parquet("stale-version", parquet_rows)
    job_result, result_uri = case.run_backfill(
        case_id="stale-version",
        parquet_uri=parquet_uri,
        mode="coalesce",
    )
    result, _ = _validate_commit_and_visibility(
        case,
        job_result,
        result_uri,
        parquet_rows,
        "coalesce",
        matched_rows=18,
    )

    status, second_commit = case.commit(result_uri)
    case.write_local_evidence(job_result, "duplicate-commit-response.json", second_commit)
    expected_segments = set(case.snapshot.segment_ids)
    _assert_manifest_commit_rejected(status, second_commit, expected_segments)

    older_result = copy.deepcopy(result)
    for segment_id_raw, segment in older_result["segments"].items():
        segment_id = int(segment_id_raw)
        segment["version"] = case.snapshot.manifest_versions[segment_id]
        assert segment["version"] < result["segments"][segment_id_raw]["version"]
    older_result_uri = case.upload_result("older-manifest-version", older_result)

    older_status, older_commit = case.commit(older_result_uri)
    case.write_local_evidence(job_result, "older-commit-response.json", older_commit)
    _assert_manifest_commit_rejected(older_status, older_commit, expected_segments)


@pytest.mark.spark_backfill_compaction
def test_active_compaction_protection_preserves_snapshot_and_allows_commit(backfill_case_factory):
    case = backfill_case_factory(compaction_protection_seconds=1800)
    snapshot = case.snapshot
    snapshot_segment_ids = set(snapshot.segment_ids)
    assert len(snapshot_segment_ids) >= 2, "test requires multiple flushed segments"
    assert int(time.time()) < snapshot.compaction_expire_time

    parquet_rows = make_backfill_rows()
    parquet_uri = case.upload_parquet("active-compaction-protection", parquet_rows)
    job_result, result_uri = case.run_backfill(
        case_id="active-compaction-protection",
        parquet_uri=parquet_uri,
        mode="coalesce",
    )
    assert job_result.succeeded, job_result.logs

    assert int(time.time()) < snapshot.compaction_expire_time, "protection expired before compaction started"
    _wait_for_compaction(case, job_result)
    assert int(time.time()) < snapshot.compaction_expire_time, "protection expired before compaction completed"

    persistent_segments = case.client.list_persistent_segments(case.collection_name)
    case.write_local_evidence(
        job_result,
        "segments-after-protected-compaction.json",
        [vars(segment) for segment in persistent_segments],
    )
    current_segment_ids = {int(segment.segment_id) for segment in persistent_segments}
    assert snapshot_segment_ids.issubset(current_segment_ids)

    _validate_commit_and_visibility(case, job_result, result_uri, parquet_rows, "coalesce", matched_rows=18)


@pytest.mark.spark_backfill_compaction
def test_expired_protection_without_compaction_still_allows_commit(backfill_case_factory):
    case = backfill_case_factory(compaction_protection_seconds=5)
    case.client.alter_collection_properties(
        collection_name=case.collection_name,
        properties={"collection.autocompaction.enabled": "false"},
    )
    parquet_rows = make_backfill_rows()
    parquet_uri = case.upload_parquet("expired-no-compaction", parquet_rows)
    job_result, result_uri = case.run_backfill(
        case_id="expired-no-compaction",
        parquet_uri=parquet_uri,
        mode="coalesce",
    )
    assert job_result.succeeded, job_result.logs
    wait_seconds = max(0, case.snapshot.compaction_expire_time - int(time.time()) + 2)
    time.sleep(wait_seconds)

    persistent_segments = case.client.list_persistent_segments(case.collection_name)
    case.write_local_evidence(
        job_result,
        "segments-before-expired-commit.json",
        [vars(segment) for segment in persistent_segments],
    )
    current_segment_ids = {int(segment.segment_id) for segment in persistent_segments}
    assert set(case.snapshot.segment_ids).issubset(current_segment_ids), (
        "test requires the snapshot segments to remain uncompacted after protection expires"
    )

    _validate_commit_and_visibility(case, job_result, result_uri, parquet_rows, "coalesce", matched_rows=18)


@pytest.mark.spark_backfill_compaction
def test_expired_protection_with_compaction_rejects_old_result_and_new_snapshot_succeeds(backfill_case_factory):
    case = backfill_case_factory(compaction_protection_seconds=5)
    old_snapshot = case.snapshot
    assert len(old_snapshot.segment_ids) >= 2, "test requires multiple flushed segments"
    parquet_rows = make_backfill_rows()
    parquet_uri = case.upload_parquet("expired-with-compaction", parquet_rows)
    old_job, old_result_uri = case.run_backfill(
        case_id="expired-with-compaction",
        parquet_uri=parquet_uri,
        mode="coalesce",
    )
    _validate_result_artifacts(case, old_job, old_result_uri, parquet_rows, matched_rows=18)
    wait_seconds = max(0, old_snapshot.compaction_expire_time - int(time.time()) + 2)
    time.sleep(wait_seconds)

    _wait_for_compaction(case, old_job)

    case.write_local_evidence(old_job, "snapshot-before-compaction.json", old_snapshot.raw)
    persistent_segments = case.client.list_persistent_segments(case.collection_name)
    case.write_local_evidence(
        old_job,
        "segments-after-compaction.json",
        [vars(segment) for segment in persistent_segments],
    )
    current_segment_ids = {int(segment.segment_id) for segment in persistent_segments}
    old_segment_ids = set(old_snapshot.segment_ids)
    changed_old_ids = old_segment_ids - current_segment_ids
    unchanged_old_ids = old_segment_ids & current_segment_ids
    assert changed_old_ids, "Protection expiry alone is not evidence: compaction changed no Snapshot segment IDs"

    old_status, old_commit = case.commit(old_result_uri)
    case.write_local_evidence(old_job, "old-result-commit-response.json", old_commit)
    assert old_status == (200 if unchanged_old_ids else 500)
    statuses_by_segment = {int(item["segment_id"]): item for item in old_commit["segment_statuses"]}
    assert set(statuses_by_segment) == old_segment_ids
    assert old_commit["failed_segments"] == len(changed_old_ids)
    assert old_commit["committed_segments"] == len(unchanged_old_ids)
    for segment_id in changed_old_ids:
        status = statuses_by_segment[segment_id]
        assert not status.get("ok", False)
        assert (
            "segment not found in meta" in status["reason"]
            or "segment state is not flushed" in status["reason"].lower()
        )
    assert all(statuses_by_segment[segment_id]["ok"] for segment_id in unchanged_old_ids)

    new_snapshot = case.create_snapshot()
    case.write_local_evidence(old_job, "snapshot-after-compaction.json", new_snapshot.raw)
    assert set(new_snapshot.segment_ids) != old_segment_ids

    new_job, new_result_uri = case.run_backfill(
        case_id="after-compaction-new-snapshot",
        parquet_uri=parquet_uri,
        mode="coalesce",
    )
    _validate_commit_and_visibility(case, new_job, new_result_uri, parquet_rows, "coalesce", matched_rows=18)
