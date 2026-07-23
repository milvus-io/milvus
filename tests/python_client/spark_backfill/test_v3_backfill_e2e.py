import time

import pytest
from pymilvus import DataType

from spark_backfill.backfill_helpers import (
    assert_commit_succeeded,
    inspect_result_artifacts,
    make_backfill_rows,
    validate_v3_result,
    wait_for_visible_rows,
)
from spark_backfill.contracts import build_ground_truth

pytestmark = [pytest.mark.spark_e2e, pytest.mark.spark_backfill_v3, pytest.mark.spark_backfill_core]
TARGET_FIELDS = ("bf_score", "bf_label", "bf_vector")


def _source_by_pk(case):
    return {row["id"]: row for row in case.source_rows}


def _parquet_by_pk(rows):
    return {row["pk"]: row for row in rows}


def _validate_commit_and_visibility(case, job_result, result_uri, parquet_rows, mode, matched_rows=None):
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
        target_fields=set(TARGET_FIELDS),
        current_manifest_versions=case.snapshot.manifest_versions,
    )
    status, commit = case.commit(result_uri)
    case.write_local_evidence(job_result, "commit-response.json", commit)
    assert status == 200, commit
    assert_commit_succeeded(commit, expected_segments=set(case.snapshot.segment_ids), expected_kind="v3")
    expected = build_ground_truth(_source_by_pk(case), _parquet_by_pk(parquet_rows), TARGET_FIELDS, mode)
    wait_for_visible_rows(case.client, case.collection_name, expected, TARGET_FIELDS)
    return result, commit


def test_spark_read_count_projection_sql_and_topk(backfill_case_factory):
    case = backfill_case_factory()

    job_result, summary = case.run_read_probe()

    assert job_result.succeeded, job_result.logs
    assert summary["count"] == 30
    assert summary["primaryKeys"] == list(range(30))
    assert summary["projection"] == {"fields": ["id", "base_float"], "count": 30}
    assert summary["sqlRows"][0]["total"] == 30
    assert len(summary["topK"]) == 5
    case.write_local_evidence(job_result, "read-result.json", summary)


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


def test_snapshot_after_insert_excludes_new_row_from_old_backfill(backfill_case_factory):
    case = backfill_case_factory()
    new_row = {
        "id": 30,
        "base_int": 30,
        "base_float": 30.0,
        "text": "row-30",
        "vector": [30.0, 30.1, 30.2, 30.3],
        "bf_score": -999.0,
        "bf_label": "inserted-after-snapshot",
        "bf_vector": [30.0] * 4,
    }
    case.client.insert(case.collection_name, [new_row])
    case.client.flush(case.collection_name)
    parquet_rows = [
        {
            "pk": primary_key,
            "bf_score": float(primary_key),
            "bf_label": f"backfill-{primary_key}",
            "bf_vector": [float(primary_key)] * 4,
        }
        for primary_key in range(31)
    ]
    parquet_uri = case.upload_parquet("insert-after-snapshot", parquet_rows)

    job_result, result_uri = case.run_backfill(
        case_id="insert-after-snapshot",
        parquet_uri=parquet_uri,
        mode="overwrite",
    )

    assert job_result.succeeded, job_result.logs
    result = case.read_result(result_uri)
    validate_v3_result(
        result,
        collection_id=case.snapshot.collection_id,
        schema_version=case.snapshot.schema_version,
        source_rows=30,
        backfill_rows=31,
        matched_rows=30,
        target_fields=set(TARGET_FIELDS),
        current_manifest_versions=case.snapshot.manifest_versions,
    )
    status, commit = case.commit(result_uri)
    assert status == 200, commit
    assert_commit_succeeded(commit, expected_segments=set(case.snapshot.segment_ids), expected_kind="v3")
    expected = build_ground_truth(_source_by_pk(case), _parquet_by_pk(parquet_rows), TARGET_FIELDS, "overwrite")
    expected[30] = {field: new_row[field] for field in TARGET_FIELDS}
    wait_for_visible_rows(case.client, case.collection_name, expected, TARGET_FIELDS)


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
    assert "not found in snapshot schema" in job_result.logs.lower()
    result_key = result_uri.split(f"s3a://{case.settings.minio_bucket}/", 1)[-1]
    objects = case.list_result_objects(result_uri)
    case.write_local_evidence(job_result, "snapshot.json", case.snapshot.raw)
    case.write_local_evidence(job_result, "objects.json", objects)
    assert result_key not in objects


@pytest.mark.spark_backfill_known_gap
@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/milvus-io/milvus/issues/51318: CommitBackfillResult lacks schemaVersion fencing",
)
def test_add_field_after_spark_before_commit_rejects_stale_result(backfill_case_factory):
    case = backfill_case_factory()
    parquet_rows = make_backfill_rows()
    parquet_uri = case.upload_parquet("schema-gap", parquet_rows)
    job_result, result_uri = case.run_backfill(case_id="schema-gap", parquet_uri=parquet_uri, mode="coalesce")
    assert job_result.succeeded, job_result.logs
    case.client.add_collection_field(case.collection_name, "post_spark_field", DataType.FLOAT, nullable=True)

    status, commit = case.commit(result_uri)

    assert status != 200
    assert commit["failed_segments"] == len(case.snapshot.segment_ids)


def test_v3_rejects_duplicate_or_stale_manifest_commit(backfill_case_factory):
    case = backfill_case_factory()
    parquet_rows = make_backfill_rows()
    parquet_uri = case.upload_parquet("stale-version", parquet_rows)
    job_result, result_uri = case.run_backfill(
        case_id="stale-version",
        parquet_uri=parquet_uri,
        mode="coalesce",
    )
    _validate_commit_and_visibility(case, job_result, result_uri, parquet_rows, "coalesce", matched_rows=18)

    status, second_commit = case.commit(result_uri)

    assert status == 500
    assert second_commit["committed_segments"] == 0
    assert second_commit["failed_segments"] == len(case.snapshot.segment_ids)
    assert all(not item["ok"] for item in second_commit["segment_statuses"])
    assert all("not greater than current" in item["reason"] for item in second_commit["segment_statuses"])


@pytest.mark.spark_backfill_compaction
def test_expired_protection_without_compaction_still_allows_commit(backfill_case_factory):
    case = backfill_case_factory(compaction_protection_seconds=5)
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
    assert old_job.succeeded, old_job.logs
    wait_seconds = max(0, old_snapshot.compaction_expire_time - int(time.time()) + 2)
    time.sleep(wait_seconds)

    compaction_id = case.client.compact(case.collection_name)
    deadline = time.monotonic() + 180
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
    case.write_local_evidence(old_job, "compaction.json", {"job_id": compaction_id, "state": state})

    new_snapshot = case.create_snapshot()
    case.write_local_evidence(old_job, "snapshot-before-compaction.json", old_snapshot.raw)
    case.write_local_evidence(old_job, "snapshot-after-compaction.json", new_snapshot.raw)
    assert set(new_snapshot.segment_ids) != set(old_snapshot.segment_ids), (
        "Protection expiry alone is not evidence: compaction did not change Snapshot segment IDs"
    )

    old_status, old_commit = case.commit(old_result_uri)
    case.write_local_evidence(old_job, "old-result-commit-response.json", old_commit)
    assert old_status in {200, 500}
    assert old_commit["failed_segments"] > 0
    assert any(not item["ok"] for item in old_commit["segment_statuses"])

    new_job, new_result_uri = case.run_backfill(
        case_id="after-compaction-new-snapshot",
        parquet_uri=parquet_uri,
        mode="coalesce",
    )
    _validate_commit_and_visibility(case, new_job, new_result_uri, parquet_rows, "coalesce", matched_rows=18)
