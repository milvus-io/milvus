import pytest
from common.common_type import CaseLabel

from spark_backfill.backfill_helpers import (
    assert_commit_succeeded,
    collection_field_ids,
    inspect_result_artifacts,
    make_backfill_rows,
    validate_v2_result,
    wait_for_visible_rows,
)
from spark_backfill.contracts import build_ground_truth

pytestmark = [
    pytest.mark.tags(CaseLabel.SparkBackfill),
    pytest.mark.spark_e2e,
    pytest.mark.spark_backfill_v2,
    pytest.mark.spark_backfill_core,
]
TARGET_FIELDS = ("bf_score", "bf_label", "bf_vector")


def _source_by_pk(case):
    return {row["id"]: row for row in case.source_rows}


def _parquet_by_pk(rows):
    return {row["pk"]: row for row in rows}


def _segment_evidence(case):
    return [vars(segment) for segment in case.client.list_persistent_segments(case.collection_name)]


def _validate_v2_job(case, job_result, result_uri, parquet_rows, target_field_ids):
    assert job_result.succeeded, job_result.logs
    result = case.read_result(result_uri)
    validate_v2_result(
        result,
        collection_id=case.snapshot.collection_id,
        schema_version=case.snapshot.schema_version,
        source_rows=len(case.source_rows),
        backfill_rows=len(parquet_rows),
        matched_rows=len(parquet_rows),
        target_fields=set(TARGET_FIELDS),
        target_field_ids=set(target_field_ids.values()),
        segment_ids=set(case.snapshot.segment_ids),
    )
    case.write_local_evidence(job_result, "snapshot.json", case.snapshot.raw)
    case.write_local_evidence(job_result, "backfill-result.json", result)
    case.write_local_evidence(job_result, "objects.json", case.list_result_objects(result_uri))
    case.write_local_evidence(
        job_result,
        "artifacts.json",
        inspect_result_artifacts(case.minio_client, case.settings.minio_bucket, result),
    )
    return result


def _commit_and_wait(case, job_result, result_uri, parquet_rows, mode):
    before = _segment_evidence(case)
    status, commit = case.commit(result_uri)
    case.write_local_evidence(job_result, "segments-before-commit.json", before)
    case.write_local_evidence(job_result, "commit-response.json", commit)
    assert status == 200, commit
    assert_commit_succeeded(commit, expected_segments=set(case.snapshot.segment_ids), expected_kind="v2")

    expected = build_ground_truth(_source_by_pk(case), _parquet_by_pk(parquet_rows), TARGET_FIELDS, mode)
    wait_for_visible_rows(case.client, case.collection_name, expected, TARGET_FIELDS)
    case.write_local_evidence(job_result, "segments-after-visibility.json", _segment_evidence(case))


def test_v2_multifield_column_groups_commit_and_replacement_become_visible(backfill_v2_case_factory):
    case = backfill_v2_case_factory()
    target_field_ids = collection_field_ids(case.client, case.collection_name, TARGET_FIELDS)

    first_rows = make_backfill_rows()
    first_parquet = case.upload_parquet("v2-first", first_rows)
    first_job, first_result_uri = case.run_backfill(
        case_id="v2-first",
        parquet_uri=first_parquet,
        mode="coalesce",
    )
    first_result = _validate_v2_job(case, first_job, first_result_uri, first_rows, target_field_ids)
    _commit_and_wait(case, first_job, first_result_uri, first_rows, "coalesce")

    second_rows = make_backfill_rows()
    for row in second_rows:
        row["bf_score"] = None if row["bf_score"] is None else row["bf_score"] + 100.0
        row["bf_label"] = f"replacement-{row['pk']}"
        row["bf_vector"] = [value + 100.0 for value in row["bf_vector"]]
    second_parquet = case.upload_parquet("v2-replacement", second_rows)
    second_job, second_result_uri = case.run_backfill(
        case_id="v2-replacement",
        parquet_uri=second_parquet,
        mode="overwrite",
    )
    second_result = _validate_v2_job(case, second_job, second_result_uri, second_rows, target_field_ids)

    for segment_id in first_result["segments"]:
        first_groups = {
            tuple(group["field_ids"]): tuple(group["binlog_files"])
            for group in first_result["segments"][segment_id]["column_groups"]
        }
        second_groups = {
            tuple(group["field_ids"]): tuple(group["binlog_files"])
            for group in second_result["segments"][segment_id]["column_groups"]
        }
        assert set(first_groups) == set(second_groups)
        assert first_groups != second_groups

    _commit_and_wait(case, second_job, second_result_uri, second_rows, "overwrite")
