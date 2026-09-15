import pyarrow as pa
import pytest
from common.common_type import CaseLabel

from spark_backfill.backfill_helpers import log_contains_message, make_backfill_rows

pytestmark = [
    pytest.mark.tags(CaseLabel.SparkBackfill),
    pytest.mark.spark_e2e,
    pytest.mark.spark_backfill_v3,
    pytest.mark.spark_backfill_negative,
]


def _assert_negative(case, job_result, result_uri, *expected_logs):
    assert not job_result.succeeded
    assert job_result.exit_code not in (None, 0)
    result_key = result_uri.split(f"s3a://{case.settings.minio_bucket}/", 1)[-1]
    objects = case.list_result_objects(result_uri)
    case.write_local_evidence(job_result, "snapshot.json", case.snapshot.raw)
    case.write_local_evidence(job_result, "objects.json", objects)
    if result_key in objects:
        result = case.read_result(result_uri)
        case.write_local_evidence(job_result, "backfill-result.json", result)
        assert result.get("success") is not True
    assert all(log_contains_message(job_result.logs, expected_log) for expected_log in expected_logs)


def test_duplicate_primary_key_fails_without_committable_result(backfill_case_factory):
    case = backfill_case_factory()
    rows = make_backfill_rows()
    rows.append(dict(rows[0]))
    parquet_uri = case.upload_parquet("duplicate-pk", rows)

    job_result, result_uri = case.run_backfill(
        case_id="duplicate-pk",
        parquet_uri=parquet_uri,
        mode="coalesce",
    )

    _assert_negative(case, job_result, result_uri, "duplicate primary key")


def test_missing_primary_key_fails_without_committable_result(backfill_case_factory):
    case = backfill_case_factory()
    rows = make_backfill_rows()
    parquet_uri = case.upload_parquet("missing-pk", rows, include_pk=False)

    job_result, result_uri = case.run_backfill(
        case_id="missing-pk",
        parquet_uri=parquet_uri,
        mode="coalesce",
    )

    _assert_negative(case, job_result, result_uri, "primary key")


@pytest.mark.parametrize(
    ("score_type", "convert", "parquet_type"),
    [
        (pa.float64(), False, "double"),
        (pa.string(), True, "string"),
    ],
)
def test_scalar_type_mismatch_fails(backfill_case_factory, score_type, convert, parquet_type):
    case = backfill_case_factory()
    rows = make_backfill_rows()
    if convert:
        for row in rows:
            if row["bf_score"] is not None:
                row["bf_score"] = str(row["bf_score"])
    parquet_uri = case.upload_parquet(
        f"score-type-{score_type}",
        rows,
        score_type=score_type,
        target_fields=("bf_score",),
    )

    job_result, result_uri = case.run_backfill(
        case_id=f"score-type-{score_type}",
        parquet_uri=parquet_uri,
        mode="coalesce",
    )

    _assert_negative(
        case,
        job_result,
        result_uri,
        "types to match",
        "bf_score",
        "snapshot float",
        f"parquet {parquet_type}",
    )


@pytest.mark.parametrize("dimension", [3, 5])
def test_vector_dimension_mismatch_fails(backfill_case_factory, dimension):
    case = backfill_case_factory()
    rows = make_backfill_rows()
    for row in rows:
        row["bf_vector"] = [float(row["pk"])] * dimension
    parquet_uri = case.upload_parquet(
        f"vector-dim-{dimension}",
        rows,
        dim=dimension,
        target_fields=("bf_vector",),
    )

    job_result, result_uri = case.run_backfill(
        case_id=f"vector-dim-{dimension}",
        parquet_uri=parquet_uri,
        mode="coalesce",
    )

    _assert_negative(
        case,
        job_result,
        result_uri,
        "dimension mismatch",
        "bf_vector",
        "expected 4",
        f"got {dimension}",
    )


def test_vector_string_with_non_array_json_fails(backfill_case_factory):
    case = backfill_case_factory()
    rows = make_backfill_rows()
    for row in rows:
        row["bf_vector"] = '{"0": 1.0}'
    parquet_uri = case.upload_parquet(
        "vector-string-object",
        rows,
        vector_type=pa.string(),
        target_fields=("bf_vector",),
    )

    job_result, result_uri = case.run_backfill(
        case_id="vector-string-object",
        parquet_uri=parquet_uri,
        mode="overwrite",
    )

    _assert_negative(
        case,
        job_result,
        result_uri,
        "vector field",
        "bf_vector",
        "expected a json array",
    )


@pytest.mark.parametrize(
    ("mode", "batch_size", "expected_log"),
    [
        ("merge", 1024, "mode must be one of"),
        ("coalesce", "not-a-number", "numberformatexception"),
        ("coalesce", 0, "batchsize must be positive"),
        ("coalesce", -1, "batchsize must be positive"),
    ],
)
def test_invalid_mode_or_batch_size_fails(backfill_case_factory, mode, batch_size, expected_log):
    case = backfill_case_factory()
    rows = make_backfill_rows()
    parquet_uri = case.upload_parquet(f"invalid-{mode}-{batch_size}", rows)

    job_result, result_uri = case.run_backfill(
        case_id=f"invalid-{mode}-{batch_size}",
        parquet_uri=parquet_uri,
        mode=mode,
        batch_size=batch_size,
    )

    _assert_negative(case, job_result, result_uri, expected_log)
