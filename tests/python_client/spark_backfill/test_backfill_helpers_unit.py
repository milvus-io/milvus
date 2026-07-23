import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spark_backfill.backfill_helpers import (
    BackfillContractError,
    assert_commit_succeeded,
    build_backfill_arguments,
    collection_field_ids,
    inspect_result_artifacts,
    make_backfill_rows,
    make_source_rows,
    parse_snapshot_metadata,
    persistent_segment_storage_versions,
    validate_v2_result,
    validate_v3_result,
)


def _snapshot_metadata():
    return {
        "format_version": 3,
        "snapshot_info": {
            "id": "9",
            "collection_id": "100",
            "compaction_expire_time": "12345",
        },
        "collection": {"schema": {"name": "collection", "version": 7}},
        "segment_ids": ["11", "12"],
        "manifest_list": [],
        "storagev2_manifest_list": [
            {"segment_id": "11", "manifest": "files/c/11/_metadata/manifest-4.avro"},
            {"segment_id": "12", "manifest": "files/c/12/_metadata/manifest-8.avro"},
        ],
    }


def test_parse_snapshot_metadata_identifies_real_v3_segments():
    view = parse_snapshot_metadata(
        _snapshot_metadata(),
        "s3a://bucket/snapshots/100/metadata/9.json",
        segment_storage_versions={11: 3, 12: 3},
    )

    assert view.collection_id == 100
    assert view.schema_version == 7
    assert view.segment_ids == (11, 12)
    assert view.storage_kind == "v3"
    assert view.segment_storage_versions == {11: 3, 12: 3}
    assert view.manifest_versions == {11: 4, 12: 8}
    assert view.compaction_expire_time == 12345


def test_parse_snapshot_metadata_rejects_mixed_v2_v3_for_dedicated_suite():
    with pytest.raises(BackfillContractError, match="mixed-version"):
        parse_snapshot_metadata(
            _snapshot_metadata(),
            "s3a://bucket/snapshot.json",
            segment_storage_versions={11: 2, 12: 3},
        )


def test_parse_snapshot_metadata_identifies_v2_from_segment_storage_versions_not_manifest_field_name():
    raw = _snapshot_metadata()
    raw["storagev2_manifest_list"] = []

    view = parse_snapshot_metadata(
        raw,
        "s3a://bucket/snapshot.json",
        segment_storage_versions={11: 2, 12: 2},
    )

    assert view.storage_kind == "v2"
    assert view.manifest_versions == {}


@pytest.mark.parametrize("versions", [{11: 0, 12: 0}, {11: 1, 12: 1}, {11: 2}])
def test_parse_snapshot_metadata_rejects_non_v2_v3_or_incomplete_segment_evidence(versions):
    with pytest.raises(BackfillContractError, match="storage version evidence"):
        parse_snapshot_metadata(
            _snapshot_metadata(),
            "s3a://bucket/snapshot.json",
            segment_storage_versions=versions,
        )


def test_persistent_segment_storage_versions_selects_snapshot_segments_only():
    class Client:
        def list_persistent_segments(self, collection_name):
            assert collection_name == "collection"
            return [
                type("Segment", (), {"segment_id": 11, "storage_version": 2})(),
                type("Segment", (), {"segment_id": 12, "storage_version": 2})(),
                type("Segment", (), {"segment_id": 99, "storage_version": 3})(),
            ]

    assert persistent_segment_storage_versions(Client(), "collection", [11, 12]) == {11: 2, 12: 2}


def test_collection_field_ids_reads_ids_by_name():
    class Client:
        def describe_collection(self, collection_name):
            assert collection_name == "collection"
            return {
                "fields": [
                    {"name": "id", "field_id": 100},
                    {"name": "bf_score", "field_id": 104},
                    {"name": "bf_label", "field_id": 105},
                ]
            }

    assert collection_field_ids(Client(), "collection", ["bf_label", "bf_score"]) == {
        "bf_label": 105,
        "bf_score": 104,
    }


def test_deterministic_source_and_backfill_rows_follow_baseline_contract():
    source = make_source_rows()
    backfill = make_backfill_rows()

    assert len(source) == 30
    assert source[0]["id"] == 0
    assert source[0]["bf_score"] == 1000.0
    assert source[1]["bf_score"] is None
    assert len(backfill) == 18
    assert {row["pk"] for row in backfill} == set(range(0, 9)) | set(range(21, 30))
    assert backfill[0]["bf_score"] == -1.0


def test_validate_v3_result_checks_counts_fields_schema_and_versions():
    result = {
        "success": True,
        "collectionId": 100,
        "schemaVersion": 7,
        "segmentsProcessed": 2,
        "totalSourceRows": 30,
        "totalBackfillDataRows": 31,
        "totalMatchedRows": 30,
        "totalRowsWritten": 30,
        "newFieldNames": ["bf_score", "bf_label", "bf_vector"],
        "segments": {
            "11": {"version": 5, "rowCount": 10, "sourceRowCount": 10, "manifestPaths": ["manifest-5"]},
            "12": {"version": 9, "rowCount": 20, "sourceRowCount": 20, "manifestPaths": ["manifest-9"]},
        },
    }

    validate_v3_result(
        result,
        collection_id=100,
        schema_version=7,
        source_rows=30,
        backfill_rows=31,
        matched_rows=30,
        target_fields={"bf_score", "bf_label", "bf_vector"},
        current_manifest_versions={11: 4, 12: 8},
    )


def test_validate_v3_result_rejects_stale_manifest_version():
    result = {
        "success": True,
        "collectionId": 100,
        "schemaVersion": 7,
        "segmentsProcessed": 1,
        "totalSourceRows": 30,
        "totalBackfillDataRows": 30,
        "totalMatchedRows": 30,
        "totalRowsWritten": 30,
        "newFieldNames": ["bf_score"],
        "segments": {
            "11": {"version": 4, "rowCount": 30, "sourceRowCount": 30, "manifestPaths": ["manifest-4"]},
        },
    }

    with pytest.raises(BackfillContractError, match="not newer"):
        validate_v3_result(
            result,
            collection_id=100,
            schema_version=7,
            source_rows=30,
            backfill_rows=30,
            matched_rows=30,
            target_fields={"bf_score"},
            current_manifest_versions={11: 4},
        )


def test_validate_v2_result_checks_single_field_groups_and_counts():
    result = {
        "success": True,
        "collectionId": 100,
        "schemaVersion": 7,
        "segmentsProcessed": 1,
        "totalSourceRows": 30,
        "totalBackfillDataRows": 18,
        "totalMatchedRows": 18,
        "totalRowsWritten": 30,
        "newFieldNames": ["bf_score", "bf_label"],
        "segments": {
            "11": {
                "version": -1,
                "rowCount": 30,
                "sourceRowCount": 30,
                "storage_version": 2,
                "column_groups": [
                    {"field_ids": [104], "binlog_files": ["s3a://bucket/11/104/1"], "row_count": 30},
                    {"field_ids": [105], "binlog_files": ["s3a://bucket/11/105/2"], "row_count": 30},
                ],
            }
        },
    }

    validate_v2_result(
        result,
        collection_id=100,
        schema_version=7,
        source_rows=30,
        backfill_rows=18,
        matched_rows=18,
        target_fields={"bf_score", "bf_label"},
        target_field_ids={104, 105},
        segment_ids={11},
    )


def test_validate_v2_result_rejects_multi_field_column_group():
    result = {
        "success": True,
        "collectionId": 100,
        "schemaVersion": 7,
        "segmentsProcessed": 1,
        "totalSourceRows": 30,
        "totalBackfillDataRows": 18,
        "totalMatchedRows": 18,
        "totalRowsWritten": 30,
        "newFieldNames": ["bf_score", "bf_label"],
        "segments": {
            "11": {
                "version": -1,
                "rowCount": 30,
                "sourceRowCount": 30,
                "storage_version": 2,
                "column_groups": [{"field_ids": [104, 105], "binlog_files": ["11/104/1"], "row_count": 30}],
            }
        },
    }

    with pytest.raises(BackfillContractError, match="exactly one field"):
        validate_v2_result(
            result,
            collection_id=100,
            schema_version=7,
            source_rows=30,
            backfill_rows=18,
            matched_rows=18,
            target_fields={"bf_score", "bf_label"},
            target_field_ids={104, 105},
            segment_ids={11},
        )


def test_inspect_result_artifacts_stats_v2_and_v3_files():
    class Stat:
        def __init__(self, size):
            self.size = size

    class Minio:
        def __init__(self):
            self.keys = []
            sink = pa.BufferOutputStream()
            pq.write_table(pa.table({"field_104": [1.0, 2.0]}), sink)
            self.parquet = sink.getvalue().to_pybytes()

        def stat_object(self, bucket, key):
            assert bucket == "bucket"
            self.keys.append(key)
            return Stat(123)

        def get_object(self, bucket, key):
            assert bucket == "bucket"
            assert key == "12/104/7"

            class Response:
                def __init__(self, payload):
                    self.payload = payload

                def read(self):
                    return self.payload

                def close(self):
                    pass

                def release_conn(self):
                    pass

            return Response(self.parquet)

    result = {
        "segments": {
            "11": {"version": 5, "manifestPaths": ["s3a://bucket/11/manifest-5.avro"]},
            "12": {
                "version": -1,
                "storage_version": 2,
                "column_groups": [{"field_ids": [104], "binlog_files": ["s3a://bucket/12/104/7"], "row_count": 2}],
            },
        }
    }
    client = Minio()

    evidence = inspect_result_artifacts(client, "bucket", result)

    assert client.keys == ["11/manifest-5.avro", "12/104/7"]
    assert [item["kind"] for item in evidence] == ["v3", "v2"]
    assert all(item["size"] == 123 for item in evidence)
    assert evidence[1]["parquet_rows"] == 2


def test_assert_commit_succeeded_requires_every_segment_status():
    response = {
        "msg": "OK",
        "total_segments": 2,
        "committed_segments": 1,
        "failed_segments": 1,
        "segment_statuses": [
            {"segment_id": 11, "ok": True, "kind": "v3"},
            {"segment_id": 12, "ok": False, "kind": "v3", "reason": "stale"},
        ],
    }

    with pytest.raises(BackfillContractError, match="segment 12"):
        assert_commit_succeeded(response, expected_segments={11, 12}, expected_kind="v3")


def test_build_backfill_arguments_contains_no_credentials():
    arguments = build_backfill_arguments(
        parquet_path="s3a://bucket/files/input.parquet",
        snapshot_path="s3a://bucket/files/snapshots/1.json",
        result_path="s3a://bucket/files/results/result.json",
        s3_endpoint="minio.ns:9000",
        s3_bucket="bucket",
        s3_root_path="files",
        mode="coalesce",
        batch_size=1024,
    )

    assert arguments[:4] == [
        "--parquet",
        "s3a://bucket/files/input.parquet",
        "--snapshot",
        "s3a://bucket/files/snapshots/1.json",
    ]
    assert arguments[-4:] == ["--mode", "coalesce", "--batch-size", "1024"]
    assert all("access-key" not in argument and "secret-key" not in argument for argument in arguments)
