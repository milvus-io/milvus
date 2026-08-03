"""Data preparation, object-storage, Result, Commit, and visibility helpers."""

from __future__ import annotations

import json
import re
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from pymilvus import DataType

from .contracts import storage_kind

_MANIFEST_VERSION_RE = re.compile(r"manifest-(\d+)\.avro")


class BackfillContractError(AssertionError):
    """A Snapshot, Backfill Result, Commit response, or visible row violated the E2E contract."""


class StaleSchemaFenceMissingError(BackfillContractError):
    """Milvus accepted at least part of a Result stamped with a stale SchemaVersion."""


def log_contains_message(logs: str, expected: str) -> bool:
    def normalize(value: str) -> str:
        tokens = re.findall(r"\w+", value.casefold())
        expanded = ("primary key" if token == "pk" else token for token in tokens)
        return " ".join(expanded)

    return normalize(expected) in normalize(logs)


@dataclass(frozen=True)
class SnapshotMetadataView:
    location: str
    collection_id: int
    schema_version: int
    segment_ids: tuple[int, ...]
    storage_kind: str
    segment_storage_versions: Mapping[int, int]
    manifest_versions: Mapping[int, int]
    compaction_expire_time: int
    raw: Mapping[str, Any]


def _as_int(value, label: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise BackfillContractError(f"Snapshot {label} is not an integer: {value!r}") from exc


def _manifest_version(raw_manifest: str) -> int:
    try:
        parsed = json.loads(raw_manifest)
        if isinstance(parsed, dict) and "ver" in parsed:
            return int(parsed["ver"])
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    match = _MANIFEST_VERSION_RE.search(raw_manifest)
    if match:
        return int(match.group(1))
    raise BackfillContractError(f"cannot determine Manifest version from {raw_manifest!r}")


def parse_snapshot_metadata(
    raw: Mapping[str, Any],
    location: str,
    *,
    segment_storage_versions: Mapping[int, int],
) -> SnapshotMetadataView:
    snapshot_info = raw.get("snapshot_info") or raw.get("snapshotInfo") or {}
    collection = raw.get("collection") or {}
    schema = collection.get("schema") or {}
    segment_ids = tuple(_as_int(value, "segment_id") for value in raw.get("segment_ids", raw.get("segmentIds", [])))
    v3_manifests = raw.get("storagev2_manifest_list", raw.get("storagev2ManifestList", [])) or []
    if not segment_ids:
        raise BackfillContractError("Snapshot contains no sealed segment IDs")

    normalized_versions = {int(segment_id): int(version) for segment_id, version in segment_storage_versions.items()}
    if set(normalized_versions) != set(segment_ids):
        raise BackfillContractError("Snapshot storage version evidence does not match Snapshot segment_ids")
    observed_versions = set(normalized_versions.values())
    if observed_versions == {2}:
        kind = "v2"
    elif observed_versions == {3}:
        kind = "v3"
    elif observed_versions.intersection({2, 3}) == {2, 3}:
        raise BackfillContractError("dedicated Storage V2/V3 suites reject mixed-version Snapshots")
    else:
        raise BackfillContractError(
            f"Snapshot storage version evidence must contain only V2 or only V3 segments: {sorted(observed_versions)}"
        )

    manifest_versions = {}
    if kind == "v3":
        if not v3_manifests:
            raise BackfillContractError("Storage V3 Snapshot contains no Loon manifests")
        for item in v3_manifests:
            segment_id = _as_int(item.get("segment_id", item.get("segmentId")), "V3 manifest segment_id")
            manifest_versions[segment_id] = _manifest_version(str(item.get("manifest", "")))
        if set(manifest_versions) != set(segment_ids):
            raise BackfillContractError("Snapshot V3 Manifest segment IDs do not match Snapshot segment_ids")
    elif v3_manifests:
        raise BackfillContractError("Storage V2 Snapshot unexpectedly contains Loon manifest evidence")

    return SnapshotMetadataView(
        location=location,
        collection_id=_as_int(snapshot_info.get("collection_id", snapshot_info.get("collectionId")), "collection_id"),
        schema_version=_as_int(schema.get("version", 0), "schema version"),
        segment_ids=segment_ids,
        storage_kind=kind,
        segment_storage_versions=normalized_versions,
        manifest_versions=manifest_versions,
        compaction_expire_time=_as_int(
            snapshot_info.get("compaction_expire_time", snapshot_info.get("compactionExpireTime", 0)),
            "compaction_expire_time",
        ),
        raw=raw,
    )


def persistent_segment_storage_versions(client, collection_name: str, segment_ids: Sequence[int]) -> dict[int, int]:
    expected_ids = {int(segment_id) for segment_id in segment_ids}
    observed = {
        int(segment.segment_id): int(segment.storage_version)
        for segment in client.list_persistent_segments(collection_name)
        if int(segment.segment_id) in expected_ids
    }
    if set(observed) != expected_ids:
        missing = sorted(expected_ids.difference(observed))
        raise BackfillContractError(f"Snapshot storage version evidence is missing segments: {missing}")
    return observed


def collection_field_ids(client, collection_name: str, field_names: Sequence[str]) -> dict[str, int]:
    description = client.describe_collection(collection_name)
    by_name = {}
    for field in description.get("fields", []):
        name = field.get("name", field.get("field_name"))
        field_id = field.get("field_id", field.get("id"))
        if name is not None and field_id is not None:
            by_name[str(name)] = int(field_id)
    missing = sorted(set(field_names).difference(by_name))
    if missing:
        raise BackfillContractError(f"Collection description is missing target field IDs: {missing}")
    return {field_name: by_name[field_name] for field_name in field_names}


def make_source_rows(count: int = 30, dim: int = 4) -> list[dict[str, Any]]:
    rows = []
    for primary_key in range(count):
        bf_score = 1000.0 if primary_key == 0 else 1010.0 if primary_key == 10 else None
        bf_label = f"source-{primary_key}" if primary_key in {0, 1, 10} else None
        bf_vector = [float(primary_key)] * dim if primary_key in {0, 2, 10} else None
        rows.append(
            {
                "id": primary_key,
                "base_int": primary_key,
                "base_float": float(primary_key),
                "text": f"row-{primary_key}",
                "vector": [float(primary_key) + offset / 10.0 for offset in range(dim)],
                "bf_score": bf_score,
                "bf_label": bf_label,
                "bf_vector": bf_vector,
            }
        )
    return rows


def make_backfill_rows(dim: int = 4, *, explicit_null_pk: int | None = None) -> list[dict[str, Any]]:
    rows = []
    for primary_key in [*range(0, 9), *range(21, 30)]:
        row = {
            "pk": primary_key,
            "bf_score": -1.0 if primary_key == 0 else float(primary_key) + 0.5,
            "bf_label": f"backfill-{primary_key}",
            "bf_vector": [float(primary_key)] * dim,
        }
        if primary_key == explicit_null_pk:
            row.update({"bf_score": None, "bf_label": None, "bf_vector": None})
        rows.append(row)
    return rows


def write_backfill_parquet(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
    *,
    dim: int = 4,
    include_pk: bool = True,
    score_type: pa.DataType | None = None,
    vector_type: pa.DataType | None = None,
    target_fields: Sequence[str] = ("bf_score", "bf_label", "bf_vector"),
) -> None:
    fields = []
    if include_pk:
        fields.append(pa.field("pk", pa.int64(), nullable=False))
    if "bf_score" in target_fields:
        fields.append(pa.field("bf_score", score_type or pa.float32(), nullable=True))
    if "bf_label" in target_fields:
        fields.append(pa.field("bf_label", pa.string(), nullable=True))
    if "bf_vector" in target_fields:
        fields.append(
            pa.field(
                "bf_vector",
                vector_type or pa.list_(pa.float32(), dim),
                nullable=True,
            )
        )
    for field in target_fields:
        if field not in {"bf_score", "bf_label", "bf_vector"}:
            fields.append(pa.field(field, pa.float32(), nullable=True))
    projected = [{field.name: row.get(field.name) for field in fields} for row in rows]
    table = pa.Table.from_pylist(projected, schema=pa.schema(fields))
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def build_backfill_arguments(
    *,
    parquet_path: str,
    snapshot_path: str,
    result_path: str,
    s3_endpoint: str,
    s3_bucket: str,
    s3_root_path: str,
    mode: str,
    batch_size: int | str,
) -> list[str]:
    return [
        "--parquet",
        parquet_path,
        "--snapshot",
        snapshot_path,
        "--s3-endpoint",
        s3_endpoint,
        "--s3-bucket",
        s3_bucket,
        "--s3-root-path",
        s3_root_path,
        "--s3-region",
        "us-east-1",
        "--output-result",
        result_path,
        "--mode",
        mode,
        "--batch-size",
        str(batch_size),
    ]


def validate_v3_result(
    result: Mapping[str, Any],
    *,
    collection_id: int,
    schema_version: int,
    source_rows: int,
    backfill_rows: int,
    matched_rows: int,
    target_fields: set[str],
    current_manifest_versions: Mapping[int, int],
) -> None:
    expected_scalars = {
        "success": True,
        "collectionId": collection_id,
        "schemaVersion": schema_version,
        "totalSourceRows": source_rows,
        "totalBackfillDataRows": backfill_rows,
        "totalMatchedRows": matched_rows,
        "totalRowsWritten": source_rows,
    }
    for key, expected in expected_scalars.items():
        if result.get(key) != expected:
            raise BackfillContractError(f"Backfill Result {key}={result.get(key)!r}, expected {expected!r}")
    if set(result.get("newFieldNames", [])) != target_fields:
        raise BackfillContractError("Backfill Result newFieldNames does not match target fields")

    segments = result.get("segments") or {}
    if int(result.get("segmentsProcessed", -1)) != len(segments):
        raise BackfillContractError("Backfill Result segmentsProcessed does not match segments")
    if {int(segment_id) for segment_id in segments} != set(current_manifest_versions):
        raise BackfillContractError("Backfill Result segment IDs do not match Snapshot V3 segments")

    total_segment_rows = 0
    for segment_id_raw, segment in segments.items():
        segment_id = int(segment_id_raw)
        if storage_kind(segment) != "v3":
            raise BackfillContractError(f"segment {segment_id} is not a V3 Result payload")
        if int(segment.get("rowCount", -1)) != int(segment.get("sourceRowCount", -2)):
            raise BackfillContractError(f"segment {segment_id} rowCount does not equal sourceRowCount")
        total_segment_rows += int(segment["rowCount"])
        version = int(segment["version"])
        if version <= current_manifest_versions[segment_id]:
            raise BackfillContractError(
                f"segment {segment_id} Manifest version {version} is not newer than {current_manifest_versions[segment_id]}"
            )
    if total_segment_rows != source_rows:
        raise BackfillContractError(
            f"sum of per-segment rowCount is {total_segment_rows}, expected source row count {source_rows}"
        )


def validate_v2_result(
    result: Mapping[str, Any],
    *,
    collection_id: int,
    schema_version: int,
    source_rows: int,
    backfill_rows: int,
    matched_rows: int,
    target_fields: set[str],
    target_field_ids: set[int],
    segment_ids: set[int],
) -> None:
    expected_scalars = {
        "success": True,
        "collectionId": collection_id,
        "schemaVersion": schema_version,
        "totalSourceRows": source_rows,
        "totalBackfillDataRows": backfill_rows,
        "totalMatchedRows": matched_rows,
        "totalRowsWritten": source_rows,
    }
    for key, expected in expected_scalars.items():
        if result.get(key) != expected:
            raise BackfillContractError(f"Backfill Result {key}={result.get(key)!r}, expected {expected!r}")
    if set(result.get("newFieldNames", [])) != target_fields:
        raise BackfillContractError("Backfill Result newFieldNames does not match target fields")

    segments = result.get("segments") or {}
    if int(result.get("segmentsProcessed", -1)) != len(segments):
        raise BackfillContractError("Backfill Result segmentsProcessed does not match segments")
    if {int(segment_id) for segment_id in segments} != segment_ids:
        raise BackfillContractError("Backfill Result segment IDs do not match Snapshot V2 segments")

    total_segment_rows = 0
    for segment_id_raw, segment in segments.items():
        segment_id = int(segment_id_raw)
        if storage_kind(segment) != "v2":
            raise BackfillContractError(f"segment {segment_id} is not a V2 Result payload")
        if int(segment.get("version", 0)) != -1:
            raise BackfillContractError(f"segment {segment_id} V2 version must be -1")
        row_count = int(segment.get("rowCount", -1))
        if row_count != int(segment.get("sourceRowCount", -2)):
            raise BackfillContractError(f"segment {segment_id} rowCount does not equal sourceRowCount")
        total_segment_rows += row_count

        field_ids = []
        for group in segment.get("column_groups", []):
            group_field_ids = [int(value) for value in group.get("field_ids", [])]
            if len(group_field_ids) != 1:
                raise BackfillContractError(f"segment {segment_id} column group must contain exactly one field")
            if not group.get("binlog_files"):
                raise BackfillContractError(f"segment {segment_id} column group has no binlog files")
            if int(group.get("row_count", -1)) != row_count:
                raise BackfillContractError(f"segment {segment_id} column group row_count does not match segment")
            field_ids.extend(group_field_ids)
        if set(field_ids) != target_field_ids or len(field_ids) != len(target_field_ids):
            raise BackfillContractError(f"segment {segment_id} column groups do not match target field IDs")

    if total_segment_rows != source_rows:
        raise BackfillContractError(
            f"sum of per-segment rowCount is {total_segment_rows}, expected source row count {source_rows}"
        )


def inspect_result_artifacts(minio_client, bucket: str, result: Mapping[str, Any]) -> list[dict[str, Any]]:
    artifacts = []
    for segment_id_raw, segment in sorted((result.get("segments") or {}).items(), key=lambda item: int(item[0])):
        segment_id = int(segment_id_raw)
        kind = storage_kind(segment)
        if kind == "v3":
            for artifact_path in segment.get("manifestPaths", []):
                manifest_path = _v3_manifest_path(artifact_path, int(segment.get("version", -1)))
                artifacts.append(_stat_artifact(minio_client, bucket, segment_id, kind, manifest_path))
        else:
            for group in segment.get("column_groups", []):
                group_artifacts = []
                for artifact_path in group.get("binlog_files", []):
                    evidence = _stat_artifact(minio_client, bucket, segment_id, kind, artifact_path)
                    evidence["parquet_rows"] = _read_parquet_rows(minio_client, bucket, evidence["object_key"])
                    group_artifacts.append(evidence)
                actual_rows = sum(item["parquet_rows"] for item in group_artifacts)
                expected_rows = int(group.get("row_count", -1))
                if actual_rows != expected_rows:
                    raise BackfillContractError(
                        f"segment {segment_id} V2 Column Group Parquet rows={actual_rows}, expected {expected_rows}"
                    )
                artifacts.extend(group_artifacts)
    if not artifacts:
        raise BackfillContractError("Backfill Result contains no physical artifacts")
    return artifacts


def _v3_manifest_path(base_path: str, version: int) -> str:
    path = str(base_path).rstrip("/")
    if path.endswith(".avro"):
        return path
    if version < 0:
        raise BackfillContractError(f"Storage V3 Result has no committed Manifest version for base path: {base_path}")
    return f"{path}/_metadata/manifest-{version}.avro"


def _stat_artifact(minio_client, bucket: str, segment_id: int, kind: str, artifact_path: str) -> dict[str, Any]:
    key = object_key(str(artifact_path), bucket)
    stat = minio_client.stat_object(bucket, key)
    size = int(getattr(stat, "size", 0))
    if size <= 0:
        raise BackfillContractError(f"Backfill artifact is empty: {artifact_path}")
    return {
        "segment_id": segment_id,
        "kind": kind,
        "path": str(artifact_path),
        "object_key": key,
        "size": size,
    }


def _read_parquet_rows(minio_client, bucket: str, key: str) -> int:
    response = minio_client.get_object(bucket, key)
    try:
        payload = response.read()
    finally:
        response.close()
        response.release_conn()
    try:
        return pq.ParquetFile(pa.BufferReader(payload)).metadata.num_rows
    except Exception as exc:
        raise BackfillContractError(f"V2 Column Group artifact is not readable Parquet: {key}") from exc


def assert_commit_succeeded(response: Mapping[str, Any], *, expected_segments: set[int], expected_kind: str) -> None:
    statuses = response.get("segment_statuses", response.get("segmentStatuses", [])) or []
    by_segment = {int(status.get("segment_id", status.get("segmentId", 0))): status for status in statuses}
    if set(by_segment) != expected_segments:
        raise BackfillContractError("Commit segment_statuses do not match expected Snapshot segments")
    for segment_id, status in by_segment.items():
        if not status.get("ok"):
            raise BackfillContractError(f"Commit failed for segment {segment_id}: {status.get('reason', '')}")
        if status.get("kind") != expected_kind:
            raise BackfillContractError(f"Commit classified segment {segment_id} as {status.get('kind')!r}")
    if int(response.get("total_segments", response.get("totalSegments", -1))) != len(expected_segments):
        raise BackfillContractError("Commit total_segments is incorrect")
    if int(response.get("committed_segments", response.get("committedSegments", -1))) != len(expected_segments):
        raise BackfillContractError("Commit committed_segments is incorrect")
    if int(response.get("failed_segments", response.get("failedSegments", -1))) != 0:
        raise BackfillContractError("Commit reported failed segments")


def assert_stale_schema_commit_rejected(
    http_status: int,
    response: Mapping[str, Any],
    *,
    result_version: int,
    current_version: int,
) -> None:
    if http_status == 200:
        raise StaleSchemaFenceMissingError("stale-schema Backfill Result was unexpectedly accepted")
    committed = int(response.get("committed_segments", response.get("committedSegments", 0)))
    if committed != 0:
        raise StaleSchemaFenceMissingError(f"stale-schema rejection committed {committed} segments")

    message = str(response.get("msg", response.get("message", ""))).casefold()
    expected = f"computed against schema version {result_version} but collection is now at version {current_version}"
    if "stale backfill result" not in message or expected not in message:
        raise BackfillContractError(f"Commit did not report the expected stale-schema rejection: {response!r}")


def unique_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def create_backfill_collection(client, collection_name: str, dim: int = 4) -> None:
    schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
    schema.add_field("base_int", DataType.INT64)
    schema.add_field("base_float", DataType.FLOAT)
    schema.add_field("text", DataType.VARCHAR, max_length=256)
    schema.add_field("vector", DataType.FLOAT_VECTOR, dim=dim)
    schema.add_field("bf_score", DataType.FLOAT, nullable=True)
    schema.add_field("bf_label", DataType.VARCHAR, max_length=256, nullable=True)
    schema.add_field("bf_vector", DataType.FLOAT_VECTOR, dim=dim, nullable=True)
    indexes = client.prepare_index_params()
    indexes.add_index("vector", index_type="FLAT", metric_type="L2")
    indexes.add_index("bf_vector", index_type="FLAT", metric_type="L2")
    client.create_collection(
        collection_name,
        schema=schema,
        index_params=indexes,
        consistency_level="Strong",
    )


def object_key(uri_or_key: str, expected_bucket: str) -> str:
    if "://" not in uri_or_key:
        return uri_or_key.lstrip("/")
    parsed = urlparse(uri_or_key)
    if parsed.netloc and parsed.netloc != expected_bucket:
        raise BackfillContractError(
            f"object URI bucket {parsed.netloc!r} does not match configured bucket {expected_bucket!r}"
        )
    return parsed.path.lstrip("/")


def read_json_object(minio_client, bucket: str, uri_or_key: str) -> dict[str, Any]:
    response = minio_client.get_object(bucket, object_key(uri_or_key, bucket))
    try:
        return json.loads(response.read())
    finally:
        response.close()
        response.release_conn()


def upload_file(minio_client, bucket: str, key: str, local_path: Path) -> str:
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)
    minio_client.fput_object(bucket, key, str(local_path))
    return f"s3a://{bucket}/{key}"


def list_object_keys(minio_client, bucket: str, prefix: str) -> list[str]:
    return sorted(item.object_name for item in minio_client.list_objects(bucket, prefix=prefix, recursive=True))


def remove_object_prefix(minio_client, bucket: str, prefix: str) -> None:
    for item in minio_client.list_objects(bucket, prefix=prefix, recursive=True):
        minio_client.remove_object(bucket, item.object_name)


def commit_backfill_result(management_endpoint: str, result_path: str, timeout: int = 120) -> tuple[int, dict]:
    response = requests.get(
        f"{management_endpoint}/management/datacoord/backfill/commit",
        params={"result_path": result_path},
        timeout=timeout,
    )
    try:
        payload = response.json()
    except requests.JSONDecodeError as exc:
        raise BackfillContractError(f"Commit endpoint returned non-JSON HTTP {response.status_code}") from exc
    return response.status_code, payload


def wait_for_visible_rows(
    client,
    collection_name: str,
    expected: Mapping[int, Mapping[str, Any]],
    target_fields: Sequence[str],
    timeout: int = 120,
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout
    last_rows = []
    while time.monotonic() < deadline:
        rows = client.query(
            collection_name,
            filter="id >= 0",
            output_fields=["id", *target_fields],
            limit=max(len(expected), 1),
        )
        last_rows = rows
        actual = {int(row["id"]): {field: row.get(field) for field in target_fields} for row in rows}
        if _values_equal(actual, expected):
            return rows
        time.sleep(2)
    raise BackfillContractError(f"Backfill values did not become visible without reload; last rows={last_rows!r}")


def _values_equal(actual: Mapping, expected: Mapping) -> bool:
    if set(actual) != set(expected):
        return False
    for primary_key, expected_fields in expected.items():
        for field, expected_value in expected_fields.items():
            actual_value = actual[primary_key].get(field)
            if isinstance(expected_value, float):
                if actual_value is None or abs(float(actual_value) - expected_value) > 1e-5:
                    return False
            elif isinstance(expected_value, list):
                if actual_value is None or len(actual_value) != len(expected_value):
                    return False
                if any(abs(float(a) - float(b)) > 1e-5 for a, b in zip(actual_value, expected_value)):
                    return False
            elif actual_value != expected_value:
                return False
    return True
