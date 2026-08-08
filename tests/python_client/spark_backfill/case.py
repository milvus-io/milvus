"""High-level reusable Backfill E2E case object."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from .backfill_helpers import (
    SnapshotMetadataView,
    build_backfill_arguments,
    collection_field_ids,
    commit_backfill_result,
    list_object_keys,
    object_key,
    parse_snapshot_metadata,
    persistent_segment_storage_versions,
    read_json_object,
    unique_name,
    upload_file,
    write_backfill_parquet,
)
from .contracts import extract_read_probe_result
from .k8s_runner import SparkJobRequest, SparkJobResult

# Keep positive E2E snapshots stable across Spark startup, execution, and commit.
# Fixture teardown drops the snapshot early, so this is a maximum lifetime, not a delay.
DEFAULT_COMPACTION_PROTECTION_SECONDS = 600


def infer_root_path(snapshot_location: str) -> str:
    key = object_key(snapshot_location, snapshot_location.split("://", 1)[-1].split("/", 1)[0])
    marker = "/snapshots/"
    if marker in f"/{key}":
        prefix = f"/{key}".split(marker, 1)[0].lstrip("/")
        return prefix
    parts = key.split("/")
    return parts[0] if len(parts) > 1 else ""


@dataclass
class BackfillCase:
    client: Any
    minio_client: Any
    runner: Any
    settings: Any
    tmp_path: Path
    collection_name: str
    snapshot_names: list[str]
    snapshot_location: str
    snapshot: SnapshotMetadataView | None
    prefix: str
    source_rows: Sequence[Mapping[str, Any]]

    @property
    def root_path(self) -> str:
        return infer_root_path(self.snapshot_location)

    def create_snapshot(
        self,
        *,
        compaction_protection_seconds: int = DEFAULT_COMPACTION_PROTECTION_SECONDS,
    ) -> SnapshotMetadataView:
        snapshot_name = unique_name("spark_backfill_snapshot")
        self.client.create_snapshot(
            snapshot_name,
            self.collection_name,
            compaction_protection_seconds=compaction_protection_seconds,
        )
        self.snapshot_names.append(snapshot_name)
        info = self.client.describe_snapshot(snapshot_name, self.collection_name)
        raw = read_json_object(self.minio_client, self.settings.minio_bucket, info.s3_location)
        raw_segment_ids = raw.get("segment_ids", raw.get("segmentIds", []))
        storage_versions = persistent_segment_storage_versions(
            self.client,
            self.collection_name,
            raw_segment_ids,
        )
        view = parse_snapshot_metadata(
            raw,
            info.s3_location,
            segment_storage_versions=storage_versions,
        )
        expected_kind = self.snapshot.storage_kind if self.snapshot is not None else view.storage_kind
        if view.storage_kind != expected_kind:
            raise AssertionError(
                f"Storage {expected_kind.upper()} suite requires {expected_kind.upper()} Snapshot segments, "
                f"observed {view.storage_kind!r}"
            )
        self.snapshot_location = info.s3_location
        self.snapshot = view
        return view

    def upload_parquet(
        self,
        case_id: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        dim: int = 4,
        include_pk: bool = True,
        score_type=None,
        vector_type=None,
        target_fields: Sequence[str] = ("bf_score", "bf_label", "bf_vector"),
        target_field_types: Mapping[str, Any] | None = None,
    ) -> str:
        local_path = self.tmp_path / case_id / "input.parquet"
        write_backfill_parquet(
            local_path,
            rows,
            dim=dim,
            include_pk=include_pk,
            score_type=score_type,
            vector_type=vector_type,
            target_fields=target_fields,
            target_field_types=target_field_types,
        )
        return upload_file(
            self.minio_client,
            self.settings.minio_bucket,
            f"{self.prefix}/{case_id}/input.parquet",
            local_path,
        )

    def result_uri(self, case_id: str) -> str:
        return f"s3a://{self.settings.minio_bucket}/{self.prefix}/{case_id}/result.json"

    def run_backfill(
        self,
        *,
        case_id: str,
        parquet_uri: str,
        mode: str,
        batch_size: int | str = 1024,
        result_uri: str | None = None,
        extra_arguments: Sequence[str] = (),
    ) -> tuple[SparkJobResult, str]:
        result_uri = result_uri or self.result_uri(case_id)
        snapshot_path = self.snapshot_location
        if "://" not in snapshot_path:
            snapshot_path = f"s3a://{self.settings.minio_bucket}/{snapshot_path.lstrip('/')}"
        arguments = build_backfill_arguments(
            parquet_path=parquet_uri,
            snapshot_path=snapshot_path,
            result_path=result_uri,
            s3_endpoint=self.settings.spark_minio_endpoint,
            s3_bucket=self.settings.minio_bucket,
            s3_root_path=self.root_path,
            mode=mode,
            batch_size=batch_size,
        )
        arguments.extend(str(value) for value in extra_arguments)
        result = self.runner.run(
            SparkJobRequest(
                case_id=case_id,
                operation="backfill",
                payload={"arguments": arguments},
            )
        )
        return result, result_uri

    def read_result(self, result_uri: str) -> dict[str, Any]:
        return read_json_object(self.minio_client, self.settings.minio_bucket, result_uri)

    def upload_result(self, case_id: str, result: Mapping[str, Any]) -> str:
        local_path = self.tmp_path / case_id / "result.json"
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
        return upload_file(
            self.minio_client,
            self.settings.minio_bucket,
            f"{self.prefix}/{case_id}/result.json",
            local_path,
        )

    def commit(self, result_uri: str) -> tuple[int, dict]:
        return commit_backfill_result(self.settings.management_endpoint, result_uri)

    def list_case_objects(self, case_id: str) -> list[str]:
        return list_object_keys(self.minio_client, self.settings.minio_bucket, f"{self.prefix}/{case_id}/")

    def list_result_objects(self, result_uri: str) -> list[str]:
        result_key = object_key(result_uri, self.settings.minio_bucket)
        parent = str(PurePosixPath(result_key).parent).rstrip("/") + "/"
        return list_object_keys(self.minio_client, self.settings.minio_bucket, parent)

    def run_read_probe(self, case_id: str = "read") -> tuple[SparkJobResult, dict[str, Any]]:
        projection_fields = ["id", "base_float"]
        projection_ids = collection_field_ids(self.client, self.collection_name, projection_fields)
        options = {
            "milvus.uri": self.settings.spark_milvus_uri,
            "milvus.collection.name": self.collection_name,
            "milvus.database.name": "default",
            "fs.address": self.settings.spark_minio_endpoint,
            "fs.bucket_name": self.settings.minio_bucket,
            "fs.root_path": self.root_path,
            "fs.use_ssl": "false",
            "fs.use_virtual_host": "false",
            "fs.region": "us-east-1",
        }
        spec = {
            "options": options,
            "primaryKey": "id",
            "projectionFields": projection_fields,
            "projectionOptions": {"fieldIDs": ",".join(str(projection_ids[field]) for field in projection_fields)},
            "sql": "SELECT COUNT(*) AS total, AVG(base_float) AS avg_float FROM milvus_backfill_read",
            "vectorSearch": {
                "query": self.source_rows[0]["vector"],
                "topK": 5,
                "metric": "L2",
                "column": "vector",
                "idColumn": "id",
            },
        }
        result = self.runner.run(SparkJobRequest(case_id=case_id, operation="read", payload=spec))
        return result, extract_read_probe_result(result.logs)

    def write_local_evidence(self, job_result: SparkJobResult, filename: str, payload: Any) -> None:
        path = Path(job_result.evidence_dir) / filename
        path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
