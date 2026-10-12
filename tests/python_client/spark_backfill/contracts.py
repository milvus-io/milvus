"""Pure contracts shared by local pytest orchestration and remote Spark jobs."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

BACKFILL_MAIN_CLASS = "com.zilliz.spark.connector.operations.backfill.BackfillApp"
REQUIRED_BUNDLE_FILES = (
    "connector-assembly.jar",
    "lib/libmilvus-storage.so",
    "lib/libmilvus-storage-jni.so",
)
READ_RESULT_PREFIX = "SPARK_BACKFILL_READ_RESULT="
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")


class BundleContractError(ValueError):
    """Connector bundle does not satisfy the Nightly runtime contract."""


class GroundTruthError(ValueError):
    """Backfill mode or input data cannot produce deterministic ground truth."""


@dataclass(frozen=True)
class ConnectorBundleManifest:
    connector_revision: str
    spark_version: str
    scala_binary_version: str
    java_major: int
    os_name: str
    arch: str
    assembly_jar: str
    backfill_main_class: str
    files: Mapping[str, str]

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> ConnectorBundleManifest:
        try:
            manifest = cls(
                connector_revision=str(raw["connectorRevision"]),
                spark_version=str(raw["sparkVersion"]),
                scala_binary_version=str(raw["scalaBinaryVersion"]),
                java_major=int(raw["javaMajor"]),
                os_name=str(raw["os"]),
                arch=str(raw["arch"]),
                assembly_jar=str(raw["assemblyJar"]),
                backfill_main_class=str(raw["backfillMainClass"]),
                files=dict(raw["files"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise BundleContractError(f"invalid connector bundle manifest: {exc}") from exc

        manifest._validate_shape()
        return manifest

    def _validate_shape(self) -> None:
        required = set(REQUIRED_BUNDLE_FILES)
        required.discard("connector-assembly.jar")
        required.add(self.assembly_jar)
        missing = sorted(required.difference(self.files))
        if missing:
            raise BundleContractError(f"connector bundle is missing required files: {', '.join(missing)}")

        if self.backfill_main_class != BACKFILL_MAIN_CLASS:
            raise BundleContractError(
                f"unexpected Backfill main class {self.backfill_main_class!r}; expected {BACKFILL_MAIN_CLASS!r}"
            )

        for filename, checksum in self.files.items():
            path = PurePosixPath(filename)
            if path.is_absolute() or ".." in path.parts:
                raise BundleContractError(f"unsafe bundle path: {filename!r}")
            if not _SHA256_RE.fullmatch(str(checksum)):
                raise BundleContractError(f"invalid SHA256 for {filename!r}")

    def validate_runtime(
        self,
        *,
        spark_version: str,
        scala_binary_version: str,
        java_major: int,
        os_name: str,
        arch: str,
    ) -> None:
        checks = (
            (self.spark_version, spark_version, "Spark version"),
            (self.scala_binary_version, scala_binary_version, "Scala binary version"),
            (self.java_major, java_major, "Java major version"),
            (self.os_name, os_name, "operating system"),
            (self.arch, arch, "architecture"),
        )
        for actual, expected, label in checks:
            if actual != expected:
                raise BundleContractError(f"{label} mismatch: bundle={actual!r}, runtime={expected!r}")


def build_ground_truth(
    source_rows: Mapping[Any, Mapping[str, Any]],
    parquet_rows: Mapping[Any, Mapping[str, Any]],
    target_fields: Sequence[str],
    mode: str,
) -> dict[Any, dict[str, Any]]:
    """Return target-field values for the immutable row set fixed by a Snapshot."""

    if mode not in {"coalesce", "overwrite", "replace"}:
        raise GroundTruthError(f"unknown backfill mode: {mode!r}")

    result: dict[Any, dict[str, Any]] = {}
    for primary_key, source in source_rows.items():
        incoming = parquet_rows.get(primary_key)
        values: dict[str, Any] = {}
        for field in target_fields:
            source_value = source.get(field)
            if mode == "replace":
                values[field] = incoming.get(field) if incoming is not None else None
            elif mode == "overwrite" and incoming is not None:
                values[field] = incoming.get(field)
            elif mode == "coalesce" and incoming is not None and source_value is None:
                values[field] = incoming.get(field)
            else:
                values[field] = source_value
        result[primary_key] = values
    return result


def storage_kind(segment: Mapping[str, Any]) -> str:
    """Classify a Backfill Result segment without using historical wire-key names."""

    if segment.get("storage_version") == 2 and segment.get("column_groups"):
        return "v2"
    if int(segment.get("version", -1)) > 0 and segment.get("manifestPaths"):
        return "v3"
    raise ValueError("segment does not contain a valid Storage V2 or V3 payload")


def extract_read_probe_result(logs: str) -> dict[str, Any]:
    """Extract the final bounded JSON result emitted by the remote PySpark probe."""

    payloads = [line[len(READ_RESULT_PREFIX) :] for line in logs.splitlines() if line.startswith(READ_RESULT_PREFIX)]
    if not payloads:
        raise ValueError(f"Spark Read probe result sentinel not found in logs:\n{logs}")
    try:
        result = json.loads(payloads[-1])
    except json.JSONDecodeError as exc:
        raise ValueError("Spark Read probe emitted invalid JSON") from exc
    if not isinstance(result, dict):
        raise ValueError("Spark Read probe result must be a JSON object")
    return result
