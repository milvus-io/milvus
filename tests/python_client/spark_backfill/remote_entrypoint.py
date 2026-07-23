"""Entrypoint mounted into the public Apache Spark image by the pytest runner."""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import subprocess
import sys
import tarfile
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from .contracts import ConnectorBundleManifest
except ImportError:  # pragma: no cover - used when mounted as a standalone ConfigMap script
    from contracts import ConnectorBundleManifest


SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.1"
SENSITIVE_FLAGS = {
    "--s3-access-key",
    "--s3-secret-key",
    "--source-s3-access-key",
    "--source-s3-secret-key",
}


class BundlePreparationError(RuntimeError):
    """The downloaded Connector archive is invalid or incompatible."""


@dataclass(frozen=True)
class PreparedConnectorBundle:
    root: Path
    jar_path: Path
    library_dir: Path
    manifest: ConnectorBundleManifest


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_extract_tar(archive: tarfile.TarFile, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    destination_root = destination.resolve()
    for member in archive.getmembers():
        if member.issym() or member.islnk():
            raise BundlePreparationError(f"unsafe link in connector bundle: {member.name!r}")
        target = (destination / member.name).resolve()
        if target != destination_root and destination_root not in target.parents:
            raise BundlePreparationError(f"unsafe path in connector bundle: {member.name!r}")
    archive.extractall(destination, filter="data")


def prepare_connector_bundle(bundle_path: Path, expected_sha256: str, destination: Path) -> PreparedConnectorBundle:
    actual_archive_sha = sha256_file(bundle_path)
    if actual_archive_sha.lower() != expected_sha256.lower():
        raise BundlePreparationError(
            f"connector archive SHA256 mismatch: expected={expected_sha256}, actual={actual_archive_sha}"
        )

    with tarfile.open(bundle_path, "r:gz") as archive:
        safe_extract_tar(archive, destination)

    manifest_path = destination / "manifest.json"
    if not manifest_path.is_file():
        raise BundlePreparationError("connector bundle is missing manifest.json")
    try:
        manifest = ConnectorBundleManifest.from_dict(json.loads(manifest_path.read_text(encoding="utf-8")))
        manifest.validate_runtime(
            spark_version=os.getenv("SPARK_BACKFILL_EXPECTED_SPARK_VERSION", "4.0.1"),
            scala_binary_version=os.getenv("SPARK_BACKFILL_EXPECTED_SCALA_VERSION", "2.13"),
            java_major=int(os.getenv("SPARK_BACKFILL_EXPECTED_JAVA_MAJOR", "21")),
            os_name="linux",
            arch="amd64",
        )
    except (ValueError, TypeError) as exc:
        raise BundlePreparationError(str(exc)) from exc

    for relative_path, expected_file_sha in manifest.files.items():
        file_path = destination / relative_path
        if not file_path.is_file():
            raise BundlePreparationError(f"connector bundle is missing {relative_path!r}")
        actual_file_sha = sha256_file(file_path)
        if actual_file_sha.lower() != expected_file_sha.lower():
            raise BundlePreparationError(
                f"SHA256 mismatch for {relative_path!r}: expected={expected_file_sha}, actual={actual_file_sha}"
            )

    return PreparedConnectorBundle(
        root=destination,
        jar_path=destination / manifest.assembly_jar,
        library_dir=destination / "lib",
        manifest=manifest,
    )


def build_spark_command(
    *,
    operation: str,
    payload: Mapping[str, Any],
    prepared: PreparedConnectorBundle,
    support_dir: Path,
    environment: Mapping[str, str],
) -> tuple[list[str], dict[str, str]]:
    library_dir = str(prepared.library_dir)
    command = [
        SPARK_SUBMIT,
        "--master",
        "local[2]",
        "--packages",
        HADOOP_AWS_PACKAGE,
        "--conf",
        f"spark.driver.extraJavaOptions=-Djava.library.path={library_dir}",
        "--conf",
        f"spark.driver.extraLibraryPath={library_dir}",
        "--conf",
        f"spark.executor.extraLibraryPath={library_dir}",
        "--conf",
        f"spark.executorEnv.LD_LIBRARY_PATH={library_dir}",
        "--conf",
        "spark.driver.userClassPathFirst=true",
        "--conf",
        "spark.executor.userClassPathFirst=true",
        "--conf",
        "spark.jars.ivy=/tmp/spark-local/ivy",
        "--conf",
        "spark.local.dir=/tmp/spark-local",
    ]
    child_env = dict(environment)
    current_library_path = child_env.get("LD_LIBRARY_PATH", "")
    child_env["LD_LIBRARY_PATH"] = library_dir + (f":{current_library_path}" if current_library_path else "")

    if operation == "backfill":
        arguments = [str(value) for value in payload.get("arguments", [])]
        if any(argument in SENSITIVE_FLAGS for argument in arguments):
            raise ValueError("Backfill credentials must be supplied through the Kubernetes Secret")
        access_key = environment.get("SPARK_BACKFILL_S3_ACCESS_KEY", "")
        secret_key = environment.get("SPARK_BACKFILL_S3_SECRET_KEY", "")
        if bool(access_key) != bool(secret_key):
            raise ValueError("S3 access key and secret key must both be present or both be absent")

        command.extend(
            [
                "--class",
                prepared.manifest.backfill_main_class,
                str(prepared.jar_path),
                *arguments,
            ]
        )
        if access_key:
            command.extend(["--s3-access-key", access_key, "--s3-secret-key", secret_key])
        elif "--use-iam" not in arguments:
            command.append("--use-iam")
    elif operation == "read":
        read_probe = support_dir / "read_probe.py"
        if not read_probe.is_file():
            raise ValueError(f"Spark Read probe not found: {read_probe}")
        read_spec = json.loads(json.dumps(payload))
        access_key = environment.get("SPARK_BACKFILL_S3_ACCESS_KEY", "")
        secret_key = environment.get("SPARK_BACKFILL_S3_SECRET_KEY", "")
        if bool(access_key) != bool(secret_key):
            raise ValueError("S3 access key and secret key must both be present or both be absent")
        child_env["SPARK_BACKFILL_READ_SPEC_JSON"] = json.dumps(read_spec, separators=(",", ":"))
        command.extend(["--jars", str(prepared.jar_path), str(read_probe)])
    else:
        raise ValueError(f"unsupported Spark operation: {operation!r}")
    return command, child_env


def redact_command(command: Sequence[str]) -> str:
    redacted: list[str] = []
    redact_next = False
    for item in command:
        if redact_next:
            redacted.append("<redacted>")
            redact_next = False
            continue
        redacted.append(item)
        if item in SENSITIVE_FLAGS:
            redact_next = True
    return shlex.join(redacted)


def redact_text(value: str, secrets: Sequence[str]) -> str:
    redacted = value
    for secret in sorted({secret for secret in secrets if secret}, key=len, reverse=True):
        redacted = redacted.replace(secret, "<redacted>")
    return redacted


def run_spark_command(command: Sequence[str], environment: Mapping[str, str]) -> int:
    secret_values = [
        environment.get("SPARK_BACKFILL_S3_ACCESS_KEY", ""),
        environment.get("SPARK_BACKFILL_S3_SECRET_KEY", ""),
        environment.get("SPARK_BACKFILL_MILVUS_TOKEN", ""),
    ]
    process = subprocess.Popen(
        list(command),
        env=dict(environment),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    if process.stdout is not None:
        for line in process.stdout:
            print(redact_text(line, secret_values), end="", flush=True)
    return process.wait()


def download_connector_bundle(url: str, destination: Path) -> None:
    if not url.startswith("https://"):
        raise BundlePreparationError("connector bundle URL must use HTTPS")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=120) as response, destination.open("wb") as output:
        while chunk := response.read(1024 * 1024):
            output.write(chunk)


def main() -> int:
    work_root = Path("/tmp/spark-backfill")
    bundle_path = work_root / "connector.tar.gz"
    bundle_root = work_root / "connector"
    try:
        operation = os.environ["SPARK_BACKFILL_OPERATION"]
        payload = json.loads(os.environ["SPARK_BACKFILL_JOB_SPEC_JSON"])
        connector_url = os.environ["SPARK_BACKFILL_CONNECTOR_URL"]
        connector_sha256 = os.environ["SPARK_BACKFILL_CONNECTOR_SHA256"]
        download_connector_bundle(connector_url, bundle_path)
        prepared = prepare_connector_bundle(bundle_path, connector_sha256, bundle_root)
        command, child_env = build_spark_command(
            operation=operation,
            payload=payload,
            prepared=prepared,
            support_dir=Path("/opt/spark-backfill"),
            environment=os.environ,
        )
        print(f"Executing Spark command: {redact_command(command)}", flush=True)
        return run_spark_command(command, child_env)
    except Exception as exc:
        print(f"Spark Backfill remote entrypoint failed: {exc}", file=sys.stderr, flush=True)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
