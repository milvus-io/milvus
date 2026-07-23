import hashlib
import io
import json
import tarfile
from pathlib import Path

import pytest

from spark_backfill.contracts import BACKFILL_MAIN_CLASS
from spark_backfill.remote_entrypoint import (
    BundlePreparationError,
    build_spark_command,
    prepare_connector_bundle,
    redact_command,
    redact_text,
    safe_extract_tar,
)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _make_bundle(tmp_path: Path):
    jar = b"jar"
    storage = b"storage"
    jni = b"jni"
    manifest = {
        "connectorRevision": "abc123",
        "sparkVersion": "4.0.1",
        "scalaBinaryVersion": "2.13",
        "javaMajor": 21,
        "os": "linux",
        "arch": "amd64",
        "assemblyJar": "connector-assembly.jar",
        "backfillMainClass": BACKFILL_MAIN_CLASS,
        "files": {
            "connector-assembly.jar": _sha(jar),
            "lib/libmilvus-storage.so": _sha(storage),
            "lib/libmilvus-storage-jni.so": _sha(jni),
        },
    }
    bundle = tmp_path / "connector.tar.gz"
    with tarfile.open(bundle, "w:gz") as archive:
        for name, content in {
            "manifest.json": json.dumps(manifest).encode(),
            "connector-assembly.jar": jar,
            "lib/libmilvus-storage.so": storage,
            "lib/libmilvus-storage-jni.so": jni,
        }.items():
            info = tarfile.TarInfo(name)
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))
    return bundle, _sha(bundle.read_bytes())


def test_safe_extract_tar_rejects_parent_traversal(tmp_path):
    bundle = tmp_path / "bad.tar.gz"
    with tarfile.open(bundle, "w:gz") as archive:
        content = b"bad"
        info = tarfile.TarInfo("../escape")
        info.size = len(content)
        archive.addfile(info, io.BytesIO(content))

    with tarfile.open(bundle, "r:gz") as archive, pytest.raises(BundlePreparationError, match="unsafe"):
        safe_extract_tar(archive, tmp_path / "out")


def test_prepare_connector_bundle_verifies_archive_and_file_hashes(tmp_path):
    bundle, checksum = _make_bundle(tmp_path)

    prepared = prepare_connector_bundle(bundle, checksum, tmp_path / "out")

    assert prepared.jar_path.name == "connector-assembly.jar"
    assert prepared.library_dir.name == "lib"
    assert prepared.manifest.connector_revision == "abc123"


def test_prepare_connector_bundle_rejects_archive_checksum_mismatch(tmp_path):
    bundle, _ = _make_bundle(tmp_path)

    with pytest.raises(BundlePreparationError, match="archive SHA256"):
        prepare_connector_bundle(bundle, "0" * 64, tmp_path / "out")


def test_build_backfill_command_uses_local_mode_and_secret_credentials(tmp_path):
    bundle, checksum = _make_bundle(tmp_path)
    prepared = prepare_connector_bundle(bundle, checksum, tmp_path / "out")

    command, child_env = build_spark_command(
        operation="backfill",
        payload={"arguments": ["--parquet", "s3a://bucket/input", "--mode", "coalesce"]},
        prepared=prepared,
        support_dir=tmp_path,
        environment={
            "SPARK_BACKFILL_S3_ACCESS_KEY": "access",
            "SPARK_BACKFILL_S3_SECRET_KEY": "secret",
        },
    )

    assert command[:3] == ["/opt/spark/bin/spark-submit", "--master", "local[2]"]
    assert "org.apache.hadoop:hadoop-aws:3.4.1" in command
    assert f"spark.driver.extraJavaOptions=-Djava.library.path={prepared.library_dir}" in command
    assert "spark.jars.ivy=/tmp/spark-local/ivy" in command
    assert BACKFILL_MAIN_CLASS in command
    assert str(prepared.jar_path) in command
    assert command[-4:] == ["--s3-access-key", "access", "--s3-secret-key", "secret"]
    assert child_env["LD_LIBRARY_PATH"].startswith(str(prepared.library_dir))
    redacted = redact_command(command)
    assert "--s3-access-key '<redacted>'" in redacted
    assert "--s3-secret-key '<redacted>'" in redacted
    assert "--s3-access-key access" not in redacted
    assert "--s3-secret-key secret" not in redacted


def test_build_read_command_passes_options_through_environment(tmp_path):
    bundle, checksum = _make_bundle(tmp_path)
    prepared = prepare_connector_bundle(bundle, checksum, tmp_path / "out")
    support_dir = tmp_path / "support"
    support_dir.mkdir()
    (support_dir / "read_probe.py").write_text("print('probe')")

    command, child_env = build_spark_command(
        operation="read",
        payload={"options": {"milvus.uri": "http://milvus:19530"}, "primaryKey": "id"},
        prepared=prepared,
        support_dir=support_dir,
        environment={
            "SPARK_BACKFILL_MILVUS_TOKEN": "root:Milvus",
            "SPARK_BACKFILL_S3_ACCESS_KEY": "access",
            "SPARK_BACKFILL_S3_SECRET_KEY": "secret",
        },
    )

    assert "--jars" in command
    assert command[-1] == str(support_dir / "read_probe.py")
    job_spec = json.loads(child_env["SPARK_BACKFILL_READ_SPEC_JSON"])
    assert "milvus.token" not in job_spec["options"]
    assert "fs.access_key_id" not in job_spec["options"]
    assert "root:Milvus" not in child_env["SPARK_BACKFILL_READ_SPEC_JSON"]
    assert "access" not in child_env["SPARK_BACKFILL_READ_SPEC_JSON"]


def test_redact_text_removes_all_runtime_secrets():
    line = "token=root:Milvus access=ak secret=sk"

    assert redact_text(line, ["root:Milvus", "ak", "sk"]) == ("token=<redacted> access=<redacted> secret=<redacted>")
