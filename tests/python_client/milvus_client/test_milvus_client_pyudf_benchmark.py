"""Explicit opt-in, 48-minute bounded measurements; no performance pass threshold."""

import json
import tarfile
import uuid
from types import SimpleNamespace

import pytest
from common import common_func as cf
from minio import Minio
from pymilvus import MilvusClient
from workloads.pyudf.benchmark import run_abba, run_batch_measurements
from workloads.pyudf.workload import prepare


@pytest.mark.tags("L3")
def test_pyudf_equivalent_semantics_abba(request, minio_host, minio_bucket, tmp_path, record_property):
    if not request.config.getoption("--run-pyudf-benchmark"):
        pytest.skip("Requires an explicitly reserved benchmark instance")
    uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
    token = cf.param_info.param_token or f"{cf.param_info.param_user}:{cf.param_info.param_password}"
    client = MilvusClient(uri, token=token)
    storage = Minio(f"{minio_host}:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
    name = "pyudf_r1_benchmark_" + uuid.uuid4().hex[:12]
    args = SimpleNamespace(collection=name, rows=100000, index_type="AUTOINDEX", bucket=minio_bucket)
    try:
        version = client.get_server_version(detail=True)
        record_property("server_version", json.dumps(version))
        expected_commit = request.config.getoption("--pyudf-expected-commit")
        if expected_commit:
            assert version["git_commit"].startswith(expected_commit), version
        checker = prepare(client, storage, args, tmp_path)
        print(json.dumps({"event": "benchmark_baseline", **checker.baseline()}), flush=True)
        run_abba(checker, uri, token)
        checker.audit_all_rows()
    finally:
        # Keep both the input ledger and failed windows in the owned object store.
        archive = tmp_path.parent / (name + ".tar.gz")
        with tarfile.open(archive, "w:gz") as output:
            output.add(tmp_path, arcname="evidence")
        storage.fput_object(minio_bucket, "qa-evidence/" + archive.name, str(archive))
        print(
            json.dumps({"event": "retained_benchmark", "collection": name, "object": "qa-evidence/" + archive.name}),
            flush=True,
        )
        client.close()


@pytest.mark.tags("L3")
def test_pyudf_batch_candidate_measurements(request, minio_host, minio_bucket, tmp_path, record_property):
    if not request.config.getoption("--run-pyudf-benchmark"):
        pytest.skip("Requires an explicitly reserved benchmark instance")
    uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
    token = cf.param_info.param_token or f"{cf.param_info.param_user}:{cf.param_info.param_password}"
    client = MilvusClient(uri, token=token)
    storage = Minio(f"{minio_host}:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
    name = "pyudf_r1_batch_" + uuid.uuid4().hex[:12]
    args = SimpleNamespace(collection=name, rows=100000, index_type="HNSW", bucket=minio_bucket)
    try:
        version = client.get_server_version(detail=True)
        record_property("server_version", json.dumps(version))
        expected_commit = request.config.getoption("--pyudf-expected-commit")
        if expected_commit:
            assert version["git_commit"].startswith(expected_commit), version
        checker = prepare(client, storage, args, tmp_path)
        print(json.dumps({"event": "batch_baseline", **checker.baseline()}), flush=True)
        run_batch_measurements(checker, uri, token)
        checker.audit_all_rows()
    finally:
        archive = tmp_path.parent / (name + ".tar.gz")
        with tarfile.open(archive, "w:gz") as output:
            output.add(tmp_path, arcname="evidence")
        storage.fput_object(minio_bucket, "qa-evidence/" + archive.name, str(archive))
        print(
            json.dumps({"event": "retained_batch", "collection": name, "object": "qa-evidence/" + archive.name}),
            flush=True,
        )
        client.close()
