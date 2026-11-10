"""Opt-in Cardinal cap/tiered checks on indexed data and retained row contents."""

import json
import tarfile
import time
import uuid
from types import SimpleNamespace

import pytest
import requests
from common import common_func as cf
from minio import Minio
from pymilvus import MilvusClient
from workloads.pyudf.workload import prepare


@pytest.mark.tags("L3")
def test_pyudf_cardinal_profile_reload(request, minio_host, minio_bucket, tmp_path, record_property):
    expected = request.config.getoption("--pyudf-expected-autoindex")
    if expected is None:
        pytest.skip("Requires an explicitly reserved Cardinal cap/tiered instance")
    endpoint = request.config.getoption("--pyudf-management-endpoint")
    assert endpoint
    response = requests.get(
        endpoint.rstrip("/") + "/management/config/get", params={"keys": "autoIndex.params.build"}, timeout=10
    )
    response.raise_for_status()
    params = json.loads(response.json()["configs"][0]["value"])
    assert params["index_type"] == expected, params
    uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
    token = cf.param_info.param_token or f"{cf.param_info.param_user}:{cf.param_info.param_password}"
    client = MilvusClient(uri, token=token)
    storage = Minio(f"{minio_host}:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
    name = "pyudf_r1_profile_" + uuid.uuid4().hex[:12]
    args = SimpleNamespace(collection=name, rows=10000, index_type="AUTOINDEX", bucket=minio_bucket)
    try:
        version = client.get_server_version(detail=True)
        record_property("server_version", json.dumps(version))
        expected_commit = request.config.getoption("--pyudf-expected-commit")
        if expected_commit:
            assert version["git_commit"].startswith(expected_commit), version
        checker = prepare(client, storage, args, tmp_path)
        index = client.describe_index(name, "vector")
        assert index["state"] == "Finished" and index["indexed_rows"] == 10000, index
        print(
            json.dumps(
                {
                    "event": "profile_baseline",
                    "expected_index": expected,
                    "build_params": params,
                    "index": index,
                    **checker.baseline(),
                }
            ),
            flush=True,
        )
        reloads = []
        for attempt in range(2):
            client.release_collection(name, timeout=60)
            started = time.monotonic()
            client.load_collection(name, replica_number=1, timeout=180)
            records = [checker.once(client, sequence) for sequence in range(100)]
            reloads.append({"attempt": attempt, "elapsed": time.monotonic() - started, "requests": records})
        checker.audit_all_rows()
        summary = {
            "outcome": "PASS",
            "expected_index": expected,
            "reloads": reloads,
            "physical_index_trace": "Requires separately retained server build/load logs",
        }
        (tmp_path / "profile-summary.json").write_text(json.dumps(summary, indent=2))
        print(
            json.dumps({"event": "profile_complete", "expected_index": expected, "reloads": 2, "rows": 10000}),
            flush=True,
        )
    finally:
        archive = tmp_path.parent / (name + ".tar.gz")
        with tarfile.open(archive, "w:gz") as output:
            output.add(tmp_path, arcname="evidence")
        storage.fput_object(minio_bucket, "qa-evidence/" + archive.name, str(archive))
        print(
            json.dumps({"event": "retained_profile", "collection": name, "object": "qa-evidence/" + archive.name}),
            flush=True,
        )
        client.close()
