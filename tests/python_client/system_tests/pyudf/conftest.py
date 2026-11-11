"""Own one isolated collection, wheel set and evidence directory per scenario."""

import json
import os
import uuid
from dataclasses import asdict, replace
from pathlib import Path

import pytest
from pymilvus import MilvusClient
from workloads.pyudf.runner import WorkloadConfig, initialize, save


def pytest_addoption(parser):
    group = parser.getgroup("pyudf-system")
    group.addoption("--pyudf-system-config", type=Path, help="JSON configuration for the reserved two-Proxy deployment")
    group.addoption(
        "--pyudf-system-evidence", type=Path, help="Parent directory for per-test evidence; defaults to tmp_path"
    )


def cleanup(client, storage, config, root):
    """Attempt every owned cleanup even when one operation fails; retain errors."""
    errors = []

    def attempt(operation, action):
        try:
            action()
        except Exception as error:
            error.add_note(operation)
            errors.append(error)

    attempt("drop collection " + config.collection, lambda: client.drop_collection(config.collection, timeout=60))
    for manifest in root.rglob("wheels.json"):
        try:
            wheels = json.loads(manifest.read_text())
            # Never use unchecked names or object paths from a retained manifest.
            for wheel in wheels:
                name, path = wheel["resource"], wheel["path"]
                assert name.startswith(config.collection + "_")
                assert path in (f"pyudf_r1/{name}.whl", f"pyudf_r2/{name}.whl")
        except Exception as error:
            errors.append(error)
            continue
        for wheel in wheels:
            name, path = wheel["resource"], wheel["path"]

            def remove_registration(name=name):
                if name in {item.name for item in client.list_file_resources(timeout=30)}:
                    client.remove_file_resource(name, timeout=30)

            attempt("remove registration " + name, remove_registration)
            attempt("remove object " + path, lambda path=path: storage.remove_object(config.bucket, path))
    save(root / "cleanup.json", {"outcome": "FAIL" if errors else "PASS", "errors": [str(error) for error in errors]})
    if errors:
        raise ExceptionGroup("PyUDF scenario cleanup failed", errors)


@pytest.fixture
def pyudf_system_config(request):
    config_path = request.config.getoption("--pyudf-system-config")
    if config_path is None:
        pytest.fail("Supply --pyudf-system-config for an isolated two-Proxy deployment", pytrace=False)
    if request.config.getoption("numprocesses", default=0):
        pytest.fail("PyUDF system workloads share deployment capacity; run serially with -n 0", pytrace=False)
    missing = [name for name in ("MILVUS_TOKEN", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY") if not os.environ.get(name)]
    if missing:
        pytest.fail("Required environment variables: " + ", ".join(missing), pytrace=False)
    values = json.loads(config_path.read_text())
    values["collection"] = "pyudf_" + uuid.uuid4().hex
    expected = request.config.getoption("--pyudf-expected-commit")
    if expected:
        values["expected_commit"] = expected
    return WorkloadConfig(**values)


@pytest.fixture
def single_worker_proxies(pyudf_system_config):
    from workloads.pyudf.stress import verify_single_worker_proxies

    verify_single_worker_proxies(pyudf_system_config)


@pytest.fixture
def pyudf_system(request, tmp_path, record_property, pyudf_system_config):
    config = pyudf_system_config
    if hasattr(request, "param"):
        config = replace(config, index_type=request.param)
    parent = request.config.getoption("--pyudf-system-evidence") or tmp_path
    root = parent / (request.node.name + "_" + config.collection)
    root.mkdir(parents=True)
    save(root / "config.json", asdict(config))
    record_property("evidence_directory", str(root.resolve()))
    storage = config.storage()
    # A dedicated connection survives the workload phases closing their clients.
    client = MilvusClient(config.proxy_uris[0], token=os.environ["MILVUS_TOKEN"], dedicated=True, timeout=30)
    try:
        assert not client.has_collection(config.collection), "Refuse to own an existing collection"
        assert not any(item.name.startswith(config.collection + "_") for item in client.list_file_resources())
        try:
            initialize(root)
            yield root
        finally:
            (root / "STOP").touch()
            cleanup(client, storage, config, root)
    finally:
        client.close()
