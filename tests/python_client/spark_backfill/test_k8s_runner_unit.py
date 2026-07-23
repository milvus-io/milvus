import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from spark_backfill.k8s_runner import (
    KubernetesSparkRunner,
    SparkJobRequest,
    SparkRuntimeConfig,
)


class FakeBatchApi:
    def __init__(self, statuses):
        self.statuses = list(statuses)
        self.created = []
        self.deleted = []

    def create_namespaced_job(self, namespace, body):
        self.created.append((namespace, body))
        return SimpleNamespace(metadata=SimpleNamespace(name=body["metadata"]["name"]))

    def read_namespaced_job_status(self, name, namespace):
        status = self.statuses.pop(0)
        return SimpleNamespace(status=SimpleNamespace(**status))

    def delete_namespaced_job(self, name, namespace, propagation_policy):
        self.deleted.append((namespace, name, propagation_policy))


class FakeCoreApi:
    def __init__(self, logs="driver logs", exit_code=0):
        terminated = SimpleNamespace(exit_code=exit_code, reason="Completed" if exit_code == 0 else "Error")
        state = SimpleNamespace(terminated=terminated)
        container_status = SimpleNamespace(name="spark-job", state=state)
        self.pod = SimpleNamespace(
            metadata=SimpleNamespace(name="spark-backfill-case-pod"),
            status=SimpleNamespace(
                container_statuses=[container_status], phase="Succeeded" if exit_code == 0 else "Failed"
            ),
        )
        self.logs = logs

    def list_namespaced_pod(self, namespace, label_selector):
        return SimpleNamespace(items=[self.pod])

    def read_namespaced_pod_log(self, name, namespace, container):
        return self.logs


def _runtime(tmp_path, **overrides):
    values = {
        "namespace": "spark-tests",
        "image": "apache/spark:4.0.1@sha256:abc",
        "connector_url": "https://artifacts.example/spark-milvus.tar.gz",
        "connector_sha256": "a" * 64,
        "config_map_name": "spark-backfill-support",
        "secret_name": "spark-backfill-secret",
        "timeout_seconds": 60,
        "evidence_root": tmp_path,
    }
    values.update(overrides)
    return SparkRuntimeConfig(**values)


def test_job_manifest_uses_secret_refs_and_fixed_runtime(tmp_path):
    runner = KubernetesSparkRunner(FakeBatchApi([]), FakeCoreApi(), _runtime(tmp_path))
    request = SparkJobRequest(
        case_id="coalesce",
        operation="backfill",
        payload={"arguments": ["--mode", "coalesce"]},
    )

    job = runner.build_job_manifest(request)

    pod_spec = job["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]
    assert job["spec"]["backoffLimit"] == 0
    assert job["spec"]["activeDeadlineSeconds"] == 60
    assert pod_spec["nodeSelector"] == {"kubernetes.io/arch": "amd64"}
    assert pod_spec["restartPolicy"] == "Never"
    assert container["resources"]["requests"] == {"cpu": "2", "memory": "8Gi"}
    assert container["imagePullPolicy"] == "IfNotPresent"
    assert container["command"] == ["python3", "/opt/spark-backfill/remote_entrypoint.py"]

    rendered = json.dumps(job)
    assert "secretKeyRef" in rendered
    assert "spark-backfill-secret" in rendered
    secret_env = {
        item["name"]: item
        for item in container["env"]
        if item["name"] in {"SPARK_BACKFILL_S3_ACCESS_KEY", "SPARK_BACKFILL_S3_SECRET_KEY"}
    }
    assert set(secret_env) == {"SPARK_BACKFILL_S3_ACCESS_KEY", "SPARK_BACKFILL_S3_SECRET_KEY"}
    assert all("value" not in item and "secretKeyRef" in item["valueFrom"] for item in secret_env.values())


def test_job_manifest_rejects_nested_plaintext_credentials(tmp_path):
    runner = KubernetesSparkRunner(FakeBatchApi([]), FakeCoreApi(), _runtime(tmp_path))

    with pytest.raises(ValueError, match="sensitive value"):
        runner.build_job_manifest(
            SparkJobRequest(
                case_id="read",
                operation="read",
                payload={"options": {"milvus.token": "root:Milvus"}},
            )
        )


def test_runner_collects_evidence_and_deletes_successful_job(tmp_path):
    batch = FakeBatchApi([{"succeeded": None, "failed": None}, {"succeeded": 1, "failed": None}])
    core = FakeCoreApi(logs="spark completed", exit_code=0)
    runner = KubernetesSparkRunner(batch, core, _runtime(tmp_path), poll_interval_seconds=0)

    result = runner.run(SparkJobRequest(case_id="read", operation="read", payload={"options": {}}))

    assert result.succeeded is True
    assert result.exit_code == 0
    assert result.logs == "spark completed"
    assert batch.deleted == [("spark-tests", result.job_name, "Background")]
    assert (Path(result.evidence_dir) / "job.json").is_file()
    assert (Path(result.evidence_dir) / "pod.log").read_text() == "spark completed"


def test_runner_can_keep_failed_job_for_debugging(tmp_path):
    batch = FakeBatchApi([{"succeeded": None, "failed": 1}])
    core = FakeCoreApi(logs="Backfill FAILED", exit_code=1)
    runner = KubernetesSparkRunner(
        batch,
        core,
        _runtime(tmp_path, keep_failed_job=True),
        poll_interval_seconds=0,
    )

    result = runner.run(SparkJobRequest(case_id="bad-pk", operation="backfill", payload={"arguments": []}))

    assert result.succeeded is False
    assert result.exit_code == 1
    assert batch.deleted == []
    assert (Path(result.evidence_dir) / "pod.log").read_text() == "Backfill FAILED"


def test_runner_deletes_timed_out_job_after_collecting_evidence(tmp_path, monkeypatch):
    batch = FakeBatchApi([{"succeeded": None, "failed": None}] * 3)
    core = FakeCoreApi(logs="still running", exit_code=137)
    runner = KubernetesSparkRunner(batch, core, _runtime(tmp_path, timeout_seconds=1), poll_interval_seconds=0)
    times = iter([0.0, 0.0, 2.0])
    monkeypatch.setattr("spark_backfill.k8s_runner.time.monotonic", lambda: next(times))

    with pytest.raises(TimeoutError, match="timed out"):
        runner.run(SparkJobRequest(case_id="timeout", operation="backfill", payload={"arguments": []}))

    assert len(batch.deleted) == 1
    evidence_dirs = list(tmp_path.iterdir())
    assert len(evidence_dirs) == 1
    assert (evidence_dirs[0] / "pod.log").read_text() == "still running"
