"""Kubernetes Job orchestration for remote Spark local-mode executions."""

from __future__ import annotations

import json
import re
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_SPARK_IMAGE = (
    "apache/spark:4.0.1-scala2.13-java21-python3-ubuntu"
    "@sha256:fb5c5e61e7bb1be94b7f3a31afe1f73c5b4d20b6008f4ffa7278fc085da08a9e"
)
_DNS_LABEL_RE = re.compile(r"[^a-z0-9-]+")
_SENSITIVE_KEY_RE = re.compile(r"(secret|token|password|access.?key)", re.IGNORECASE)


@dataclass(frozen=True)
class SparkRuntimeConfig:
    namespace: str
    image: str
    connector_url: str
    connector_sha256: str
    config_map_name: str
    secret_name: str
    timeout_seconds: int
    evidence_root: Path
    keep_failed_job: bool = False
    service_account_name: str | None = None


@dataclass(frozen=True)
class SparkJobRequest:
    case_id: str
    operation: str
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class SparkJobResult:
    job_name: str
    pod_name: str | None
    succeeded: bool
    exit_code: int | None
    reason: str
    logs: str
    evidence_dir: str


class KubernetesSparkRunner:
    def __init__(self, batch_api, core_api, runtime: SparkRuntimeConfig, poll_interval_seconds: float = 2.0):
        self.batch_api = batch_api
        self.core_api = core_api
        self.runtime = runtime
        self.poll_interval_seconds = poll_interval_seconds

    def build_job_manifest(self, request: SparkJobRequest) -> dict[str, Any]:
        self._validate_request(request)
        job_name = self._new_job_name(request.case_id)
        environment = [
            {"name": "SPARK_BACKFILL_OPERATION", "value": request.operation},
            {"name": "SPARK_BACKFILL_JOB_SPEC_JSON", "value": json.dumps(request.payload, separators=(",", ":"))},
            {"name": "SPARK_BACKFILL_CONNECTOR_URL", "value": self.runtime.connector_url},
            {"name": "SPARK_BACKFILL_CONNECTOR_SHA256", "value": self.runtime.connector_sha256},
            {"name": "SPARK_BACKFILL_EXPECTED_SPARK_VERSION", "value": "4.0.1"},
            {"name": "SPARK_BACKFILL_EXPECTED_SCALA_VERSION", "value": "2.13"},
            {"name": "SPARK_BACKFILL_EXPECTED_JAVA_MAJOR", "value": "21"},
            {
                "name": "SPARK_BACKFILL_S3_ACCESS_KEY",
                "valueFrom": {
                    "secretKeyRef": {"name": self.runtime.secret_name, "key": "s3-access-key", "optional": True}
                },
            },
            {
                "name": "SPARK_BACKFILL_S3_SECRET_KEY",
                "valueFrom": {
                    "secretKeyRef": {"name": self.runtime.secret_name, "key": "s3-secret-key", "optional": True}
                },
            },
            {
                "name": "SPARK_BACKFILL_MILVUS_TOKEN",
                "valueFrom": {
                    "secretKeyRef": {"name": self.runtime.secret_name, "key": "milvus-token", "optional": True}
                },
            },
        ]
        pod_spec: dict[str, Any] = {
            "restartPolicy": "Never",
            "nodeSelector": {"kubernetes.io/arch": "amd64"},
            "containers": [
                {
                    "name": "spark-job",
                    "image": self.runtime.image,
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["python3", "/opt/spark-backfill/remote_entrypoint.py"],
                    "env": environment,
                    "resources": {
                        "requests": {"cpu": "2", "memory": "8Gi"},
                        "limits": {"cpu": "2", "memory": "8Gi"},
                    },
                    "volumeMounts": [
                        {"name": "support", "mountPath": "/opt/spark-backfill", "readOnly": True},
                        {"name": "work", "mountPath": "/tmp/spark-backfill"},
                        {"name": "spark-local", "mountPath": "/tmp/spark-local"},
                    ],
                }
            ],
            "volumes": [
                {"name": "support", "configMap": {"name": self.runtime.config_map_name}},
                {"name": "work", "emptyDir": {}},
                {"name": "spark-local", "emptyDir": {}},
            ],
        }
        if self.runtime.service_account_name:
            pod_spec["serviceAccountName"] = self.runtime.service_account_name

        return {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "namespace": self.runtime.namespace,
                "labels": {"app": "spark-milvus-backfill", "spark-backfill-case": self._label(request.case_id)},
            },
            "spec": {
                "backoffLimit": 0,
                "activeDeadlineSeconds": self.runtime.timeout_seconds,
                "ttlSecondsAfterFinished": 3600,
                "template": {
                    "metadata": {"labels": {"app": "spark-milvus-backfill", "job-name": job_name}},
                    "spec": pod_spec,
                },
            },
        }

    def run(self, request: SparkJobRequest) -> SparkJobResult:
        manifest = self.build_job_manifest(request)
        job_name = manifest["metadata"]["name"]
        evidence_dir = self.runtime.evidence_root / job_name
        evidence_dir.mkdir(parents=True, exist_ok=False)
        self._write_json(evidence_dir / "job.json", self._redact(manifest))

        self.batch_api.create_namespaced_job(namespace=self.runtime.namespace, body=manifest)
        started = time.monotonic()
        timed_out = False
        status = None
        try:
            while True:
                status = self.batch_api.read_namespaced_job_status(job_name, self.runtime.namespace).status
                if getattr(status, "succeeded", None) or getattr(status, "failed", None):
                    break
                if time.monotonic() - started >= self.runtime.timeout_seconds:
                    timed_out = True
                    break
                time.sleep(self.poll_interval_seconds)

            pod_name, logs, exit_code, pod_reason = self._collect_pod_evidence(job_name)
            (evidence_dir / "pod.log").write_text(logs, encoding="utf-8")

            if timed_out:
                result = SparkJobResult(
                    job_name=job_name,
                    pod_name=pod_name,
                    succeeded=False,
                    exit_code=exit_code,
                    reason="TimedOut",
                    logs=logs,
                    evidence_dir=str(evidence_dir),
                )
                self._write_json(evidence_dir / "result.json", result.__dict__)
                raise TimeoutError(f"Spark Kubernetes Job {job_name} timed out after {self.runtime.timeout_seconds}s")

            succeeded = bool(getattr(status, "succeeded", None)) and exit_code in (None, 0)
            reason = "Completed" if succeeded else pod_reason or "Failed"
            result = SparkJobResult(
                job_name=job_name,
                pod_name=pod_name,
                succeeded=succeeded,
                exit_code=exit_code,
                reason=reason,
                logs=logs,
                evidence_dir=str(evidence_dir),
            )
            self._write_json(evidence_dir / "result.json", result.__dict__)
            return result
        finally:
            failed = timed_out or status is None or not bool(getattr(status, "succeeded", None))
            if timed_out or not (failed and self.runtime.keep_failed_job):
                self.batch_api.delete_namespaced_job(
                    job_name,
                    self.runtime.namespace,
                    propagation_policy="Background",
                )

    def _collect_pod_evidence(self, job_name: str) -> tuple[str | None, str, int | None, str]:
        pods = self.core_api.list_namespaced_pod(
            namespace=self.runtime.namespace,
            label_selector=f"job-name={job_name}",
        ).items
        if not pods:
            return None, "", None, "PodNotFound"
        pod = pods[0]
        pod_name = pod.metadata.name
        logs = self.core_api.read_namespaced_pod_log(
            name=pod_name,
            namespace=self.runtime.namespace,
            container="spark-job",
        )
        exit_code = None
        reason = getattr(pod.status, "phase", "") or ""
        for container_status in getattr(pod.status, "container_statuses", None) or []:
            if container_status.name != "spark-job":
                continue
            terminated = getattr(container_status.state, "terminated", None)
            if terminated is not None:
                exit_code = getattr(terminated, "exit_code", None)
                reason = getattr(terminated, "reason", None) or reason
        return pod_name, logs or "", exit_code, reason

    @staticmethod
    def _validate_request(request: SparkJobRequest) -> None:
        if request.operation not in {"backfill", "read"}:
            raise ValueError(f"unsupported Spark operation: {request.operation!r}")
        KubernetesSparkRunner._reject_sensitive_keys(request.payload)

    @staticmethod
    def _reject_sensitive_keys(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                if _SENSITIVE_KEY_RE.search(str(key)):
                    raise ValueError(f"sensitive value {key!r} must be provided through a Kubernetes Secret")
                KubernetesSparkRunner._reject_sensitive_keys(item)
        elif isinstance(value, list):
            for item in value:
                KubernetesSparkRunner._reject_sensitive_keys(item)

    @staticmethod
    def _new_job_name(case_id: str) -> str:
        label = KubernetesSparkRunner._label(case_id)[:35].strip("-") or "case"
        return f"spark-backfill-{label}-{uuid.uuid4().hex[:8]}"

    @staticmethod
    def _label(value: str) -> str:
        return _DNS_LABEL_RE.sub("-", value.lower()).strip("-")[:63] or "case"

    @classmethod
    def _redact(cls, value: Any, key: str = "") -> Any:
        if _SENSITIVE_KEY_RE.search(key):
            return "<redacted>"
        if isinstance(value, Mapping):
            return {item_key: cls._redact(item_value, str(item_key)) for item_key, item_value in value.items()}
        if isinstance(value, list):
            return [cls._redact(item) for item in value]
        return value

    @staticmethod
    def _write_json(path: Path, value: Any) -> None:
        path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str), encoding="utf-8")
