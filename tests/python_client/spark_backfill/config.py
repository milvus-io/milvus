"""Configuration normalization for the Spark Backfill pytest runners."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path, PurePath

from .k8s_runner import DEFAULT_SPARK_IMAGE

_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")


class SparkBackfillConfigurationError(ValueError):
    """Required external environment configuration is missing or unsafe."""


@dataclass(frozen=True)
class SparkBackfillSettings:
    runner_mode: str
    local_milvus_uri: str
    spark_milvus_uri: str
    milvus_token: str
    local_minio_endpoint: str
    spark_minio_endpoint: str
    minio_bucket: str
    management_endpoint: str
    spark_k8s_context: str
    spark_k8s_namespace: str
    spark_image: str
    connector_url: str
    connector_sha256: str
    storage_secret_name: str
    service_account_name: str
    job_timeout: int
    keep_failed_job: bool
    evidence_root: Path
    toolbox_pod: str
    toolbox_label: str
    toolbox_container: str
    toolbox_wrapper: str
    toolbox_workspace: str

    @classmethod
    def from_values(
        cls,
        *,
        host,
        port,
        uri,
        token,
        minio_host,
        minio_bucket,
        milvus_namespace,
        management_endpoint,
        spark_k8s_context,
        spark_k8s_namespace,
        spark_image,
        connector_url,
        connector_sha256,
        spark_milvus_uri,
        spark_minio_endpoint,
        storage_secret_name,
        service_account_name,
        job_timeout,
        keep_failed_job,
        evidence_root,
        runner_mode="job",
        toolbox_pod="",
        toolbox_label="app=spark-milvus-toolbox",
        toolbox_container="spark-toolbox",
        toolbox_wrapper="/usr/local/bin/spark-submit-milvus",
        toolbox_workspace="/workspace/spark-backfill-pytest",
    ) -> SparkBackfillSettings:
        local_milvus_uri = str(uri).strip() or f"http://{host}:{port}"
        management_endpoint = str(management_endpoint).strip().rstrip("/")
        connector_url = str(connector_url).strip()
        connector_sha256 = str(connector_sha256).strip()
        namespace = str(spark_k8s_namespace).strip() or str(milvus_namespace).strip()
        timeout = int(job_timeout)
        runner_mode = str(runner_mode).strip().lower() or "job"
        toolbox_pod = str(toolbox_pod).strip()
        toolbox_label = str(toolbox_label).strip()
        toolbox_container = str(toolbox_container).strip()
        toolbox_wrapper = str(toolbox_wrapper).strip()
        toolbox_workspace = str(toolbox_workspace).strip().rstrip("/")

        if not management_endpoint.startswith(("http://", "https://")):
            raise SparkBackfillConfigurationError("management endpoint must be an absolute HTTP(S) URL")
        if runner_mode not in {"job", "toolbox"}:
            raise SparkBackfillConfigurationError("Spark runner mode must be 'job' or 'toolbox'")
        if runner_mode == "job":
            if not connector_url.startswith("https://"):
                raise SparkBackfillConfigurationError("Connector bundle URL must use HTTPS")
            if not _SHA256_RE.fullmatch(connector_sha256):
                raise SparkBackfillConfigurationError("Connector bundle SHA256 must contain 64 hexadecimal characters")
        elif not toolbox_pod and not toolbox_label:
            raise SparkBackfillConfigurationError("Toolbox Pod name or label is required")
        if not namespace:
            raise SparkBackfillConfigurationError("Spark Kubernetes namespace is required")
        if timeout <= 0:
            raise SparkBackfillConfigurationError("Spark Job timeout must be positive")

        return cls(
            runner_mode=runner_mode,
            local_milvus_uri=local_milvus_uri,
            spark_milvus_uri=str(spark_milvus_uri).strip() or local_milvus_uri,
            milvus_token=str(token),
            local_minio_endpoint=_minio_endpoint(str(minio_host)),
            spark_minio_endpoint=_minio_endpoint(str(spark_minio_endpoint).strip() or str(minio_host)),
            minio_bucket=str(minio_bucket),
            management_endpoint=management_endpoint,
            spark_k8s_context=str(spark_k8s_context).strip(),
            spark_k8s_namespace=namespace,
            spark_image=str(spark_image).strip() or DEFAULT_SPARK_IMAGE,
            connector_url=connector_url,
            connector_sha256=connector_sha256.lower(),
            storage_secret_name=str(storage_secret_name).strip(),
            service_account_name=str(service_account_name).strip(),
            job_timeout=timeout,
            keep_failed_job=bool(keep_failed_job),
            evidence_root=Path(evidence_root).expanduser().resolve(),
            toolbox_pod=toolbox_pod,
            toolbox_label=toolbox_label,
            toolbox_container=toolbox_container,
            toolbox_wrapper=toolbox_wrapper,
            toolbox_workspace=toolbox_workspace,
        )


@dataclass(frozen=True)
class SparkJobsSettings:
    """Configuration for the managed Spark Batch Jobs API E2E suite."""

    endpoint: str
    api_key: str
    project_id: str
    region_id: str
    volume_name: str
    output_volume_name: str
    input_path: str
    output_path: str
    artifact_path: str
    volume_bucket: str
    volume_root: str
    minio_endpoint: str
    storage_secure: bool
    evidence_root: Path
    job_timeout: int
    poll_interval: float

    @classmethod
    def from_values(
        cls,
        *,
        endpoint,
        api_key,
        project_id,
        region_id,
        volume_name,
        output_volume_name,
        input_path,
        output_path,
        artifact_path,
        volume_bucket,
        volume_root,
        minio_host,
        storage_secure,
        evidence_root,
        job_timeout,
        poll_interval,
    ) -> SparkJobsSettings:
        endpoint = str(endpoint).strip().rstrip("/")
        if not endpoint.startswith(("http://", "https://")):
            raise SparkBackfillConfigurationError("Spark Batch Jobs endpoint must be an absolute HTTP(S) URL")
        if not str(api_key).strip():
            raise SparkBackfillConfigurationError("Spark Batch Jobs API key is required")
        if not str(project_id).strip():
            raise SparkBackfillConfigurationError("Spark Batch Jobs projectId is required")
        if not str(region_id).strip():
            raise SparkBackfillConfigurationError("Spark Batch Jobs regionId is required")
        if not str(volume_name).strip():
            raise SparkBackfillConfigurationError("Spark Batch Jobs volumeName is required")
        timeout = int(job_timeout)
        if timeout <= 0:
            raise SparkBackfillConfigurationError("Spark Batch Jobs timeout must be positive")
        return cls(
            endpoint=endpoint,
            api_key=str(api_key).strip(),
            project_id=str(project_id).strip(),
            region_id=str(region_id).strip(),
            volume_name=str(volume_name).strip(),
            output_volume_name=str(output_volume_name).strip() or str(volume_name).strip(),
            input_path=str(input_path).strip().strip("/"),
            output_path=str(output_path).strip().strip("/"),
            artifact_path=str(artifact_path).strip().strip("/"),
            volume_bucket=str(volume_bucket).strip(),
            volume_root=str(volume_root).strip().strip("/"),
            minio_endpoint=_minio_endpoint(str(minio_host)),
            storage_secure=bool(storage_secure),
            evidence_root=Path(evidence_root).expanduser().resolve(),
            job_timeout=timeout,
            poll_interval=float(poll_interval),
        )


def _minio_endpoint(value: str) -> str:
    endpoint = value.strip().removeprefix("s3://").removeprefix("http://").removeprefix("https://").rstrip("/")
    if not endpoint:
        raise SparkBackfillConfigurationError("storage endpoint is required")
    if "/" in endpoint:
        raise SparkBackfillConfigurationError(
            "storage endpoint must be a bare host[:port] with no path; "
            f"put the bucket in --spark-jobs-volume-bucket and any prefix in --spark-jobs-volume-root, got {value!r}"
        )
    if ":" not in endpoint and "." not in endpoint:
        endpoint = f"{endpoint}:9000"
    return endpoint


def is_spark_backfill_path(path) -> bool:
    return "spark_backfill" in PurePath(str(path)).parts


def ensure_serial_execution(numprocesses) -> None:
    if numprocesses not in (None, 0, "0"):
        raise SparkBackfillConfigurationError(
            "Spark Backfill tests do not support pytest-xdist; run with -n 0 or without -n"
        )
