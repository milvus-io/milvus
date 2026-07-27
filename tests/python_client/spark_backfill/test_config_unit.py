from pathlib import Path

import pytest

from spark_backfill.config import (
    SparkBackfillConfigurationError,
    SparkBackfillSettings,
    ensure_serial_execution,
    is_spark_backfill_path,
)


def test_settings_reuse_existing_pytest_endpoints_by_default(tmp_path):
    settings = SparkBackfillSettings.from_values(
        host="milvus.example",
        port=19530,
        uri="",
        token="root:Milvus",
        minio_host="minio.example",
        minio_bucket="bucket",
        milvus_namespace="chaos-testing",
        management_endpoint="http://milvus.example:9091/",
        spark_k8s_context="ctx",
        spark_k8s_namespace="",
        spark_image="apache/spark:4.0.1@sha256:abc",
        connector_url="https://artifacts.example/connector.tar.gz",
        connector_sha256="a" * 64,
        spark_milvus_uri="",
        spark_minio_endpoint="",
        storage_secret_name="",
        service_account_name="",
        job_timeout=1800,
        keep_failed_job=False,
        evidence_root=str(tmp_path),
    )

    assert settings.local_milvus_uri == "http://milvus.example:19530"
    assert settings.spark_milvus_uri == "http://milvus.example:19530"
    assert settings.local_minio_endpoint == "minio.example:9000"
    assert settings.spark_minio_endpoint == "minio.example:9000"
    assert settings.spark_k8s_namespace == "chaos-testing"
    assert settings.management_endpoint == "http://milvus.example:9091"
    assert settings.evidence_root == Path(tmp_path)


def test_toolbox_settings_do_not_require_connector_bundle(tmp_path):
    settings = SparkBackfillSettings.from_values(
        host="milvus.example",
        port=19530,
        uri="",
        token="root:Milvus",
        minio_host="minio.example",
        minio_bucket="bucket",
        milvus_namespace="default",
        management_endpoint="http://milvus.example:9091",
        spark_k8s_context="ctx",
        spark_k8s_namespace="default",
        spark_image="",
        connector_url="",
        connector_sha256="",
        spark_milvus_uri="http://milvus.default:19530",
        spark_minio_endpoint="minio.default:9000",
        storage_secret_name="",
        service_account_name="",
        job_timeout=1800,
        keep_failed_job=False,
        evidence_root=str(tmp_path),
        runner_mode="toolbox",
        toolbox_pod="",
        toolbox_label="app=spark-milvus-toolbox",
        toolbox_container="spark-toolbox",
        toolbox_wrapper="/usr/local/bin/spark-submit-milvus",
        toolbox_workspace="/workspace/spark-backfill-pytest",
    )

    assert settings.runner_mode == "toolbox"
    assert settings.connector_url == ""
    assert settings.connector_sha256 == ""
    assert settings.toolbox_pod == ""
    assert settings.toolbox_label == "app=spark-milvus-toolbox"
    assert settings.toolbox_container == "spark-toolbox"
    assert settings.toolbox_wrapper == "/usr/local/bin/spark-submit-milvus"
    assert settings.toolbox_workspace == "/workspace/spark-backfill-pytest"


def test_toolbox_settings_require_pod_or_label(tmp_path):
    with pytest.raises(SparkBackfillConfigurationError, match="Toolbox Pod name or label"):
        SparkBackfillSettings.from_values(
            host="milvus",
            port=19530,
            uri="",
            token="root:Milvus",
            minio_host="minio",
            minio_bucket="bucket",
            milvus_namespace="default",
            management_endpoint="http://milvus:9091",
            spark_k8s_context="",
            spark_k8s_namespace="default",
            spark_image="",
            connector_url="",
            connector_sha256="",
            spark_milvus_uri="",
            spark_minio_endpoint="",
            storage_secret_name="",
            service_account_name="",
            job_timeout=1800,
            keep_failed_job=False,
            evidence_root=str(tmp_path),
            runner_mode="toolbox",
            toolbox_pod="",
            toolbox_label="",
            toolbox_container="spark-toolbox",
            toolbox_wrapper="/usr/local/bin/spark-submit-milvus",
            toolbox_workspace="/workspace/spark-backfill-pytest",
        )


def test_settings_reject_unknown_runner_mode(tmp_path):
    with pytest.raises(SparkBackfillConfigurationError, match="runner mode"):
        SparkBackfillSettings.from_values(
            host="milvus",
            port=19530,
            uri="",
            token="root:Milvus",
            minio_host="minio",
            minio_bucket="bucket",
            milvus_namespace="default",
            management_endpoint="http://milvus:9091",
            spark_k8s_context="",
            spark_k8s_namespace="default",
            spark_image="",
            connector_url="",
            connector_sha256="",
            spark_milvus_uri="",
            spark_minio_endpoint="",
            storage_secret_name="",
            service_account_name="",
            job_timeout=1800,
            keep_failed_job=False,
            evidence_root=str(tmp_path),
            runner_mode="cluster",
            toolbox_pod="",
            toolbox_label="app=spark-milvus-toolbox",
            toolbox_container="spark-toolbox",
            toolbox_wrapper="/usr/local/bin/spark-submit-milvus",
            toolbox_workspace="/workspace/spark-backfill-pytest",
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("management_endpoint", "", "management endpoint"),
        ("connector_url", "", "Connector bundle URL"),
        ("connector_sha256", "bad", "Connector bundle SHA256"),
    ],
)
def test_settings_reject_missing_required_nightly_configuration(tmp_path, field, value, message):
    values = {
        "host": "milvus",
        "port": 19530,
        "uri": "",
        "token": "root:Milvus",
        "minio_host": "minio",
        "minio_bucket": "bucket",
        "milvus_namespace": "ns",
        "management_endpoint": "http://milvus:9091",
        "spark_k8s_context": "",
        "spark_k8s_namespace": "ns",
        "spark_image": "spark",
        "connector_url": "https://example/connector.tar.gz",
        "connector_sha256": "a" * 64,
        "spark_milvus_uri": "",
        "spark_minio_endpoint": "",
        "storage_secret_name": "",
        "service_account_name": "",
        "job_timeout": 1800,
        "keep_failed_job": False,
        "evidence_root": str(tmp_path),
    }
    values[field] = value

    with pytest.raises(SparkBackfillConfigurationError, match=message):
        SparkBackfillSettings.from_values(**values)


def test_spark_backfill_path_detection_is_directory_specific():
    assert is_spark_backfill_path("/repo/tests/python_client/spark_backfill/test_x.py")
    assert not is_spark_backfill_path("/repo/tests/python_client/milvus_client/test_x.py")


@pytest.mark.parametrize("workers", [1, 2, 6, "auto"])
def test_spark_backfill_rejects_xdist(workers):
    with pytest.raises(SparkBackfillConfigurationError, match="pytest-xdist"):
        ensure_serial_execution(workers)


@pytest.mark.parametrize("workers", [None, 0, "0"])
def test_spark_backfill_accepts_serial_execution(workers):
    ensure_serial_execution(workers)
