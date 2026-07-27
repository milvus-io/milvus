import os
import uuid
from pathlib import Path

import pytest
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from minio import Minio
from pymilvus import MilvusClient

from spark_backfill.backfill_helpers import (
    create_backfill_collection,
    make_source_rows,
    parse_snapshot_metadata,
    persistent_segment_storage_versions,
    read_json_object,
    remove_object_prefix,
    unique_name,
)
from spark_backfill.case import BackfillCase, infer_root_path
from spark_backfill.config import SparkBackfillSettings
from spark_backfill.k8s_resources import (
    assert_rbac_permissions,
    build_ephemeral_secret,
    build_support_config_map,
)
from spark_backfill.k8s_runner import KubernetesSparkRunner, SparkRuntimeConfig
from spark_backfill.toolbox_runner import ToolboxRuntimeConfig, ToolboxSparkRunner


@pytest.fixture(scope="session")
def spark_backfill_settings(request):
    settings = SparkBackfillSettings.from_values(
        host=request.config.getoption("--host"),
        port=request.config.getoption("--port"),
        uri=request.config.getoption("--uri"),
        token=request.config.getoption("--token"),
        minio_host=request.config.getoption("--minio_host"),
        minio_bucket=request.config.getoption("--minio_bucket"),
        milvus_namespace=request.config.getoption("--milvus_ns"),
        management_endpoint=request.config.getoption("--management-endpoint"),
        spark_k8s_context=request.config.getoption("--spark-k8s-context"),
        spark_k8s_namespace=request.config.getoption("--spark-k8s-namespace"),
        spark_image=request.config.getoption("--spark-image"),
        connector_url=request.config.getoption("--spark-connector-url"),
        connector_sha256=request.config.getoption("--spark-connector-sha256"),
        spark_milvus_uri=request.config.getoption("--spark-milvus-uri"),
        spark_minio_endpoint=request.config.getoption("--spark-minio-endpoint"),
        storage_secret_name=request.config.getoption("--spark-storage-secret-name"),
        service_account_name=request.config.getoption("--spark-service-account-name"),
        job_timeout=request.config.getoption("--spark-job-timeout"),
        keep_failed_job=request.config.getoption("--spark-keep-failed-job"),
        evidence_root=request.config.getoption("--spark-evidence-root"),
        runner_mode=request.config.getoption("--spark-runner-mode"),
        toolbox_pod=request.config.getoption("--spark-toolbox-pod"),
        toolbox_label=request.config.getoption("--spark-toolbox-label"),
        toolbox_container=request.config.getoption("--spark-toolbox-container"),
        toolbox_wrapper=request.config.getoption("--spark-toolbox-wrapper"),
        toolbox_workspace=request.config.getoption("--spark-toolbox-workspace"),
    )
    settings.evidence_root.mkdir(parents=True, exist_ok=True)
    return settings


@pytest.fixture(scope="session")
def spark_storage_credentials():
    access_key = os.getenv("SPARK_BACKFILL_S3_ACCESS_KEY", "")
    secret_key = os.getenv("SPARK_BACKFILL_S3_SECRET_KEY", "")
    if bool(access_key) != bool(secret_key):
        pytest.fail("SPARK_BACKFILL_S3_ACCESS_KEY and SPARK_BACKFILL_S3_SECRET_KEY must both be set or both be empty")
    return access_key, secret_key


@pytest.fixture(scope="session")
def spark_k8s_apis(spark_backfill_settings):
    if spark_backfill_settings.spark_k8s_context:
        k8s_config.load_kube_config(context=spark_backfill_settings.spark_k8s_context)
    elif os.getenv("KUBERNETES_SERVICE_HOST"):
        k8s_config.load_incluster_config()
    else:
        k8s_config.load_kube_config()
    return (
        k8s_client.BatchV1Api(),
        k8s_client.CoreV1Api(),
        k8s_client.AuthorizationV1Api(),
    )


@pytest.fixture(scope="session")
def spark_support_resources(spark_backfill_settings, spark_storage_credentials, spark_k8s_apis):
    _, core_api, authorization_api = spark_k8s_apis
    namespace = spark_backfill_settings.spark_k8s_namespace
    if spark_backfill_settings.runner_mode == "toolbox":
        assert_rbac_permissions(
            authorization_api,
            namespace,
            create_secret=False,
            runner_mode="toolbox",
        )
        yield None, None
        return

    create_secret = not bool(spark_backfill_settings.storage_secret_name)
    assert_rbac_permissions(
        authorization_api,
        namespace,
        create_secret=create_secret,
        runner_mode="job",
    )

    suffix = uuid.uuid4().hex[:8]
    config_map_name = f"spark-backfill-support-{suffix}"
    package_dir = Path(__file__).parent
    files = {
        filename: (package_dir / filename).read_text(encoding="utf-8")
        for filename in ("contracts.py", "remote_entrypoint.py", "read_probe.py")
    }
    core_api.create_namespaced_config_map(
        namespace=namespace,
        body=build_support_config_map(config_map_name, files),
    )

    secret_name = spark_backfill_settings.storage_secret_name
    created_secret = False
    try:
        if not secret_name:
            secret_name = f"spark-backfill-secret-{suffix}"
            access_key, secret_key = spark_storage_credentials
            core_api.create_namespaced_secret(
                namespace=namespace,
                body=build_ephemeral_secret(
                    secret_name,
                    access_key=access_key,
                    secret_key=secret_key,
                    milvus_token=spark_backfill_settings.milvus_token,
                ),
            )
            created_secret = True
        yield config_map_name, secret_name
    finally:
        try:
            core_api.delete_namespaced_config_map(config_map_name, namespace)
        finally:
            if created_secret:
                core_api.delete_namespaced_secret(secret_name, namespace)


@pytest.fixture(scope="session")
def spark_job_runner(spark_backfill_settings, spark_k8s_apis, spark_support_resources):
    batch_api, core_api, _ = spark_k8s_apis
    if spark_backfill_settings.runner_mode == "toolbox":
        package_dir = Path(__file__).parent
        support_files = {
            filename: (package_dir / filename).read_text(encoding="utf-8")
            for filename in ("contracts.py", "read_probe.py")
        }
        return ToolboxSparkRunner(
            core_api,
            ToolboxRuntimeConfig(
                namespace=spark_backfill_settings.spark_k8s_namespace,
                pod_name=spark_backfill_settings.toolbox_pod,
                pod_label=spark_backfill_settings.toolbox_label,
                container=spark_backfill_settings.toolbox_container,
                wrapper_path=spark_backfill_settings.toolbox_wrapper,
                workspace_path=spark_backfill_settings.toolbox_workspace,
                timeout_seconds=spark_backfill_settings.job_timeout,
                evidence_root=spark_backfill_settings.evidence_root,
            ),
            support_files=support_files,
        )

    config_map_name, secret_name = spark_support_resources
    return KubernetesSparkRunner(
        batch_api,
        core_api,
        SparkRuntimeConfig(
            namespace=spark_backfill_settings.spark_k8s_namespace,
            image=spark_backfill_settings.spark_image,
            connector_url=spark_backfill_settings.connector_url,
            connector_sha256=spark_backfill_settings.connector_sha256,
            config_map_name=config_map_name,
            secret_name=secret_name,
            timeout_seconds=spark_backfill_settings.job_timeout,
            evidence_root=spark_backfill_settings.evidence_root,
            keep_failed_job=spark_backfill_settings.keep_failed_job,
            service_account_name=spark_backfill_settings.service_account_name or None,
        ),
    )


@pytest.fixture(scope="session")
def spark_minio_client(spark_backfill_settings, spark_storage_credentials):
    access_key, secret_key = spark_storage_credentials
    if not access_key:
        pytest.fail(
            "Static S3 credentials are required by the local MinIO client for the Spark Backfill suite"
        )
    return Minio(
        spark_backfill_settings.local_minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )


@pytest.fixture(scope="session")
def spark_milvus_client(spark_backfill_settings):
    client = MilvusClient(uri=spark_backfill_settings.local_milvus_uri, token=spark_backfill_settings.milvus_token)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def spark_backfill_case_factory(
    spark_backfill_settings,
    spark_milvus_client,
    spark_minio_client,
    spark_job_runner,
    tmp_path,
):
    resources = []

    def factory(*, expected_storage_kind, compaction_protection_seconds=0, flush_batch_size=10):
        collection_name = unique_name("spark_backfill")
        snapshot_names = []
        resource = {"collection_name": collection_name, "snapshot_names": snapshot_names, "prefix": ""}
        resources.append(resource)
        source_rows = make_source_rows()
        create_backfill_collection(spark_milvus_client, collection_name)
        for start in range(0, len(source_rows), flush_batch_size):
            spark_milvus_client.insert(collection_name, source_rows[start : start + flush_batch_size])
            spark_milvus_client.flush(collection_name)
        spark_milvus_client.load_collection(collection_name)

        snapshot_name = unique_name("spark_backfill_snapshot")
        spark_milvus_client.create_snapshot(
            snapshot_name,
            collection_name,
            compaction_protection_seconds=compaction_protection_seconds,
        )
        snapshot_names.append(snapshot_name)
        snapshot_info = spark_milvus_client.describe_snapshot(snapshot_name, collection_name)
        root_path = infer_root_path(snapshot_info.s3_location)
        prefix = "/".join(part for part in (root_path, "spark-backfill", uuid.uuid4().hex) if part)
        resource["prefix"] = prefix
        raw_metadata = read_json_object(
            spark_minio_client,
            spark_backfill_settings.minio_bucket,
            snapshot_info.s3_location,
        )
        raw_segment_ids = raw_metadata.get("segment_ids", raw_metadata.get("segmentIds", []))
        storage_versions = persistent_segment_storage_versions(
            spark_milvus_client,
            collection_name,
            raw_segment_ids,
        )
        snapshot = parse_snapshot_metadata(
            raw_metadata,
            snapshot_info.s3_location,
            segment_storage_versions=storage_versions,
        )
        if snapshot.storage_kind != expected_storage_kind:
            pytest.fail(
                f"Spark Backfill {expected_storage_kind.upper()} suite requires real "
                f"Storage {expected_storage_kind.upper()} segments; "
                f"Snapshot reported {snapshot.storage_kind!r}"
            )
        case = BackfillCase(
            client=spark_milvus_client,
            minio_client=spark_minio_client,
            runner=spark_job_runner,
            settings=spark_backfill_settings,
            tmp_path=tmp_path,
            collection_name=collection_name,
            snapshot_names=snapshot_names,
            snapshot_location=snapshot_info.s3_location,
            snapshot=snapshot,
            prefix=prefix,
            source_rows=source_rows,
        )
        return case

    yield factory

    for resource in reversed(resources):
        collection_name = resource["collection_name"]
        for snapshot_name in reversed(resource["snapshot_names"]):
            try:
                spark_milvus_client.drop_snapshot(snapshot_name, collection_name)
            except Exception:
                pass
        try:
            spark_milvus_client.drop_collection(collection_name)
        except Exception:
            pass
        if resource["prefix"]:
            try:
                remove_object_prefix(
                    spark_minio_client,
                    spark_backfill_settings.minio_bucket,
                    resource["prefix"],
                )
            except Exception:
                pass


@pytest.fixture
def backfill_case_factory(spark_backfill_case_factory):
    def factory(**kwargs):
        return spark_backfill_case_factory(expected_storage_kind="v3", **kwargs)

    return factory


@pytest.fixture
def backfill_v2_case_factory(spark_backfill_case_factory):
    def factory(**kwargs):
        return spark_backfill_case_factory(expected_storage_kind="v2", **kwargs)

    return factory
