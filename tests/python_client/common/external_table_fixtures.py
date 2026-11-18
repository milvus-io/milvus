"""Object-store fixtures shared by external-collection E2E modules."""

import random
import re
import time

import pytest
from common.external_table_common import (
    build_external_source,
    cleanup_minio_prefix,
    get_minio_config,
    new_minio_client,
)


@pytest.fixture(scope="module")
def minio_env(request):
    """Shared MinIO client + config for the module."""
    cfg = get_minio_config(
        minio_host=request.config.getoption("--minio_host"),
        minio_bucket=request.config.getoption("--minio_bucket"),
    )
    mc = new_minio_client(cfg)
    assert mc.bucket_exists(cfg["bucket"]), f"MinIO bucket {cfg['bucket']} not accessible at {cfg['address']}"
    return mc, cfg


@pytest.fixture(scope="function")
def external_prefix(minio_env, request):
    """Per-test MinIO prefix bundled with the resolved external_source URL."""
    mc, cfg = minio_env
    # Sanitize node.name aggressively — parametrize IDs can contain chars MinIO or
    # Milvus's S3 URI parser rejects. Keep only [A-Za-z0-9_-].
    safe = re.sub(r"[^A-Za-z0-9_-]", "_", request.node.name)
    key = f"external-e2e-core/{safe}-{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
    yield {"key": key, "url": build_external_source(cfg, key)}
    cleanup_minio_prefix(mc, cfg["bucket"], f"{key}/")
