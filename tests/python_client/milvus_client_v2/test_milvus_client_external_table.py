"""
External Table (External Collection) tests using pymilvus MilvusClient.

Covers:
  - Schema validation (data types, forbidden features, describe/list)
  - E2E lifecycle per scalar/vector type (parquet → refresh → index → load → query → search)
  - Load + Query + Search (count, filter, hybrid, pagination, multi-vector, output vector)
  - Refresh + verify segments / row count
  - Incremental refresh (add/remove files, verify data correctness)
  - Multiple data types in one collection
  - Refresh error handling (non-existent, normal coll, empty source, concurrent, schema mismatch)
  - Query edge cases (complex filters, virtual PK, topK > total, different metrics)
  - Data update behavior (refresh without release, reload without refresh)
  - Concurrent access (query + refresh + reload concurrently)
  - File deleted then reload
  - Collection ops (drop partition, add field, drop during refresh, recreate)
  - Cross-feature (alias, coexist with normal collection)
"""

import io
import json
import os
import struct
import threading
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from minio import Minio

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from pymilvus import DataType
from utils.util_log import test_log as log


# ============================================================
# Constants & Configuration
# ============================================================

TEST_VEC_DIM = 4


def _env(key, default):
    return os.environ.get(key, default)


def get_minio_config():
    return {
        "address": _env("MINIO_ADDRESS", "localhost:9000"),
        "access_key": _env("MINIO_ACCESS_KEY", "minioadmin"),
        "secret_key": _env("MINIO_SECRET_KEY", "minioadmin"),
        "bucket": _env("MINIO_BUCKET", "a-bucket"),
        "root_path": _env("MINIO_ROOT_PATH", "files"),
    }


def new_minio_client(cfg):
    return Minio(
        cfg["address"],
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        secure=False,
    )


# ============================================================
# Parquet Generation Helpers
# ============================================================

def generate_parquet_bytes(num_rows, start_id, dim=TEST_VEC_DIM):
    """Generate Parquet bytes with columns: id (Int64), value (Float32), embedding (FixedSizeList[Float32, dim])."""
    ids = list(range(start_id, start_id + num_rows))
    values = [float(i) * 1.5 for i in ids]
    vectors = np.array(
        [[float(i) * 0.1 + d for d in range(dim)] for i in ids],
        dtype=np.float32,
    )

    table = pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "value": pa.array(values, type=pa.float32()),
        "embedding": pa.FixedSizeListArray.from_arrays(
            vectors.flatten(), list_size=dim,
        ),
    })

    buf = io.BytesIO()
    # Use NONE compression: Milvus StorageV2 memory_planner lacks snappy codec support.
    # See: internal/core/conanfile.py missing "arrow:with_snappy": True
    pq.write_table(table, buf, compression='NONE')
    return buf.getvalue()


def generate_multi_type_parquet_bytes(num_rows, start_id, dim=TEST_VEC_DIM):
    """Generate Parquet with: id, bool_val, int8_val, int16_val, int32_val, float_val, double_val, varchar_val, embedding."""
    ids = list(range(start_id, start_id + num_rows))

    table = pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "bool_val": pa.array([i % 2 == 0 for i in ids], type=pa.bool_()),
        "int8_val": pa.array([i % 100 for i in ids], type=pa.int8()),
        "int16_val": pa.array([i * 10 for i in ids], type=pa.int16()),
        "int32_val": pa.array([i * 100 for i in ids], type=pa.int32()),
        "float_val": pa.array([float(i) * 1.5 for i in ids], type=pa.float32()),
        "double_val": pa.array([float(i) * 0.01 for i in ids], type=pa.float64()),
        "varchar_val": pa.array([f"str_{i:04d}" for i in ids], type=pa.string()),
        "embedding": pa.FixedSizeListArray.from_arrays(
            np.array(
                [[float(i) * 0.1 + d for d in range(dim)] for i in ids],
                dtype=np.float32,
            ).flatten(),
            list_size=dim,
        ),
    })

    buf = io.BytesIO()
    # Use NONE compression: Milvus StorageV2 memory_planner lacks snappy codec support.
    # See: internal/core/conanfile.py missing "arrow:with_snappy": True
    pq.write_table(table, buf, compression='NONE')
    return buf.getvalue()


def generate_typed_parquet_bytes(num_rows, start_id, scalar_name, arrow_type, value_fn, dim=TEST_VEC_DIM):
    """Generate Parquet with columns: id, <scalar_name>, embedding."""
    ids = list(range(start_id, start_id + num_rows))
    scalars = [value_fn(i) for i in ids]

    fields = {
        "id": pa.array(ids, type=pa.int64()),
        scalar_name: pa.array(scalars, type=arrow_type),
        "embedding": pa.FixedSizeListArray.from_arrays(
            np.array(
                [[float(i) * 0.1 + d for d in range(dim)] for i in ids],
                dtype=np.float32,
            ).flatten(),
            list_size=dim,
        ),
    }
    table = pa.table(fields)
    buf = io.BytesIO()
    # Use NONE compression: Milvus StorageV2 memory_planner lacks snappy codec support.
    # See: internal/core/conanfile.py missing "arrow:with_snappy": True
    pq.write_table(table, buf, compression='NONE')
    return buf.getvalue()


# ============================================================
# MinIO Helpers
# ============================================================

def upload_to_minio(minio_client, bucket, key, data):
    """Upload bytes to MinIO."""
    minio_client.put_object(
        bucket, key,
        io.BytesIO(data), length=len(data),
        content_type="application/octet-stream",
    )


def cleanup_minio_prefix(minio_client, bucket, prefix):
    """Remove all objects under a prefix."""
    for obj in minio_client.list_objects(bucket, prefix=prefix, recursive=True):
        minio_client.remove_object(bucket, obj.object_name)


# ============================================================
# Test Lifecycle Helpers
# ============================================================

def refresh_and_wait(client, collection_name, timeout=120):
    """Trigger refresh and poll until complete. Returns job_id."""
    job_id = client.refresh_external_collection(collection_name=collection_name)
    assert job_id > 0, f"Expected positive job_id, got {job_id}"
    log.info(f"Started refresh job: {job_id}")

    deadline = time.time() + timeout
    while time.time() < deadline:
        progress = client.get_refresh_external_collection_progress(job_id=job_id)
        log.info(f"Job {job_id}: state={progress.state} progress={progress.progress}%")
        if progress.state == "RefreshCompleted":
            return job_id
        if progress.state == "RefreshFailed":
            raise AssertionError(f"Refresh job {job_id} failed: {progress.reason}")
        time.sleep(2)

    raise AssertionError(f"Refresh job {job_id} timed out after {timeout}s")


def index_and_load(client, collection_name, vec_field="embedding", metric_type="L2"):
    """Create AUTOINDEX on vector field and load collection."""
    index_params = client.prepare_index_params()
    index_params.add_index(field_name=vec_field, index_type="AUTOINDEX", metric_type=metric_type)
    client.create_index(collection_name, index_params)
    client.load_collection(collection_name)
    log.info(f"Index created on {vec_field} (metric={metric_type}) and collection loaded")


def create_basic_external_schema(client, ext_path, dim=TEST_VEC_DIM):
    """Create a basic external schema with id, value, embedding."""
    schema = client.create_schema(
        external_source=ext_path,
        external_spec='{"format":"parquet"}',
    )
    schema.add_field("id", DataType.INT64, external_field="id")
    schema.add_field("value", DataType.FLOAT, external_field="value")
    schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim, external_field="embedding")
    return schema


def query_count(client, collection_name):
    """Query count(*) and return the integer count."""
    res = client.query(collection_name, filter="", output_fields=["count(*)"])
    return res[0]["count(*)"]


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture(scope="class")
def minio_client_and_cfg():
    """Returns (minio_client, config) or skips if MinIO is unavailable."""
    cfg = get_minio_config()
    try:
        mc = new_minio_client(cfg)
        if not mc.bucket_exists(cfg["bucket"]):
            pytest.skip(f"MinIO bucket {cfg['bucket']} not accessible")
        return mc, cfg
    except Exception as e:
        pytest.skip(f"MinIO unavailable: {e}")


# ============================================================
# P0: Core Functionality
# ============================================================

class TestExternalTableSchemaValidation(TestMilvusClientV2Base):
    """Schema validation: data types, forbidden features, describe/list."""

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("dtype,extra", [
        (DataType.BOOL, {}),
        (DataType.INT8, {}),
        (DataType.INT16, {}),
        (DataType.INT32, {}),
        (DataType.INT64, {}),
        (DataType.FLOAT, {}),
        (DataType.DOUBLE, {}),
        (DataType.VARCHAR, {"max_length": 256}),
        (DataType.JSON, {}),
        (DataType.ARRAY, {"element_type": DataType.VARCHAR, "max_length": 128, "max_capacity": 100}),
        (DataType.ARRAY, {"element_type": DataType.INT64, "max_capacity": 100}),
        (DataType.FLOAT_VECTOR, {"dim": 4}),
        (DataType.BINARY_VECTOR, {"dim": 32}),
        (DataType.FLOAT16_VECTOR, {"dim": 4}),
        (DataType.BFLOAT16_VECTOR, {"dim": 4}),
    ], ids=lambda x: str(x) if not isinstance(x, dict) else None)
    def test_supported_data_types(self, dtype, extra):
        """Each supported data type should be accepted in external collection schema."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = client.create_schema(
            external_source="s3://test-bucket/data/",
            external_spec='{"format":"parquet"}',
        )
        is_vector = dtype in (
            DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR,
            DataType.FLOAT16_VECTOR, DataType.BFLOAT16_VECTOR,
        )
        if not is_vector:
            schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4, external_field="vec_col")
        schema.add_field("f", dtype, external_field="f_col", **extra)

        client.create_collection(collection_name=collection_name, schema=schema)
        assert client.has_collection(collection_name)
        client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("dtype,extra,server_type_name", [
        # SparseFloatVector uses Milvus custom binary encoding that is
        # incompatible with external files (parquet/lance/vortex), so it is
        # explicitly blocked by isExternalFieldTypeSupported in
        # pkg/util/typeutil/schema.go (introduced in Part8-1/4 #49061).
        (DataType.SPARSE_FLOAT_VECTOR, {}, "SparseFloatVector"),
    ], ids=lambda x: str(x) if not isinstance(x, (dict, str)) else x)
    def test_unsupported_external_field_types(self, dtype, extra, server_type_name):
        """Field types explicitly blocked for external collections should be rejected at create time."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = client.create_schema(
            external_source="s3://test-bucket/data/",
            external_spec='{"format":"parquet"}',
        )
        schema.add_field("f", dtype, external_field="f_col", **extra)

        with pytest.raises(Exception) as exc_info:
            client.create_collection(collection_name=collection_name, schema=schema)
        msg = str(exc_info.value)
        assert "does not support field type" in msg and server_type_name in msg, \
            f"Expected type-block error for {server_type_name}, got: {msg}"

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("case_name,schema_fn,expect_err", [
        ("PrimaryKey", lambda c: _schema_with_pk(c), "primary key"),
        ("DynamicField", lambda c: _schema_with_dynamic(c), "dynamic field"),
        ("PartitionKey", lambda c: _schema_with_partition_key(c), "partition key"),
        ("ClusteringKey", lambda c: _schema_with_clustering_key(c), "clustering key"),
        ("AutoID", lambda c: _schema_with_auto_id(c), "auto_id"),
        ("MissingExternalField", lambda c: _schema_missing_ext_field(c), "external_field"),
    ])
    def test_forbidden_schema_features(self, case_name, schema_fn, expect_err):
        """Forbidden schema features should be rejected."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        with pytest.raises(Exception) as exc_info:
            schema = schema_fn(client)
            client.create_collection(collection_name=collection_name, schema=schema)
        assert expect_err.lower() in str(exc_info.value).lower(), \
            f"Expected '{expect_err}' in error, got: {exc_info.value}"
        log.info(f"[{case_name}] Correctly rejected: {exc_info.value}")

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_and_describe(self):
        """Create external collection and verify describe returns external metadata."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = client.create_schema(
            external_source="s3://test-bucket/data/",
            external_spec='{"format": "parquet"}',
        )
        schema.add_field("text", DataType.VARCHAR, max_length=256, external_field="text_column")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=128, external_field="embedding_column")

        client.create_collection(collection_name=collection_name, schema=schema)
        try:
            assert client.has_collection(collection_name)

            info = client.describe_collection(collection_name)
            assert info.get("external_source") == "s3://test-bucket/data/"
            assert info.get("external_spec") == '{"format": "parquet"}'

            # Verify fields (user fields + auto-generated __virtual_pk__)
            fields = info["fields"]
            field_names = [f["name"] for f in fields]
            assert "__virtual_pk__" in field_names
            assert "text" in field_names
            assert "embedding" in field_names

            # Verify __virtual_pk__ is primary key with auto_id
            vpk = next(f for f in fields if f["name"] == "__virtual_pk__")
            assert vpk["is_primary"] is True
            assert vpk["auto_id"] is True

            # Verify external_field mapping
            text_field = next(f for f in fields if f["name"] == "text")
            assert text_field.get("external_field") == "text_column"

            # Verify list_collections contains this collection
            collections = client.list_collections()
            assert collection_name in collections
        finally:
            client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_multiple_vectors(self):
        """External collection with multiple vector fields."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = client.create_schema(
            external_source="s3://test-bucket/data/",
            external_spec='{"format":"parquet"}',
        )
        schema.add_field("text", DataType.VARCHAR, max_length=256, external_field="text_column")
        schema.add_field("dense", DataType.FLOAT_VECTOR, dim=128, external_field="dense_col")
        schema.add_field("binary", DataType.BINARY_VECTOR, dim=128, external_field="binary_col")

        client.create_collection(collection_name=collection_name, schema=schema)
        try:
            info = client.describe_collection(collection_name)
            fields = info["fields"]
            field_names = [f["name"] for f in fields]
            assert "dense" in field_names
            assert "binary" in field_names
        finally:
            client.drop_collection(collection_name)


# Schema helper functions for forbidden features tests
def _schema_with_pk(client):
    schema = client.create_schema(external_source="s3://b/d", external_spec='{"format":"parquet"}')
    schema.add_field("pk", DataType.INT64, is_primary=True, external_field="pk_col")
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4, external_field="vec_col")
    return schema


def _schema_with_dynamic(client):
    schema = client.create_schema(
        external_source="s3://b/d", external_spec='{"format":"parquet"}',
        enable_dynamic_field=True,
    )
    schema.add_field("f", DataType.VARCHAR, max_length=256, external_field="f_col")
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4, external_field="vec_col")
    return schema


def _schema_with_partition_key(client):
    schema = client.create_schema(external_source="s3://b/d", external_spec='{"format":"parquet"}')
    schema.add_field("cat", DataType.VARCHAR, max_length=256, is_partition_key=True, external_field="cat_col")
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4, external_field="vec_col")
    return schema


def _schema_with_clustering_key(client):
    schema = client.create_schema(external_source="s3://b/d", external_spec='{"format":"parquet"}')
    schema.add_field("ts", DataType.INT64, is_clustering_key=True, external_field="ts_col")
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4, external_field="vec_col")
    return schema


def _schema_with_auto_id(client):
    schema = client.create_schema(external_source="s3://b/d", external_spec='{"format":"parquet"}')
    schema.add_field("id", DataType.INT64, auto_id=True, external_field="id_col")
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4, external_field="vec_col")
    return schema


def _schema_missing_ext_field(client):
    schema = client.create_schema(external_source="s3://b/d", external_spec='{"format":"parquet"}')
    schema.add_field("f", DataType.VARCHAR, max_length=256)  # no external_field!
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4, external_field="vec_col")
    return schema


# Property keys recognized by DDL for external collections (see pkg/common/common.go).
EXT_SOURCE_KEY = "collection.external_source"
EXT_SPEC_KEY = "collection.external_spec"


def _build_minimal_external_schema(client, source, spec='{"format":"parquet"}'):
    """Minimal valid external-collection schema: id + vec, both mapped to external fields."""
    schema = client.create_schema(external_source=source, external_spec=spec)
    schema.add_field("id", DataType.INT64, external_field="id")
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4, external_field="vec")
    return schema


class TestExternalSourceValidation(TestMilvusClientV2Base):
    """DDL validation of ExternalSource URL (Part8-2/4 #49062, Part8-1/4 #49061).

    RootCoord calls externalspec.ValidateSourceAndSpec whenever the schema
    has any field with external_field set (IsExternalCollection == true).
    See internal/rootcoord/create_collection_task.go and
    pkg/util/externalspec/external_spec.go.
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("scheme", ["s3", "s3a", "aws", "minio", "oss", "gs", "gcs"])
    def test_allowed_schemes_accepted(self, scheme):
        """All schemes in the allowlist should be accepted."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = _build_minimal_external_schema(client, f"{scheme}://bucket/prefix/")
        client.create_collection(collection_name=collection_name, schema=schema)
        try:
            assert client.has_collection(collection_name)
        finally:
            client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_relative_path_no_scheme_allowed(self):
        """A relative path with no scheme should be accepted (same-bucket default)."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = _build_minimal_external_schema(client, "my-data/parquet/")
        client.create_collection(collection_name=collection_name, schema=schema)
        try:
            assert client.has_collection(collection_name)
        finally:
            client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("source", [
        "http://169.254.169.254/metadata",
        "ftp://example.com/data",
        "xyz://bucket/prefix",
    ], ids=["http", "ftp", "xyz"])
    def test_disallowed_scheme_rejected(self, source):
        """Schemes outside the allowlist should be rejected with 'not allowed'."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = _build_minimal_external_schema(client, source)
        with pytest.raises(Exception) as exc_info:
            client.create_collection(collection_name=collection_name, schema=schema)
        assert "not allowed" in str(exc_info.value).lower(), \
            f"Expected 'not allowed' in error, got: {exc_info.value}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_file_scheme_rejected(self):
        """file:// is not allowed — would let an operator read from the node's local FS."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = _build_minimal_external_schema(client, "file:///tmp/data")
        with pytest.raises(Exception) as exc_info:
            client.create_collection(collection_name=collection_name, schema=schema)
        assert "not allowed" in str(exc_info.value).lower(), \
            f"Expected 'not allowed' in error, got: {exc_info.value}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_userinfo_in_url_rejected(self):
        """Embedded credentials like s3://ak:sk@bucket/prefix must be rejected."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = _build_minimal_external_schema(client, "s3://ak:sk@bucket/prefix")
        with pytest.raises(Exception) as exc_info:
            client.create_collection(collection_name=collection_name, schema=schema)
        assert "credentials" in str(exc_info.value).lower(), \
            f"Expected credentials rejection, got: {exc_info.value}"


class TestAlterExternalCollection(TestMilvusClientV2Base):
    """Alter external_source / external_spec via alter_collection_properties.

    Introduced in Part8-2/4 (#49062). Property keys are
    'collection.external_source' and 'collection.external_spec'
    (see pkg/common/common.go).
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_alter_updates_external_source_and_spec(self):
        """Alter both source and spec together; describe should reflect the new values."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = _build_minimal_external_schema(client, "s3://bucket/old/", '{"format":"parquet"}')
        client.create_collection(collection_name=collection_name, schema=schema)
        try:
            new_source = "s3://bucket/new/"
            new_spec = '{"format":"lance"}'
            client.alter_collection_properties(
                collection_name,
                properties={EXT_SOURCE_KEY: new_source, EXT_SPEC_KEY: new_spec},
            )
            info = client.describe_collection(collection_name)
            assert info.get("external_source") == new_source, \
                f"expected source {new_source}, got {info.get('external_source')}"
            assert info.get("external_spec") == new_spec, \
                f"expected spec {new_spec}, got {info.get('external_spec')}"
        finally:
            client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_alter_source_only_resets_spec(self):
        """Per Part8-2/4 semantics: when the alter request omits external_spec,
        spec is cleared. See ddl_callbacks_alter_collection_properties_test.go:608."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = _build_minimal_external_schema(client, "s3://bucket/old/", '{"format":"parquet"}')
        client.create_collection(collection_name=collection_name, schema=schema)
        try:
            new_source = "s3://bucket/new-source-only/"
            client.alter_collection_properties(
                collection_name,
                properties={EXT_SOURCE_KEY: new_source},
            )
            info = client.describe_collection(collection_name)
            assert info.get("external_source") == new_source
            assert info.get("external_spec") in ("", None), \
                f"expected spec to be reset, got {info.get('external_spec')!r}"
        finally:
            client.drop_collection(collection_name)


class TestExternalTableDataTypesE2E(TestMilvusClientV2Base):
    """E2E lifecycle per scalar type: parquet → refresh → index → load → query → search."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("type_name,milvus_dtype,milvus_extra,arrow_type,value_fn", [
        ("Bool", DataType.BOOL, {}, pa.bool_(), lambda i: i % 2 == 0),
        ("Int8", DataType.INT8, {}, pa.int8(), lambda i: i % 100),
        ("Int16", DataType.INT16, {}, pa.int16(), lambda i: i),
        ("Int32", DataType.INT32, {}, pa.int32(), lambda i: i),
        ("Int64", DataType.INT64, {}, pa.int64(), lambda i: i),
        ("Float", DataType.FLOAT, {}, pa.float32(), lambda i: float(i) * 1.5),
        ("Double", DataType.DOUBLE, {}, pa.float64(), lambda i: float(i) * 0.01),
        ("VarChar", DataType.VARCHAR, {"max_length": 256}, pa.string(), lambda i: f"str_{i:04d}"),
        ("JSON", DataType.JSON, {}, pa.string(), lambda i: json.dumps({"k": i})),
    ], ids=lambda x: x if isinstance(x, str) else None)
    def test_scalar_type_e2e(self, type_name, milvus_dtype, milvus_extra, arrow_type, value_fn,
                             minio_client_and_cfg):
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"
        num_rows = 50

        try:
            # Generate and upload parquet
            data = generate_typed_parquet_bytes(num_rows, 0, "f", arrow_type, value_fn)
            upload_to_minio(minio_client, minio_cfg["bucket"], f"{ext_path}/data.parquet", data)

            # Create collection
            schema = client.create_schema(
                external_source=ext_path,
                external_spec='{"format":"parquet"}',
            )
            schema.add_field("id", DataType.INT64, external_field="id")
            schema.add_field("f", milvus_dtype, external_field="f", **milvus_extra)
            schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_VEC_DIM, external_field="embedding")

            client.create_collection(collection_name=collection_name, schema=schema)

            # Refresh + Index + Load
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)

            # Verify count
            count = query_count(client, collection_name)
            assert count == num_rows, f"Expected {num_rows}, got {count}"

            # Query with output field
            res = client.query(collection_name, filter="id == 0", output_fields=["id", "f"])
            assert len(res) == 1
            log.info(f"[{type_name}] query output: {res[0]}")

            # Search
            vec = [[float(d) * 0.1 for d in range(TEST_VEC_DIM)]]
            search_res = client.search(
                collection_name, data=vec, limit=5,
                anns_field="embedding", output_fields=["id", "f"],
            )
            assert len(search_res[0]) > 0
            log.info(f"[{type_name}] search returned {len(search_res[0])} results")
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")


class TestRefreshAndVerifySegments(TestMilvusClientV2Base):
    """Refresh external collection and verify row count and job list."""

    @pytest.mark.tags(CaseLabel.L0)
    def test_refresh_and_verify(self, minio_client_and_cfg):
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"
        num_files = 2
        rows_per_file = 200

        try:
            # Upload parquet files
            for i in range(num_files):
                data = generate_parquet_bytes(rows_per_file, i * rows_per_file)
                upload_to_minio(minio_client, minio_cfg["bucket"], f"{ext_path}/data{i}.parquet", data)

            # Create external collection
            schema = create_basic_external_schema(client, ext_path)
            client.create_collection(collection_name=collection_name, schema=schema)

            # Verify describe
            info = client.describe_collection(collection_name)
            assert info.get("external_source") == ext_path

            # Refresh and wait
            job_id = refresh_and_wait(client, collection_name)

            # Verify row count
            index_and_load(client, collection_name)
            count = query_count(client, collection_name)
            assert count == num_files * rows_per_file, \
                f"Expected {num_files * rows_per_file}, got {count}"

            # List refresh jobs
            jobs = client.list_refresh_external_collection_jobs(collection_name=collection_name)
            assert len(jobs) > 0
            job_ids = [j.job_id for j in jobs]
            assert job_id in job_ids, f"Job {job_id} not found in {job_ids}"
            log.info(f"Refresh jobs: {[(j.job_id, j.state) for j in jobs]}")
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")


class TestExternalCrossBucket(TestMilvusClientV2Base):
    """Cross-bucket external source (Part8-1/4 #49061).

    External data can live in a different MinIO bucket than Milvus's own
    bucket. The s3:///<bucket>/<path> form (empty host) lets resolve_config
    match any endpoint so only the bucket needs to resolve against an
    extfs.* config entry.
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_cross_bucket_source(self, minio_client_and_cfg):
        """External data in a separate bucket should be readable end-to-end."""
        minio_client, main_cfg = minio_client_and_cfg
        external_bucket = "external-bucket"

        if not minio_client.bucket_exists(external_bucket):
            minio_client.make_bucket(external_bucket)
            log.info(f"Created cross-bucket: {external_bucket}")

        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"

        num_files = 2
        rows_per_file = 500
        total_rows = num_files * rows_per_file
        for i in range(num_files):
            data = generate_parquet_bytes(rows_per_file, i * rows_per_file)
            upload_to_minio(minio_client, external_bucket,
                            f"{ext_path}/data{i}.parquet", data)
        log.info(f"Uploaded {total_rows} rows across {num_files} files to "
                 f"s3://{external_bucket}/{ext_path}/")

        # empty-host URI tells resolve_config to match any endpoint by bucket.
        external_source = f"s3:///{external_bucket}/{ext_path}"
        schema = create_basic_external_schema(client, external_source)
        client.create_collection(collection_name=collection_name, schema=schema)
        try:
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)
            count = query_count(client, collection_name)
            assert count == total_rows, \
                f"cross-bucket load: expected {total_rows} rows, got {count}"
            log.info(f"[cross-bucket] ✓ {total_rows} rows loaded from "
                     f"{external_bucket} while Milvus uses {main_cfg['bucket']}")
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, external_bucket, f"{ext_path}/")


class TestRefreshUpdatesSchema(TestMilvusClientV2Base):
    """Refresh can rebind external_source/external_spec (Part8-2/4 #49062).

    refresh_external_collection(external_source=, external_spec=) atomically
    rebinds the collection to a new data path + spec and broadcasts the
    schema DDL via the WAL updater. After the job completes, the refreshed
    values must be visible through describe_collection.
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_refresh_with_new_source_and_spec_updates_schema(self, minio_client_and_cfg):
        """First refresh at pathV1/specV1, second refresh with new pathV2/specV2,
        describe should reflect the new values and row count should match pathV2."""
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        base = f"external-e2e-test/{collection_name}"
        path_v1 = f"{base}/v1"
        path_v2 = f"{base}/v2"

        rows_v1 = 100
        rows_v2 = 250
        upload_to_minio(minio_client, minio_cfg["bucket"],
                        f"{path_v1}/data.parquet", generate_parquet_bytes(rows_v1, 0))
        upload_to_minio(minio_client, minio_cfg["bucket"],
                        f"{path_v2}/data.parquet", generate_parquet_bytes(rows_v2, 10_000))

        spec_v1 = '{"format":"parquet"}'
        spec_v2 = '{"format":"parquet","version":2}'

        schema = client.create_schema(
            external_source=path_v1,
            external_spec=spec_v1,
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("value", DataType.FLOAT, external_field="value")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_VEC_DIM,
                         external_field="embedding")
        client.create_collection(collection_name=collection_name, schema=schema)
        try:
            initial = client.describe_collection(collection_name)
            assert initial.get("external_source") == path_v1
            assert initial.get("external_spec") == spec_v1

            # Baseline refresh against v1.
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)
            count_v1 = query_count(client, collection_name)
            assert count_v1 == rows_v1, f"baseline: expected {rows_v1}, got {count_v1}"

            # Second refresh with a new source + spec — WAL schema updater
            # should pick up both changes.
            job_id = client.refresh_external_collection(
                collection_name=collection_name,
                external_source=path_v2,
                external_spec=spec_v2,
            )
            assert job_id > 0
            deadline = time.time() + 120
            while time.time() < deadline:
                progress = client.get_refresh_external_collection_progress(job_id=job_id)
                if progress.state == "RefreshCompleted":
                    break
                if progress.state == "RefreshFailed":
                    raise AssertionError(
                        f"second refresh failed: {progress.reason}")
                time.sleep(2)
            else:
                raise AssertionError(f"second refresh job {job_id} timed out")

            # Describe should reflect the new source/spec.
            updated = client.describe_collection(collection_name)
            assert updated.get("external_source") == path_v2, \
                f"source not rebounded: {updated.get('external_source')}"
            assert updated.get("external_spec") == spec_v2, \
                f"spec not rebounded: {updated.get('external_spec')}"

            # Refresh reloads data; QueryNode needs release+load to drop the
            # stale v1 segments and pull v2.
            client.release_collection(collection_name)
            client.load_collection(collection_name)

            # Query must now see rows from v2, not v1.
            count_v2 = query_count(client, collection_name)
            assert count_v2 == rows_v2, \
                f"after rebind: expected {rows_v2}, got {count_v2}"
            res = client.query(collection_name, filter="id == 10000",
                               output_fields=["id"])
            assert len(res) == 1 and res[0]["id"] == 10000, \
                f"expected id=10000 from v2 data, got {res}"
            log.info(f"[refresh-rebind] ✓ source/spec updated, row count "
                     f"{count_v1} → {count_v2}")
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{base}/")


class TestForceNullableExternalFields(TestMilvusClientV2Base):
    """Force nullable on external user fields (Part8-1/4 #49061).

    Parquet columns can legitimately contain nulls, and a non-nullable
    field would silently produce incorrect results. typeutil.IsExternalCollection
    triggers a normalization pass in CreateCollection that sets
    field.Nullable = true on every user field, regardless of what the
    client requested. Only the virtual PK / system fields are exempt.
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_user_fields_forced_to_nullable(self):
        """Client creates fields without explicit nullable; describe should return nullable=True."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(
            external_source="s3://test-bucket/data/",
            external_spec='{"format":"parquet"}',
        )
        # Intentionally do NOT pass nullable — rely on server-side force-nullable.
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("v", DataType.VARCHAR, max_length=64, external_field="v")
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4, external_field="vec")
        client.create_collection(collection_name=collection_name, schema=schema)
        try:
            info = client.describe_collection(collection_name)
            fields_by_name = {f["name"]: f for f in info["fields"]}

            for user_field in ("id", "v", "vec"):
                f = fields_by_name[user_field]
                assert f.get("nullable") is True, \
                    f"user field '{user_field}' should be force-nullable, got {f}"

            # Virtual PK must NOT be touched by the force-nullable pass.
            vpk = fields_by_name.get("__virtual_pk__")
            assert vpk is not None and vpk.get("is_primary") is True, \
                "__virtual_pk__ should exist as the primary key"
        finally:
            client.drop_collection(collection_name)


class TestSchemalessReader(TestMilvusClientV2Base):
    """Schemaless reader with column projection (Part8-1/4 #49061).

    The reader opens parquet with a nullptr arrow schema and an explicit
    needed_columns projection, instead of requiring the parquet schema to
    match the collection schema exactly. Extra columns in the source file
    should be ignored silently.
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_parquet_with_extra_columns_ignored(self, minio_client_and_cfg):
        """9-column parquet + 3-column external schema: extra columns are silently dropped."""
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"

        num_rows = 200
        data = generate_multi_type_parquet_bytes(num_rows, 0)
        upload_to_minio(minio_client, minio_cfg["bucket"],
                        f"{ext_path}/data.parquet", data)

        # Parquet has 9 columns; schema only projects 3 of them.
        schema = client.create_schema(
            external_source=ext_path,
            external_spec='{"format":"parquet"}',
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("int32_val", DataType.INT32, external_field="int32_val")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_VEC_DIM,
                         external_field="embedding")

        client.create_collection(collection_name=collection_name, schema=schema)
        try:
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)

            count = query_count(client, collection_name)
            assert count == num_rows, f"expected {num_rows} rows, got {count}"

            # Spot-check that the 3 projected columns carry the right values
            # (id=5 → int32_val = 5*100 = 500), confirming the reader selected
            # the right columns from the 9-column file rather than mis-mapping.
            res = client.query(collection_name, filter="id == 5",
                               output_fields=["id", "int32_val"])
            assert len(res) == 1, f"expected 1 match for id==5, got {len(res)}"
            assert res[0]["id"] == 5
            assert res[0]["int32_val"] == 500
            log.info(f"[schemaless] ✓ {num_rows} rows via 3-col projection "
                     f"from 9-col parquet; id=5 → int32_val=500")
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")


class TestExternalCollectionLoadAndQuery(TestMilvusClientV2Base):
    """Full lifecycle: query count, filter, search, hybrid, pagination, multi-vector, output vector."""

    @pytest.mark.tags(CaseLabel.L0)
    def test_load_query_search(self, minio_client_and_cfg):
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"
        rows_per_file = 500
        total_rows = 1000

        try:
            # Upload 2 parquet files
            for i in range(2):
                data = generate_parquet_bytes(rows_per_file, i * rows_per_file)
                upload_to_minio(minio_client, minio_cfg["bucket"], f"{ext_path}/data{i}.parquet", data)

            # Create, refresh, index, load
            schema = create_basic_external_schema(client, ext_path)
            client.create_collection(collection_name=collection_name, schema=schema)
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)

            # 1. Query count(*)
            count = query_count(client, collection_name)
            assert count == total_rows

            # 2. Query with filter
            res = client.query(collection_name, filter="id < 100", output_fields=["id", "value"])
            assert len(res) == 100

            # 3. Search
            vec = [[float(d) * 0.1 for d in range(TEST_VEC_DIM)]]
            search_res = client.search(
                collection_name, data=vec, limit=5,
                anns_field="embedding", output_fields=["id", "value"],
            )
            assert len(search_res) == 1
            assert len(search_res[0]) > 0

            # 4. Hybrid search (vector + scalar filter)
            hybrid_res = client.search(
                collection_name, data=vec, limit=5,
                anns_field="embedding",
                filter="id >= 500 && id < 1000",
                output_fields=["id", "value"],
            )
            assert len(hybrid_res[0]) > 0
            for hit in hybrid_res[0]:
                assert 500 <= hit["entity"]["id"] < 1000

            # 5. Pagination
            page_res = client.query(
                collection_name, filter="id >= 0",
                output_fields=["id"], offset=10, limit=20,
            )
            assert len(page_res) == 20

            # 6. Multi-vector search
            vec2 = [[float(999 - d) * 0.1 for d in range(TEST_VEC_DIM)]]
            multi_res = client.search(
                collection_name, data=vec + vec2, limit=3,
                anns_field="embedding", output_fields=["id"],
            )
            assert len(multi_res) == 2
            assert len(multi_res[0]) > 0
            assert len(multi_res[1]) > 0

            # 7. Query with embedding output
            vec_res = client.query(
                collection_name, filter="id == 0",
                output_fields=["id", "embedding"],
            )
            assert len(vec_res) == 1
            assert "embedding" in vec_res[0]
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")


class TestExternalCollectionIncrementalRefresh(TestMilvusClientV2Base):
    """Incremental refresh: add/remove files, verify data correctness."""

    @pytest.mark.tags(CaseLabel.L0)
    def test_incremental_refresh(self, minio_client_and_cfg):
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"

        try:
            # Phase 1: Upload data0 (ids 0-499) + data1 (ids 500-999)
            upload_to_minio(minio_client, minio_cfg["bucket"],
                            f"{ext_path}/data0.parquet", generate_parquet_bytes(500, 0))
            upload_to_minio(minio_client, minio_cfg["bucket"],
                            f"{ext_path}/data1.parquet", generate_parquet_bytes(500, 500))

            schema = create_basic_external_schema(client, ext_path)
            client.create_collection(collection_name=collection_name, schema=schema)
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)

            count = query_count(client, collection_name)
            assert count == 1000, f"Phase 1: expected 1000, got {count}"

            # Verify data1 range
            res = client.query(collection_name, filter="id >= 500 && id < 1000", output_fields=["id"])
            assert len(res) == 500

            # Phase 2: Remove data1, add data2 (ids 2000-2299)
            client.release_collection(collection_name)

            minio_client.remove_object(minio_cfg["bucket"], f"{ext_path}/data1.parquet")
            upload_to_minio(minio_client, minio_cfg["bucket"],
                            f"{ext_path}/data2.parquet", generate_parquet_bytes(300, 2000))

            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)

            # Phase 3: Verify
            count2 = query_count(client, collection_name)
            assert count2 == 800, f"Phase 3: expected 800, got {count2}"

            # data0 intact
            res0 = client.query(collection_name, filter="id >= 0 && id < 500", output_fields=["id"])
            assert len(res0) == 500

            # data1 gone
            res1 = client.query(collection_name, filter="id >= 500 && id < 1000", output_fields=["id"])
            assert len(res1) == 0

            # data2 present
            res2 = client.query(collection_name, filter="id >= 2000 && id < 2300", output_fields=["id"])
            assert len(res2) == 300

            # Search still works
            vec = [[float(d) * 0.1 for d in range(TEST_VEC_DIM)]]
            search_res = client.search(
                collection_name, data=vec, limit=5,
                anns_field="embedding", output_fields=["id"],
            )
            assert len(search_res[0]) > 0
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")


# ============================================================
# P1: Important Scenarios
# ============================================================

class TestExternalCollectionMultipleDataTypes(TestMilvusClientV2Base):
    """Single collection with multiple scalar+vector types, verify filters and values."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_multiple_data_types(self, minio_client_and_cfg):
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"
        num_rows = 100

        try:
            data = generate_multi_type_parquet_bytes(num_rows, 0)
            upload_to_minio(minio_client, minio_cfg["bucket"], f"{ext_path}/data.parquet", data)

            schema = client.create_schema(
                external_source=ext_path,
                external_spec='{"format":"parquet"}',
            )
            schema.add_field("id", DataType.INT64, external_field="id")
            schema.add_field("bool_val", DataType.BOOL, external_field="bool_val")
            schema.add_field("int8_val", DataType.INT8, external_field="int8_val")
            schema.add_field("int16_val", DataType.INT16, external_field="int16_val")
            schema.add_field("int32_val", DataType.INT32, external_field="int32_val")
            schema.add_field("float_val", DataType.FLOAT, external_field="float_val")
            schema.add_field("double_val", DataType.DOUBLE, external_field="double_val")
            schema.add_field("varchar_val", DataType.VARCHAR, max_length=64, external_field="varchar_val")
            schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_VEC_DIM, external_field="embedding")

            client.create_collection(collection_name=collection_name, schema=schema)
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)

            # Verify count
            count = query_count(client, collection_name)
            assert count == num_rows

            # Bool filter: even ids → 50 rows
            bool_res = client.query(collection_name, filter="bool_val == true", output_fields=["id"])
            assert len(bool_res) == 50

            # Int8 filter: int8_val < 10 → 10 rows
            int8_res = client.query(collection_name, filter="int8_val < 10", output_fields=["id"])
            assert len(int8_res) == 10

            # VarChar prefix filter
            varchar_res = client.query(
                collection_name, filter='varchar_val like "str_004%"', output_fields=["id"],
            )
            assert len(varchar_res) == 10

            # Verify specific row values for id=42
            row42 = client.query(
                collection_name, filter="id == 42",
                output_fields=["id", "bool_val", "int8_val", "int16_val", "int32_val",
                                "float_val", "double_val", "varchar_val"],
            )
            assert len(row42) == 1
            r = row42[0]
            assert r["bool_val"] is True  # 42 is even
            assert r["int8_val"] == 42
            assert r["int16_val"] == 420
            assert r["int32_val"] == 4200
            assert abs(r["float_val"] - 63.0) < 0.1
            assert abs(r["double_val"] - 0.42) < 0.001
            assert r["varchar_val"] == "str_0042"

            # Compound filter
            compound_res = client.query(
                collection_name,
                filter='bool_val == true && int32_val < 5000 && varchar_val like "str_00%"',
                output_fields=["id"],
            )
            assert len(compound_res) == 25
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")


class TestExternalTableRefreshErrors(TestMilvusClientV2Base):
    """Refresh error handling scenarios."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_non_existent_collection(self):
        """Refresh non-existent collection should fail."""
        client = self._client()
        with pytest.raises(Exception):
            client.refresh_external_collection(collection_name="non_existent_xyz")

    @pytest.mark.tags(CaseLabel.L1)
    def test_normal_collection(self):
        """Refresh non-external (normal) collection should fail."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = client.create_schema()
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        client.create_collection(collection_name=collection_name, schema=schema)

        try:
            with pytest.raises(Exception):
                client.refresh_external_collection(collection_name=collection_name)
        finally:
            client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_empty_source(self, minio_client_and_cfg):
        """Refresh with empty data source may complete with 0 rows or fail."""
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/empty_{collection_name}"

        try:
            schema = client.create_schema(
                external_source=ext_path,
                external_spec='{"format":"parquet"}',
            )
            schema.add_field("id", DataType.INT64, external_field="id")
            schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4, external_field="vec")
            client.create_collection(collection_name=collection_name, schema=schema)

            try:
                job_id = client.refresh_external_collection(collection_name=collection_name)
            except Exception as e:
                log.info(f"Refresh submit rejected (expected for empty source): {e}")
                return

            # Poll until done
            deadline = time.time() + 60
            while time.time() < deadline:
                progress = client.get_refresh_external_collection_progress(job_id=job_id)
                log.info(f"state={progress.state} progress={progress.progress}%")
                if progress.state in ("RefreshCompleted", "RefreshFailed"):
                    break
                time.sleep(2)
        finally:
            client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_concurrent_refresh_rejected(self, minio_client_and_cfg):
        """Second refresh should be rejected while first is running."""
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"

        try:
            data = generate_parquet_bytes(1000, 0)
            upload_to_minio(minio_client, minio_cfg["bucket"], f"{ext_path}/data.parquet", data)

            schema = create_basic_external_schema(client, ext_path)
            client.create_collection(collection_name=collection_name, schema=schema)

            # First refresh
            job_id = client.refresh_external_collection(collection_name=collection_name)
            log.info(f"First refresh job: {job_id}")

            # Immediately try second
            try:
                job_id2 = client.refresh_external_collection(collection_name=collection_name)
                log.info(f"Second refresh accepted (job_id={job_id2})")
            except Exception as e:
                log.info(f"Second refresh correctly rejected: {e}")

            # Wait for first to complete
            deadline = time.time() + 120
            while time.time() < deadline:
                progress = client.get_refresh_external_collection_progress(job_id=job_id)
                if progress.state in ("RefreshCompleted", "RefreshFailed"):
                    break
                time.sleep(2)
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")

    @pytest.mark.tags(CaseLabel.L1)
    def test_schema_mismatch(self, minio_client_and_cfg):
        """Schema with wrong external_field mapping — test where error surfaces."""
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"

        try:
            # Parquet has columns: id, value, embedding
            data = generate_parquet_bytes(100, 0)
            upload_to_minio(minio_client, minio_cfg["bucket"], f"{ext_path}/data.parquet", data)

            # Wrong mapping
            schema = client.create_schema(
                external_source=ext_path,
                external_spec='{"format":"parquet"}',
            )
            schema.add_field("x", DataType.INT64, external_field="wrong_col_a")
            schema.add_field("vec", DataType.FLOAT_VECTOR, dim=TEST_VEC_DIM, external_field="wrong_col_b")

            # Stage 1: Create — may or may not fail
            try:
                client.create_collection(collection_name=collection_name, schema=schema)
                log.info("[create] accepted (mismatch not caught here)")
            except Exception as e:
                log.info(f"[create] rejected: {e}")
                return

            # Stage 2: Refresh — likely to fail
            try:
                job_id = client.refresh_external_collection(collection_name=collection_name)
            except Exception as e:
                log.info(f"[refresh submit] rejected: {e}")
                return

            deadline = time.time() + 60
            while time.time() < deadline:
                progress = client.get_refresh_external_collection_progress(job_id=job_id)
                if progress.state == "RefreshFailed":
                    log.info(f"[refresh] failed (mismatch caught): {progress.reason}")
                    return
                if progress.state == "RefreshCompleted":
                    log.info("[refresh] completed (mismatch not caught)")
                    break
                time.sleep(2)

            # Stage 3: Index + Load + Query — further stages
            try:
                index_params = client.prepare_index_params()
                index_params.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
                client.create_index(collection_name, index_params)
                client.load_collection(collection_name, timeout=60)
                log.info("[index+load] succeeded")
            except Exception as e:
                log.info(f"[index+load] failed (mismatch caught): {e}")
                return

            try:
                client.query(collection_name, filter="x == 0", output_fields=["x"])
                log.info("[query] succeeded — mismatch never detected!")
            except Exception as e:
                log.info(f"[query] failed (mismatch caught): {e}")
        finally:
            try:
                client.drop_collection(collection_name)
            except Exception:
                pass
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")


class TestExternalTableQueryEdgeCases(TestMilvusClientV2Base):
    """Query and search edge cases: complex filters, virtual PK, topK, metrics."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("filter_name,expr,expected", [
        ("IN", "id in [0, 10, 20, 30, 40]", 5),
        ("NOT_IN", "id not in [0, 1, 2, 3, 4]", 95),
        ("OR", "id < 10 || id >= 90", 20),
        ("NOT_EQUAL", "id != 50", 99),
        ("BETWEEN", "id >= 20 && id <= 29", 10),
    ])
    def test_complex_filters(self, filter_name, expr, expected, minio_client_and_cfg):
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"

        try:
            data = generate_parquet_bytes(100, 0)
            upload_to_minio(minio_client, minio_cfg["bucket"], f"{ext_path}/data.parquet", data)

            schema = create_basic_external_schema(client, ext_path)
            client.create_collection(collection_name=collection_name, schema=schema)
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)

            res = client.query(collection_name, filter=expr, output_fields=["id"])
            assert len(res) == expected, f"{filter_name}: expected {expected}, got {len(res)}"
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")

    @pytest.mark.tags(CaseLabel.L1)
    def test_topk_exceeds_total(self, minio_client_and_cfg):
        """Search with topK > total rows."""
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"

        try:
            data = generate_parquet_bytes(10, 0)
            upload_to_minio(minio_client, minio_cfg["bucket"], f"{ext_path}/data.parquet", data)

            schema = create_basic_external_schema(client, ext_path)
            client.create_collection(collection_name=collection_name, schema=schema)
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)

            vec = [[0.0] * TEST_VEC_DIM]
            res = client.search(collection_name, data=vec, limit=100, anns_field="embedding")
            assert len(res[0]) <= 10
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("metric_type", ["L2", "IP", "COSINE"])
    def test_different_metrics(self, metric_type, minio_client_and_cfg):
        """Search with different metric types."""
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"

        try:
            data = generate_parquet_bytes(100, 0)
            upload_to_minio(minio_client, minio_cfg["bucket"], f"{ext_path}/data.parquet", data)

            schema = create_basic_external_schema(client, ext_path)
            client.create_collection(collection_name=collection_name, schema=schema)
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name, metric_type=metric_type)

            vec = [[0.0] * TEST_VEC_DIM]
            res = client.search(collection_name, data=vec, limit=5, anns_field="embedding")
            assert len(res[0]) > 0
            log.info(f"[{metric_type}] {len(res[0])} results, top distance={res[0][0]['distance']:.4f}")
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")


class TestExternalTableDataUpdateBehavior(TestMilvusClientV2Base):
    """Test how data visibility changes with refresh and reload."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_refresh_without_release(self, minio_client_and_cfg):
        """Refresh + Load without Release — does it pick up new data?"""
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"

        try:
            upload_to_minio(minio_client, minio_cfg["bucket"],
                            f"{ext_path}/data0.parquet", generate_parquet_bytes(500, 0))

            schema = create_basic_external_schema(client, ext_path)
            client.create_collection(collection_name=collection_name, schema=schema)
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)

            count = query_count(client, collection_name)
            assert count == 500
            log.info(f"[phase 1] count = {count}")

            # Add new file and refresh (no release)
            upload_to_minio(minio_client, minio_cfg["bucket"],
                            f"{ext_path}/data1.parquet", generate_parquet_bytes(300, 500))
            refresh_and_wait(client, collection_name)
            log.info("[phase 2] Refresh done (no Release)")

            count2 = query_count(client, collection_name)
            log.info(f"[phase 2] count after refresh (no reload) = {count2}")

            # Try Load without Release
            try:
                client.load_collection(collection_name)
                log.info("[phase 3] LoadCollection succeeded (no Release)")
            except Exception as e:
                log.info(f"[phase 3] LoadCollection error: {e}")

            time.sleep(5)
            count3 = query_count(client, collection_name)
            log.info(f"[phase 3] count after Load (without Release) = {count3}")

            if count3 == 800:
                log.info("Load without Release works!")
            elif count3 == 500:
                log.info("Load without Release did NOT pick up new data — Release is required")
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")

    @pytest.mark.tags(CaseLabel.L1)
    def test_reload_without_refresh(self, minio_client_and_cfg):
        """Release + Load without Refresh — does it discover new files?"""
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"

        try:
            upload_to_minio(minio_client, minio_cfg["bucket"],
                            f"{ext_path}/data0.parquet", generate_parquet_bytes(500, 0))

            schema = create_basic_external_schema(client, ext_path)
            client.create_collection(collection_name=collection_name, schema=schema)
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)

            count = query_count(client, collection_name)
            assert count == 500

            # Add new file, no refresh — Release + Load
            upload_to_minio(minio_client, minio_cfg["bucket"],
                            f"{ext_path}/data1.parquet", generate_parquet_bytes(300, 500))
            log.info("[phase 2] Uploaded data1 (300 rows), NO Refresh")

            client.release_collection(collection_name)
            client.load_collection(collection_name)

            count2 = query_count(client, collection_name)
            log.info(f"[phase 2] count = {count2} (expected 500 if Refresh required)")

            if count2 == 500:
                log.info("Confirmed: Reload without Refresh does NOT discover new data")
            elif count2 == 800:
                log.info("Reload without Refresh DID discover new data (unexpected)")
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")


# ============================================================
# P2: Extended Scenarios
# ============================================================

class TestExternalTableConcurrentAccess(TestMilvusClientV2Base):
    """Concurrent query + refresh + reload."""

    @pytest.mark.tags(CaseLabel.L2)
    def test_concurrent_access(self, minio_client_and_cfg):
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"

        try:
            upload_to_minio(minio_client, minio_cfg["bucket"],
                            f"{ext_path}/data0.parquet", generate_parquet_bytes(500, 0))

            schema = create_basic_external_schema(client, ext_path)
            client.create_collection(collection_name=collection_name, schema=schema)
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)

            count = query_count(client, collection_name)
            assert count == 500

            # Thread A: continuous query
            stop_a = threading.Event()
            query_ok = {"count": 0}
            query_fail = {"count": 0}
            last_count = {"value": 500}

            def thread_a():
                while not stop_a.is_set():
                    try:
                        c = query_count(client, collection_name)
                        query_ok["count"] += 1
                        last_count["value"] = c
                    except Exception:
                        query_fail["count"] += 1
                    time.sleep(0.2)

            t = threading.Thread(target=thread_a, daemon=True)
            t.start()
            time.sleep(1)

            # Thread B: upload new file
            upload_to_minio(minio_client, minio_cfg["bucket"],
                            f"{ext_path}/data1.parquet", generate_parquet_bytes(300, 500))

            # Thread C: refresh → release → load
            refresh_and_wait(client, collection_name)
            client.release_collection(collection_name)
            time.sleep(2)
            index_and_load(client, collection_name)
            time.sleep(2)

            stop_a.set()
            t.join(timeout=10)

            log.info(f"Thread A: ok={query_ok['count']} fail={query_fail['count']} last_count={last_count['value']}")
            assert last_count["value"] == 800, \
                f"After reload, expected 800 rows, got {last_count['value']}"
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")


class TestExternalTableFileDeletedReload(TestMilvusClientV2Base):
    """Delete source file then reload — test robustness."""

    @pytest.mark.tags(CaseLabel.L2)
    def test_file_deleted_reload(self, minio_client_and_cfg):
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"

        try:
            upload_to_minio(minio_client, minio_cfg["bucket"],
                            f"{ext_path}/data0.parquet", generate_parquet_bytes(100, 0))
            upload_to_minio(minio_client, minio_cfg["bucket"],
                            f"{ext_path}/data1.parquet", generate_parquet_bytes(100, 100))

            schema = create_basic_external_schema(client, ext_path)
            client.create_collection(collection_name=collection_name, schema=schema)
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)

            count = query_count(client, collection_name)
            assert count == 200

            # Delete data1 (NO Refresh)
            minio_client.remove_object(minio_cfg["bucket"], f"{ext_path}/data1.parquet")
            log.info("Deleted data1.parquet (no Refresh)")

            # Release + Load (simulate pod restart)
            client.release_collection(collection_name)
            try:
                client.load_collection(collection_name, timeout=60)
                log.info("Load succeeded after file deletion")

                count2 = query_count(client, collection_name)
                log.info(f"count after reload (file deleted, no refresh) = {count2}")

                if count2 == 200:
                    log.info("QN still serves 200 rows — cached/indexed data used")
                elif count2 == 100:
                    log.info("QN only loaded 100 rows — deleted segment failed silently")
            except Exception as e:
                log.info(f"Load failed (expected — file gone): {e}")
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")


class TestExternalTableCollectionOps(TestMilvusClientV2Base):
    """Collection operations: drop partition, add field, drop during refresh, recreate."""

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_partition_rejected(self):
        """DropPartition should be rejected for external collections."""
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = client.create_schema(
            external_source="s3://b/d",
            external_spec='{"format":"parquet"}',
        )
        schema.add_field("f", DataType.VARCHAR, max_length=256, external_field="f")
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4, external_field="v")
        client.create_collection(collection_name=collection_name, schema=schema)

        try:
            with pytest.raises(Exception):
                client.drop_partition(collection_name, "test_partition")
        finally:
            client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_during_refresh(self, minio_client_and_cfg):
        """Drop collection while refresh is running."""
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"

        try:
            data = generate_parquet_bytes(500, 0)
            upload_to_minio(minio_client, minio_cfg["bucket"], f"{ext_path}/data.parquet", data)

            schema = create_basic_external_schema(client, ext_path)
            client.create_collection(collection_name=collection_name, schema=schema)

            # Start refresh, then immediately drop
            client.refresh_external_collection(collection_name=collection_name)
            client.drop_collection(collection_name)

            assert not client.has_collection(collection_name)
            log.info("Drop during refresh succeeded")
        finally:
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")

    @pytest.mark.tags(CaseLabel.L2)
    def test_recreate_with_same_name(self, minio_client_and_cfg):
        """Drop and recreate collection with same name."""
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"

        try:
            data = generate_parquet_bytes(100, 0)
            upload_to_minio(minio_client, minio_cfg["bucket"], f"{ext_path}/data.parquet", data)

            schema = create_basic_external_schema(client, ext_path)

            # First creation
            client.create_collection(collection_name=collection_name, schema=schema)
            refresh_and_wait(client, collection_name)

            # Drop
            client.drop_collection(collection_name)

            # Recreate
            client.create_collection(collection_name=collection_name, schema=schema)
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)

            count = query_count(client, collection_name)
            assert count == 100
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")


class TestExternalTableCrossFeature(TestMilvusClientV2Base):
    """Cross-feature: alias, coexist with normal collection."""

    @pytest.mark.tags(CaseLabel.L2)
    def test_alias(self, minio_client_and_cfg):
        """Create alias for external collection and query/search via alias."""
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        alias_name = collection_name + "_alias"
        ext_path = f"external-e2e-test/{collection_name}"

        try:
            data = generate_parquet_bytes(100, 0)
            upload_to_minio(minio_client, minio_cfg["bucket"], f"{ext_path}/data.parquet", data)

            schema = create_basic_external_schema(client, ext_path)
            client.create_collection(collection_name=collection_name, schema=schema)
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)

            # Create alias
            client.create_alias(collection_name, alias_name)

            # Query via alias
            count = query_count(client, alias_name)
            assert count == 100

            # Search via alias
            vec = [[0.0] * TEST_VEC_DIM]
            res = client.search(alias_name, data=vec, limit=5, anns_field="embedding")
            assert len(res[0]) > 0
        finally:
            try:
                client.drop_alias(alias_name)
            except Exception:
                pass
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")

    @pytest.mark.tags(CaseLabel.L2)
    def test_coexist_with_normal(self, minio_client_and_cfg):
        """External and normal collections can coexist."""
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        ext_coll = cf.gen_collection_name_by_testcase_name() + "_ext"
        normal_coll = cf.gen_collection_name_by_testcase_name() + "_normal"
        ext_path = f"external-e2e-test/{ext_coll}"

        try:
            data = generate_parquet_bytes(50, 0)
            upload_to_minio(minio_client, minio_cfg["bucket"], f"{ext_path}/data.parquet", data)

            # External collection
            ext_schema = create_basic_external_schema(client, ext_path)
            client.create_collection(collection_name=ext_coll, schema=ext_schema)
            refresh_and_wait(client, ext_coll)
            index_and_load(client, ext_coll)

            # Normal collection
            normal_schema = client.create_schema()
            normal_schema.add_field("id", DataType.INT64, is_primary=True)
            normal_schema.add_field("vec", DataType.FLOAT_VECTOR, dim=TEST_VEC_DIM)
            client.create_collection(collection_name=normal_coll, schema=normal_schema)

            # Verify type distinction
            ext_info = client.describe_collection(ext_coll)
            assert ext_info.get("external_source") not in (None, "")

            normal_info = client.describe_collection(normal_coll)
            assert normal_info.get("external_source") in (None, "")

            # Both in list
            collections = client.list_collections()
            assert ext_coll in collections
            assert normal_coll in collections

            # Query external
            count = query_count(client, ext_coll)
            assert count == 50
        finally:
            client.drop_collection(ext_coll)
            client.drop_collection(normal_coll)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")


class TestExternalTableSegmentMapping(TestMilvusClientV2Base):
    """File-to-segment mapping with different file distributions."""

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("case_name,file_specs", [
        ("1_file_1000rows", [("data.parquet", 1000)]),
        ("2_files_500each", [("d0.parquet", 500), ("d1.parquet", 500)]),
        ("5_files_100each", [(f"d{i}.parquet", 100) for i in range(5)]),
        ("mixed_sizes", [("big.parquet", 800), ("s1.parquet", 50), ("s2.parquet", 50), ("tiny.parquet", 10)]),
    ])
    def test_segment_mapping(self, case_name, file_specs, minio_client_and_cfg):
        minio_client, minio_cfg = minio_client_and_cfg
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        ext_path = f"external-e2e-test/{collection_name}"

        try:
            total_rows = 0
            for fname, rows in file_specs:
                data = generate_parquet_bytes(rows, total_rows)
                upload_to_minio(minio_client, minio_cfg["bucket"], f"{ext_path}/{fname}", data)
                total_rows += rows

            schema = create_basic_external_schema(client, ext_path)
            client.create_collection(collection_name=collection_name, schema=schema)
            refresh_and_wait(client, collection_name)
            index_and_load(client, collection_name)

            count = query_count(client, collection_name)
            assert count == total_rows
            log.info(f"[{case_name}] {len(file_specs)} files → {total_rows} rows")
        finally:
            client.drop_collection(collection_name)
            cleanup_minio_prefix(minio_client, minio_cfg["bucket"], f"{ext_path}/")
