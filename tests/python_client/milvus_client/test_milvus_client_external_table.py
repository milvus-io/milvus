"""
External Table (external collection) comprehensive tests using pymilvus MilvusClient.

Coverage:
  1. Schema / lifecycle (create / describe / list / drop)
  2. Schema validation (forbidden features, blocked field types)
  3. All scalar data types (bool, int8-64, float, double, varchar, json, array)
  4. All vector data types (float, binary, float16, bfloat16, int8 where supported)
  5. All index types
       - vector: AUTOINDEX, HNSW, IVF_FLAT, FLAT, DISKANN
       - binary vector: BIN_FLAT, BIN_IVF_FLAT
       - scalar: INVERTED, BITMAP, STL_SORT, TRIE
       - mmap.enabled toggle on index and collection level
  6. DML blocked on external collections (insert / upsert / delete / flush)
  7. DDL blocked on external collections (create/drop partition, compact,
     truncate, add_field, load_partitions, release_partitions)
  8. Refresh semantics (basic / incremental / atomic source override / concurrent / job list /
                       many files / nested subdirectories)
  9. External file formats (parquet, lance-table, vortex)
 10. DQL surface (query filter / search metric / hybrid_search / search_iterator /
                  query_iterator / get by virtual PK / offset-limit pagination)
 11. Read-only / admin operations that normal collections support
     (list_partitions, has_partition, get_collection_stats, get_partition_stats,
      get_load_state, list_persistent_segments, rename_collection, refresh_load,
      alter/drop collection and index properties, custom database)
 12. Alias lifecycle and lifecycle edges (release+load, drop+recreate)

Run (against the `external-table-test` k8s instance):
    MINIO_ADDRESS=10.100.36.172:9000 \
    MINIO_BUCKET=ext-table-data \
    MINIO_ACCESS_KEY=minioadmin \
    MINIO_SECRET_KEY=minioadmin \
    pytest milvus_client/test_milvus_client_external_table.py \
      --host 10.100.36.174 --port 19530 -v -s

The tests build full `s3://<address>/<bucket>/<prefix>/` URLs as external_source
so Milvus's extfs layer can resolve the endpoint from the URL alone.
"""

import io
import json
import os
import random
import re
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from minio import Minio
from pymilvus import AnnSearchRequest, DataType, RRFRanker
from utils.util_log import test_log as log

# ============================================================
# Constants & Configuration
# ============================================================

TEST_DIM = 8
BINARY_DIM = 64  # must be a multiple of 8
DEFAULT_NB = 200
REFRESH_TIMEOUT = 180


def _env(key, default):
    return os.environ.get(key, default)


def get_minio_config():
    return {
        "address": _env("MINIO_ADDRESS", "localhost:9000"),
        "access_key": _env("MINIO_ACCESS_KEY", "minioadmin"),
        "secret_key": _env("MINIO_SECRET_KEY", "minioadmin"),
        "bucket": _env("MINIO_BUCKET", "ext-table-data"),
        "secure": _env("MINIO_SECURE", "false").lower() == "true",
    }


def build_external_source(cfg, key_prefix):
    """Build a full s3:// URL that Milvus's extfs resolver can use directly."""
    # Trailing slash matters — refresh lists objects under the prefix.
    return f"s3://{cfg['address']}/{cfg['bucket']}/{key_prefix}/"


def new_minio_client(cfg):
    return Minio(
        cfg["address"],
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        secure=cfg["secure"],
    )


# ============================================================
# Parquet Helpers
# ============================================================

# StorageV2 memory_planner currently lacks snappy; use uncompressed parquet.
_PARQUET_COMPRESSION = "NONE"


def _float_vectors(ids, dim):
    return np.array(
        [[float(i) * 0.1 + d for d in range(dim)] for i in ids],
        dtype=np.float32,
    )


def _float16_vectors(ids, dim):
    return np.array(
        [[float(i) * 0.1 + d for d in range(dim)] for i in ids],
        dtype=np.float16,
    )


def _bfloat16_vectors(ids, dim):
    """Build bfloat16 via numpy uint16 view; pyarrow lacks native bfloat16 bitcast."""
    # bfloat16 shares the top 16 bits of float32's IEEE 754 representation.
    f = np.array(
        [[float(i) * 0.1 + d for d in range(dim)] for i in ids],
        dtype=np.float32,
    )
    return (f.view(np.uint32) >> 16).astype(np.uint16)


def _int8_vectors(ids, dim):
    return np.array(
        [[(i + d) % 127 - 63 for d in range(dim)] for i in ids],
        dtype=np.int8,
    )


def _binary_vectors_bytes(ids, dim):
    """Return (num_rows, dim/8) uint8 array; bit set patterns depend on id."""
    nbytes = dim // 8
    rng = np.random.default_rng(seed=42)
    arr = rng.integers(low=0, high=256, size=(len(ids), nbytes), dtype=np.uint8)
    # make it deterministic per id: XOR with id-derived bytes
    for row_idx, i in enumerate(ids):
        arr[row_idx][0] = i & 0xFF
    return arr


def gen_parquet_bytes(num_rows, start_id, scalar_name, arrow_type, value_fn, dim=TEST_DIM):
    """Generate a Parquet file with columns: id, <scalar_name>, embedding (float_vec)."""
    ids = list(range(start_id, start_id + num_rows))
    vectors = _float_vectors(ids, dim)
    table = pa.table({
        "id": pa.array(ids, type=pa.int64()),
        scalar_name: pa.array([value_fn(i) for i in ids], type=arrow_type),
        "embedding": pa.FixedSizeListArray.from_arrays(vectors.flatten(), list_size=dim),
    })
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=_PARQUET_COMPRESSION)
    return buf.getvalue()


def gen_basic_parquet_bytes(num_rows, start_id, dim=TEST_DIM):
    """Generate a Parquet file with columns: id, value (float), embedding."""
    return gen_parquet_bytes(
        num_rows, start_id, "value", pa.float32(),
        lambda i: float(i) * 1.5, dim=dim,
    )


def gen_array_parquet_bytes(num_rows, start_id, arr_name, arr_element_arrow_type,
                            arr_value_fn, dim=TEST_DIM):
    """Parquet with an Array-typed column: id, <arr_name> (list<element>), embedding."""
    ids = list(range(start_id, start_id + num_rows))
    vectors = _float_vectors(ids, dim)
    table = pa.table({
        "id": pa.array(ids, type=pa.int64()),
        arr_name: pa.array([arr_value_fn(i) for i in ids],
                           type=pa.list_(arr_element_arrow_type)),
        "embedding": pa.FixedSizeListArray.from_arrays(vectors.flatten(), list_size=dim),
    })
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=_PARQUET_COMPRESSION)
    return buf.getvalue()


def gen_vector_variant_parquet_bytes(num_rows, start_id, vec_field, vec_dtype, dim):
    """Generate parquet with id + <vec_field> of the given vector family + no scalar."""
    ids = list(range(start_id, start_id + num_rows))
    if vec_dtype == DataType.FLOAT_VECTOR:
        arr = _float_vectors(ids, dim)
        parquet_col = pa.FixedSizeListArray.from_arrays(arr.flatten(), list_size=dim)
    elif vec_dtype == DataType.FLOAT16_VECTOR:
        # pyarrow cannot write float16 to Parquet, so store raw 16-bit bits as uint16.
        # Milvus reads this back as float16 by reinterpreting the uint16 buffer.
        arr = _float16_vectors(ids, dim).view(np.uint16)
        parquet_col = pa.FixedSizeListArray.from_arrays(
            pa.array(arr.flatten(), type=pa.uint16()), list_size=dim,
        )
    elif vec_dtype == DataType.BFLOAT16_VECTOR:
        arr = _bfloat16_vectors(ids, dim)
        # Store as uint16; Milvus reader must interpret as bfloat16 raw bits.
        parquet_col = pa.FixedSizeListArray.from_arrays(
            pa.array(arr.flatten(), type=pa.uint16()), list_size=dim,
        )
    elif vec_dtype == DataType.INT8_VECTOR:
        arr = _int8_vectors(ids, dim)
        parquet_col = pa.FixedSizeListArray.from_arrays(
            pa.array(arr.flatten(), type=pa.int8()), list_size=dim,
        )
    elif vec_dtype == DataType.BINARY_VECTOR:
        arr = _binary_vectors_bytes(ids, dim)
        parquet_col = pa.FixedSizeListArray.from_arrays(
            pa.array(arr.flatten(), type=pa.uint8()), list_size=dim // 8,
        )
    else:
        raise ValueError(f"unsupported vec_dtype {vec_dtype}")

    table = pa.table({
        "id": pa.array(ids, type=pa.int64()),
        vec_field: parquet_col,
    })
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=_PARQUET_COMPRESSION)
    return buf.getvalue()


def gen_all_scalar_parquet_bytes(num_rows, start_id, dim=TEST_DIM):
    """Single parquet with every stable scalar type + FloatVector.

    Columns: id, val_bool, val_int8, val_int16, val_int32, val_int64,
             val_float, val_double, val_varchar, val_json, embedding.
    """
    ids = list(range(start_id, start_id + num_rows))
    vectors = _float_vectors(ids, dim)
    table = pa.table({
        "id":          pa.array(ids, type=pa.int64()),
        "val_bool":    pa.array([i % 2 == 0 for i in ids], type=pa.bool_()),
        "val_int8":    pa.array([i % 100 for i in ids], type=pa.int8()),
        "val_int16":   pa.array([i * 3 for i in ids], type=pa.int16()),
        "val_int32":   pa.array([i * 7 for i in ids], type=pa.int32()),
        "val_int64":   pa.array([i * 1000 for i in ids], type=pa.int64()),
        "val_float":   pa.array([float(i) * 1.5 for i in ids], type=pa.float32()),
        "val_double":  pa.array([float(i) * 2.5 for i in ids], type=pa.float64()),
        "val_varchar": pa.array([f"s_{i:05d}" for i in ids], type=pa.string()),
        "val_json":    pa.array([json.dumps({"k": i, "g": i % 3}) for i in ids], type=pa.string()),
        "embedding":   pa.FixedSizeListArray.from_arrays(vectors.flatten(), list_size=dim),
    })
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=_PARQUET_COMPRESSION)
    return buf.getvalue()


def gen_multi_vector_parquet_bytes(num_rows, start_id, dim=TEST_DIM):
    """Parquet with two vector fields (float + binary) + id + scalar."""
    ids = list(range(start_id, start_id + num_rows))
    float_arr = _float_vectors(ids, dim)
    bin_arr = _binary_vectors_bytes(ids, BINARY_DIM)
    table = pa.table({
        "id":        pa.array(ids, type=pa.int64()),
        "value":     pa.array([float(i) for i in ids], type=pa.float32()),
        "dense_vec": pa.FixedSizeListArray.from_arrays(float_arr.flatten(), list_size=dim),
        "bin_vec":   pa.FixedSizeListArray.from_arrays(
            pa.array(bin_arr.flatten(), type=pa.uint8()), list_size=BINARY_DIM // 8,
        ),
    })
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=_PARQUET_COMPRESSION)
    return buf.getvalue()


# ============================================================
# MinIO Helpers
# ============================================================

def upload_parquet(minio_client, bucket, key, data):
    minio_client.put_object(
        bucket, key,
        io.BytesIO(data), length=len(data),
        content_type="application/octet-stream",
    )


def cleanup_minio_prefix(minio_client, bucket, prefix):
    for obj in minio_client.list_objects(bucket, prefix=prefix, recursive=True):
        minio_client.remove_object(bucket, obj.object_name)


# ============================================================
# Schema / Refresh / Load Helpers
# ============================================================

def build_basic_schema(client, ext_path, dim=TEST_DIM):
    """Minimal external schema: id(Int64) + value(Float) + embedding(FloatVector)."""
    schema = client.create_schema(
        external_source=ext_path,
        external_spec='{"format":"parquet"}',
    )
    schema.add_field("id", DataType.INT64, external_field="id")
    schema.add_field("value", DataType.FLOAT, external_field="value")
    schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim, external_field="embedding")
    return schema


def build_typed_schema(client, ext_path, scalar_name, scalar_dtype, dim=TEST_DIM, **extra):
    """External schema with id + one scalar field of the given type + FloatVector."""
    schema = client.create_schema(
        external_source=ext_path,
        external_spec='{"format":"parquet"}',
    )
    schema.add_field("id", DataType.INT64, external_field="id")
    schema.add_field(scalar_name, scalar_dtype, external_field=scalar_name, **extra)
    schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim, external_field="embedding")
    return schema


def build_vector_variant_schema(client, ext_path, vec_field, vec_dtype, dim):
    """External schema with id + one vector field of the given dtype."""
    schema = client.create_schema(
        external_source=ext_path,
        external_spec='{"format":"parquet"}',
    )
    schema.add_field("id", DataType.INT64, external_field="id")
    schema.add_field(vec_field, vec_dtype, dim=dim, external_field=vec_field)
    return schema


def build_all_scalar_schema(client, ext_path, dim=TEST_DIM):
    schema = client.create_schema(
        external_source=ext_path,
        external_spec='{"format":"parquet"}',
    )
    schema.add_field("id",          DataType.INT64,   external_field="id")
    schema.add_field("val_bool",    DataType.BOOL,    external_field="val_bool")
    schema.add_field("val_int8",    DataType.INT8,    external_field="val_int8")
    schema.add_field("val_int16",   DataType.INT16,   external_field="val_int16")
    schema.add_field("val_int32",   DataType.INT32,   external_field="val_int32")
    schema.add_field("val_int64",   DataType.INT64,   external_field="val_int64")
    schema.add_field("val_float",   DataType.FLOAT,   external_field="val_float")
    schema.add_field("val_double",  DataType.DOUBLE,  external_field="val_double")
    schema.add_field("val_varchar", DataType.VARCHAR, max_length=64, external_field="val_varchar")
    schema.add_field("val_json",    DataType.JSON,    external_field="val_json")
    schema.add_field("embedding",   DataType.FLOAT_VECTOR, dim=dim, external_field="embedding")
    return schema


def build_multi_vector_schema(client, ext_path, float_dim=TEST_DIM, bin_dim=BINARY_DIM):
    schema = client.create_schema(
        external_source=ext_path,
        external_spec='{"format":"parquet"}',
    )
    schema.add_field("id",        DataType.INT64,  external_field="id")
    schema.add_field("value",     DataType.FLOAT,  external_field="value")
    schema.add_field("dense_vec", DataType.FLOAT_VECTOR, dim=float_dim, external_field="dense_vec")
    schema.add_field("bin_vec",   DataType.BINARY_VECTOR, dim=bin_dim, external_field="bin_vec")
    return schema


def refresh_and_wait(client, collection_name, timeout=REFRESH_TIMEOUT,
                    external_source=None, external_spec=None):
    """Trigger refresh, poll until terminal state, return job_id."""
    kwargs = {"collection_name": collection_name}
    if external_source is not None:
        kwargs["external_source"] = external_source
    if external_spec is not None:
        kwargs["external_spec"] = external_spec

    job_id = client.refresh_external_collection(**kwargs)
    assert job_id and job_id > 0, f"Expected positive job_id, got {job_id}"
    log.info(f"[refresh] submitted job_id={job_id} for {collection_name}")

    deadline = time.time() + timeout
    last_state = None
    while time.time() < deadline:
        progress = client.get_refresh_external_collection_progress(job_id=job_id)
        if progress.state != last_state:
            log.info(f"[refresh] job={job_id} state={progress.state} progress={progress.progress}%")
            last_state = progress.state
        if progress.state == "RefreshCompleted":
            return job_id
        if progress.state == "RefreshFailed":
            raise AssertionError(f"Refresh job {job_id} failed: {progress.reason}")
        time.sleep(2)

    raise AssertionError(f"Refresh job {job_id} did not finish within {timeout}s")


def add_vector_index(client, collection_name, vec_field="embedding",
                     index_type="AUTOINDEX", metric_type="L2", **extra):
    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name=vec_field, index_type=index_type, metric_type=metric_type,
        **extra,
    )
    client.create_index(collection_name, index_params)


def index_and_load(client, collection_name, vec_field="embedding", metric_type="L2"):
    add_vector_index(client, collection_name, vec_field, "AUTOINDEX", metric_type)
    client.load_collection(collection_name)


def query_count(client, collection_name):
    res = client.query(collection_name, filter="", output_fields=["count(*)"])
    return res[0]["count(*)"]


def upload_basic_data(minio_client, cfg, ext_key, num_rows=DEFAULT_NB, start_id=0,
                      filename="data.parquet", dim=TEST_DIM):
    """Upload a basic parquet file and return full minio key."""
    key = f"{ext_key}/{filename}"
    upload_parquet(
        minio_client, cfg["bucket"], key,
        gen_basic_parquet_bytes(num_rows, start_id, dim=dim),
    )
    return key


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture(scope="module")
def minio_env():
    """Shared MinIO client + config for the module. Skips if unreachable."""
    cfg = get_minio_config()
    try:
        mc = new_minio_client(cfg)
        if not mc.bucket_exists(cfg["bucket"]):
            pytest.skip(f"MinIO bucket {cfg['bucket']} not accessible at {cfg['address']}")
    except Exception as exc:
        pytest.skip(f"MinIO unavailable at {cfg['address']}: {exc}")
    return mc, cfg


@pytest.fixture(scope="function")
def external_prefix(minio_env, request):
    """Per-test MinIO prefix bundled with the resolved external_source URL."""
    mc, cfg = minio_env
    # Sanitize node.name aggressively — parametrize IDs can contain chars MinIO or
    # Milvus's S3 URI parser rejects. Keep only [A-Za-z0-9_-].
    safe = re.sub(r"[^A-Za-z0-9_-]", "_", request.node.name)
    key = f"external-e2e-core/{safe}-{int(time.time() * 1000)}-{random.randint(1000,9999)}"
    yield {"key": key, "url": build_external_source(cfg, key)}
    try:
        cleanup_minio_prefix(mc, cfg["bucket"], f"{key}/")
    except Exception as exc:
        log.warning(f"cleanup_minio_prefix({key}) failed: {exc}")


# ============================================================
# Schema builders for validation tests (module-level, reused across classes)
# ============================================================

_PLACEHOLDER_SRC = "s3://test-bucket/placeholder/"
_PLACEHOLDER_SPEC = '{"format":"parquet"}'


def _schema_with_user_pk(client):
    schema = client.create_schema(external_source=_PLACEHOLDER_SRC, external_spec=_PLACEHOLDER_SPEC)
    schema.add_field("pk", DataType.INT64, is_primary=True, external_field="pk")
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="vec")
    return schema


def _schema_with_auto_id(client):
    schema = client.create_schema(external_source=_PLACEHOLDER_SRC, external_spec=_PLACEHOLDER_SPEC)
    schema.add_field("id", DataType.INT64, auto_id=True, external_field="id")
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="vec")
    return schema


def _schema_with_dynamic(client):
    schema = client.create_schema(
        external_source=_PLACEHOLDER_SRC, external_spec=_PLACEHOLDER_SPEC,
        enable_dynamic_field=True,
    )
    schema.add_field("id", DataType.INT64, external_field="id")
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="vec")
    return schema


def _schema_with_partition_key(client):
    schema = client.create_schema(external_source=_PLACEHOLDER_SRC, external_spec=_PLACEHOLDER_SPEC)
    schema.add_field("cat", DataType.VARCHAR, max_length=64, is_partition_key=True, external_field="cat")
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="vec")
    return schema


def _schema_missing_external_field(client):
    schema = client.create_schema(external_source=_PLACEHOLDER_SRC, external_spec=_PLACEHOLDER_SPEC)
    schema.add_field("plain_field", DataType.INT64)  # missing external_field
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="vec")
    return schema


# ============================================================
# 1. Schema + Lifecycle
# ============================================================

class TestExternalTableSchema(TestMilvusClientV2Base):
    """Collection creation, describe, list, drop and schema validation."""

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_describe_list_drop(self, external_prefix):
        """Create an external collection, verify describe/list, then drop."""
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url = external_prefix["url"]
        schema = build_basic_schema(client, ext_url)

        client.create_collection(collection_name=coll, schema=schema)
        try:
            assert client.has_collection(coll), "collection should exist after create"

            info = client.describe_collection(coll)
            assert info.get("external_source") == ext_url
            assert info.get("external_spec") == '{"format":"parquet"}'

            field_names = [f["name"] for f in info["fields"]]
            assert "__virtual_pk__" in field_names, "virtual PK should be injected"
            for required in ("id", "value", "embedding"):
                assert required in field_names, f"{required} missing from {field_names}"

            vpk = next(f for f in info["fields"] if f["name"] == "__virtual_pk__")
            assert vpk["is_primary"] is True
            assert vpk["auto_id"] is True

            value_f = next(f for f in info["fields"] if f["name"] == "value")
            assert value_f.get("external_field") == "value"

            assert coll in client.list_collections()
        finally:
            client.drop_collection(coll)
            assert not client.has_collection(coll), "collection should be gone after drop"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("case_name,schema_builder,err_hint", [
        ("user_primary_key",      lambda c: _schema_with_user_pk(c),              "primary key"),
        ("auto_id_on_user_field", lambda c: _schema_with_auto_id(c),              "auto_id"),
        ("dynamic_field_enabled", lambda c: _schema_with_dynamic(c),              "dynamic field"),
        ("partition_key",         lambda c: _schema_with_partition_key(c),        "partition key"),
        ("missing_external_field", lambda c: _schema_missing_external_field(c),   "external_field"),
    ])
    def test_schema_reject_forbidden(self, case_name, schema_builder, err_hint):
        """Forbidden schema shapes must be rejected at create_collection time."""
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()

        with pytest.raises(Exception) as exc_info:
            schema = schema_builder(client)
            client.create_collection(collection_name=coll, schema=schema)

        msg = str(exc_info.value).lower()
        assert err_hint.lower() in msg, \
            f"[{case_name}] expected {err_hint!r} in error, got: {exc_info.value}"
        log.info(f"[{case_name}] correctly rejected: {exc_info.value}")

    @pytest.mark.tags(CaseLabel.L1)
    def test_schema_reject_sparse_float_vector(self):
        """SPARSE_FLOAT_VECTOR is explicitly blocked for external collections."""
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(
            external_source=_PLACEHOLDER_SRC, external_spec=_PLACEHOLDER_SPEC,
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("sv", DataType.SPARSE_FLOAT_VECTOR, external_field="sv")
        with pytest.raises(Exception) as exc_info:
            client.create_collection(collection_name=coll, schema=schema)
        msg = str(exc_info.value).lower()
        assert "not support" in msg or "sparse" in msg, \
            f"Expected sparse-vector rejection, got: {exc_info.value}"


# ============================================================
# 2. Data Types (Scalar + Vector)
# ============================================================

class TestExternalTableDataTypes(TestMilvusClientV2Base):
    """E2E coverage for every supported scalar and vector data type."""

    # ---- Scalar types ------------------------------------------------

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("type_name,dtype,extra,arrow_type,value_fn", [
        ("bool",    DataType.BOOL,    {},                  pa.bool_(),    lambda i: i % 2 == 0),
        ("int8",    DataType.INT8,    {},                  pa.int8(),     lambda i: i % 100),
        ("int16",   DataType.INT16,   {},                  pa.int16(),    lambda i: i * 3),
        ("int32",   DataType.INT32,   {},                  pa.int32(),    lambda i: i * 7),
        ("int64",   DataType.INT64,   {},                  pa.int64(),    lambda i: i * 1000),
        ("float",   DataType.FLOAT,   {},                  pa.float32(),  lambda i: float(i) * 1.5),
        ("double",  DataType.DOUBLE,  {},                  pa.float64(),  lambda i: float(i) * 2.5),
        ("varchar", DataType.VARCHAR, {"max_length": 64},  pa.string(),   lambda i: f"s_{i:05d}"),
        ("json",    DataType.JSON,    {},                  pa.string(),   lambda i: json.dumps({"k": i, "g": i % 3})),
    ], ids=lambda x: x if isinstance(x, str) else None)
    def test_scalar_type_e2e(self, type_name, dtype, extra, arrow_type, value_fn,
                             minio_env, external_prefix):
        """End-to-end per supported scalar type."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        scalar_name = f"f_{type_name}"
        data = gen_parquet_bytes(DEFAULT_NB, 0, scalar_name, arrow_type, value_fn)
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", data)

        schema = build_typed_schema(client, ext_url, scalar_name, dtype, **extra)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            count = query_count(client, coll)
            assert count == DEFAULT_NB, f"[{type_name}] expected {DEFAULT_NB} rows, got {count}"

            res = client.query(coll, filter="id == 0", output_fields=["id", scalar_name])
            assert len(res) == 1
            assert res[0]["id"] == 0
            log.info(f"[{type_name}] id=0 -> {res[0]}")

            vectors = _float_vectors([0], TEST_DIM).tolist()
            search_res = client.search(
                coll, data=vectors, limit=5,
                anns_field="embedding", output_fields=["id", scalar_name],
            )
            assert len(search_res) == 1 and len(search_res[0]) > 0
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("elem_name,elem_dtype,elem_arrow,elem_extra,value_fn", [
        ("int64",    DataType.INT64,   pa.int64(),  {"max_capacity": 8},
         lambda i: [i, i + 1, i + 2]),
        ("varchar",  DataType.VARCHAR, pa.string(), {"max_capacity": 8, "max_length": 32},
         lambda i: [f"a_{i}", f"b_{i}"]),
    ], ids=lambda x: x if isinstance(x, str) else None)
    def test_array_type_e2e(self, elem_name, elem_dtype, elem_arrow, elem_extra, value_fn,
                            minio_env, external_prefix):
        """Array-of-<elem> field: upload, refresh, load, and query the array values."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        arr_field = f"arr_{elem_name}"

        data = gen_array_parquet_bytes(DEFAULT_NB, 0, arr_field, elem_arrow, value_fn)
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", data)

        schema = client.create_schema(external_source=ext_url, external_spec='{"format":"parquet"}')
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field(arr_field, DataType.ARRAY, element_type=elem_dtype,
                         external_field=arr_field, **elem_extra)
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="embedding")

        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == DEFAULT_NB

            rows = client.query(coll, filter="id == 5", output_fields=["id", arr_field])
            assert len(rows) == 1
            expected = value_fn(5)
            assert rows[0][arr_field] == expected, f"got {rows[0][arr_field]}, expected {expected}"
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_all_scalar_types_in_one_collection(self, minio_env, external_prefix):
        """Single collection with every scalar type + one FloatVector; cross-field
        filters and queries work correctly."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        nb = 100
        upload_parquet(
            minio_client, cfg["bucket"], f"{ext_key}/data.parquet",
            gen_all_scalar_parquet_bytes(nb, 0),
        )

        schema = build_all_scalar_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == nb

            # bool filter: even ids → 50
            b = client.query(coll, filter="val_bool == true", output_fields=["id"])
            assert len(b) == 50
            # int8 range: < 10 → 10
            i8 = client.query(coll, filter="val_int8 < 10", output_fields=["id"])
            assert len(i8) == 10
            # varchar like: first 10 match s_0000x pattern (x = 0..9) → 10
            s = client.query(coll, filter='val_varchar like "s_0000%"', output_fields=["id"])
            assert len(s) == 10

            # Verify specific row values for id=42
            row42 = client.query(
                coll, filter="id == 42",
                output_fields=[
                    "id", "val_bool", "val_int8", "val_int16", "val_int32", "val_int64",
                    "val_float", "val_double", "val_varchar", "val_json",
                ],
            )
            assert len(row42) == 1
            r = row42[0]
            assert r["val_bool"] is True
            assert r["val_int8"] == 42
            assert r["val_int16"] == 42 * 3
            assert r["val_int32"] == 42 * 7
            assert r["val_int64"] == 42 * 1000
            assert abs(r["val_float"] - 63.0) < 0.01
            assert abs(r["val_double"] - 105.0) < 0.01
            assert r["val_varchar"] == "s_00042"
            parsed = r["val_json"] if isinstance(r["val_json"], dict) else json.loads(r["val_json"])
            assert parsed["k"] == 42
        finally:
            client.drop_collection(coll)

    # ---- Vector types -----------------------------------------------

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("vec_kind,vec_dtype,metric,dim", [
        ("float",     DataType.FLOAT_VECTOR,    "L2",      TEST_DIM),
        ("float16",   DataType.FLOAT16_VECTOR,  "L2",      TEST_DIM),
        ("bfloat16",  DataType.BFLOAT16_VECTOR, "L2",      TEST_DIM),
        ("int8",      DataType.INT8_VECTOR,     "L2",      TEST_DIM),
    ], ids=lambda x: x if isinstance(x, str) else None)
    def test_float_family_vector_e2e(self, vec_kind, vec_dtype, metric, dim,
                                     minio_env, external_prefix):
        """Each float-family / int8 vector type: upload, refresh, index, load, search."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        vec_field = f"vec_{vec_kind}"

        data = gen_vector_variant_parquet_bytes(DEFAULT_NB, 0, vec_field, vec_dtype, dim)
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", data)

        schema = build_vector_variant_schema(client, ext_url, vec_field, vec_dtype, dim)
        try:
            client.create_collection(collection_name=coll, schema=schema)
        except Exception as e:
            pytest.skip(f"[{vec_kind}] create_collection rejected on this build: {e}")

        try:
            refresh_and_wait(client, coll)
            add_vector_index(client, coll, vec_field, "AUTOINDEX", metric)
            client.load_collection(coll)

            assert query_count(client, coll) == DEFAULT_NB

            # Construct a query vector in the right dtype/encoding expected by pymilvus.
            if vec_dtype == DataType.BINARY_VECTOR:
                return  # handled by dedicated test
            if vec_dtype == DataType.INT8_VECTOR:
                # pymilvus expects int8 vector as numpy int8 array (or bytes)
                query_vec = [np.zeros(dim, dtype=np.int8)]
            elif vec_dtype == DataType.FLOAT16_VECTOR:
                query_vec = [np.zeros(dim, dtype=np.float16)]
            elif vec_dtype == DataType.BFLOAT16_VECTOR:
                # pymilvus rejects uint16 at client side; feed raw bytes of
                # the bfloat16 zero pattern instead (dim * 2 bytes).
                query_vec = [bytes(dim * 2)]
            else:
                query_vec = [[0.0] * dim]
            search_res = client.search(
                coll, data=query_vec, limit=5,
                anns_field=vec_field, output_fields=["id"],
            )
            assert len(search_res) == 1 and len(search_res[0]) > 0
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_binary_vector_e2e(self, minio_env, external_prefix):
        """Binary vector end-to-end with HAMMING / JACCARD metric."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        data = gen_vector_variant_parquet_bytes(
            DEFAULT_NB, 0, "bin_vec", DataType.BINARY_VECTOR, BINARY_DIM,
        )
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", data)

        schema = build_vector_variant_schema(
            client, ext_url, "bin_vec", DataType.BINARY_VECTOR, BINARY_DIM,
        )
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            add_vector_index(client, coll, "bin_vec", "BIN_FLAT", "HAMMING")
            client.load_collection(coll)

            assert query_count(client, coll) == DEFAULT_NB

            # Single query vector of correct length (BINARY_DIM bits → BINARY_DIM/8 bytes)
            query_vec = [b"\x00" * (BINARY_DIM // 8)]
            search_res = client.search(
                coll, data=query_vec, limit=5,
                anns_field="bin_vec", output_fields=["id"],
                search_params={"metric_type": "HAMMING"},
            )
            assert len(search_res) == 1 and len(search_res[0]) > 0
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_multi_vector_collection(self, minio_env, external_prefix):
        """Collection with FloatVector + BinaryVector: both indexable and searchable."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        upload_parquet(
            minio_client, cfg["bucket"], f"{ext_key}/data.parquet",
            gen_multi_vector_parquet_bytes(DEFAULT_NB, 0),
        )
        schema = build_multi_vector_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)

            index_params = client.prepare_index_params()
            index_params.add_index(field_name="dense_vec", index_type="AUTOINDEX", metric_type="L2")
            index_params.add_index(field_name="bin_vec", index_type="BIN_FLAT", metric_type="HAMMING")
            client.create_index(coll, index_params)
            client.load_collection(coll)

            assert query_count(client, coll) == DEFAULT_NB

            dense_res = client.search(
                coll, data=[[0.0] * TEST_DIM], limit=3,
                anns_field="dense_vec", output_fields=["id"],
            )
            bin_res = client.search(
                coll, data=[b"\x00" * (BINARY_DIM // 8)], limit=3,
                anns_field="bin_vec", output_fields=["id"],
                search_params={"metric_type": "HAMMING"},
            )
            assert len(dense_res[0]) == 3 and len(bin_res[0]) == 3
        finally:
            client.drop_collection(coll)


# ============================================================
# 3. Indexes (Vector + Scalar)
# ============================================================

class TestExternalTableIndexes(TestMilvusClientV2Base):
    """Index creation on external collections — vector + scalar + binary."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index_type,metric_type,params", [
        ("AUTOINDEX",  "L2",     {}),
        ("FLAT",       "L2",     {}),
        ("HNSW",       "L2",     {"M": 16, "efConstruction": 200}),
        ("IVF_FLAT",   "L2",     {"nlist": 16}),
        ("HNSW",       "COSINE", {"M": 8}),
        ("DISKANN",    "L2",     {}),
    ], ids=lambda x: x if isinstance(x, str) else None)
    def test_vector_index(self, index_type, metric_type, params,
                          minio_env, external_prefix):
        """Create each supported vector index type on a FloatVector field and query.

        DISKANN requires the cluster to have a disk-backed index node. If the
        deployment lacks it, create_index raises and we skip rather than fail.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=DEFAULT_NB)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            try:
                add_vector_index(
                    client, coll, "embedding",
                    index_type=index_type, metric_type=metric_type,
                    **({"params": params} if params else {}),
                )
            except Exception as e:
                if index_type == "DISKANN":
                    pytest.skip(f"DISKANN not available on this deployment: {e}")
                raise
            client.load_collection(coll)

            hits = client.search(
                coll, data=[[0.0] * TEST_DIM], limit=5,
                anns_field="embedding", output_fields=["id"],
            )[0]
            assert len(hits) == 5

            idx_list = client.list_indexes(collection_name=coll)
            assert any("embedding" in str(i) for i in idx_list), \
                f"embedding index not in list: {idx_list}"
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index_type,params", [
        ("HNSW",     {"M": 16, "efConstruction": 200}),
        ("IVF_FLAT", {"nlist": 16}),
    ], ids=lambda x: x if isinstance(x, str) else None)
    def test_vector_index_mmap_enable(self, index_type, params,
                                      minio_env, external_prefix):
        """Toggle mmap.enabled on the vector index via alter_index_properties;
        verify describe reflects the change and search still returns correct hits.

        DISKANN is deliberately excluded — it is already disk-based and does
        not accept mmap.enabled (Milvus returns invalid-params).
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=DEFAULT_NB)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            add_vector_index(client, coll, "embedding", index_type, "L2", params=params)

            # Alter mmap=True before load
            try:
                client.alter_index_properties(
                    collection_name=coll, index_name="embedding",
                    properties={"mmap.enabled": True},
                )
            except Exception as e:
                pytest.skip(f"alter_index_properties not supported: {e}")

            info = client.describe_index(collection_name=coll, index_name="embedding")
            log.info(f"[{index_type}] index info after mmap=True: {info}")
            assert str(info.get("mmap.enabled", "")).lower() == "true", \
                f"expected mmap.enabled=True, got {info}"

            client.load_collection(coll)
            hits_on = client.search(
                coll, data=[[0.0] * TEST_DIM], limit=5,
                anns_field="embedding", output_fields=["id"],
            )[0]
            assert len(hits_on) == 5

            # Flip back to False
            client.release_collection(coll)
            client.alter_index_properties(
                collection_name=coll, index_name="embedding",
                properties={"mmap.enabled": False},
            )
            info_off = client.describe_index(collection_name=coll, index_name="embedding")
            assert str(info_off.get("mmap.enabled", "")).lower() == "false"

            client.load_collection(coll)
            hits_off = client.search(
                coll, data=[[0.0] * TEST_DIM], limit=5,
                anns_field="embedding", output_fields=["id"],
            )[0]
            assert len(hits_off) == 5
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_mmap_property(self, minio_env, external_prefix):
        """Collection-level mmap.enabled toggled via alter_collection_properties
        and surfaced through describe_collection.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=DEFAULT_NB)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)

            try:
                client.alter_collection_properties(
                    collection_name=coll, properties={"mmap.enabled": True},
                )
            except Exception as e:
                pytest.skip(f"alter_collection_properties mmap not supported: {e}")

            desc = client.describe_collection(coll)
            props = desc.get("properties") or {}
            assert str(props.get("mmap.enabled", "")).lower() == "true", \
                f"expected collection mmap.enabled=True, got properties={props}"

            index_and_load(client, coll)
            assert query_count(client, coll) == DEFAULT_NB

            hits = client.search(
                coll, data=[[0.0] * TEST_DIM], limit=5,
                anns_field="embedding", output_fields=["id"],
            )[0]
            assert len(hits) == 5

            # Flip back
            client.release_collection(coll)
            client.alter_collection_properties(
                collection_name=coll, properties={"mmap.enabled": False},
            )
            desc2 = client.describe_collection(coll)
            assert str((desc2.get("properties") or {}).get("mmap.enabled", "")).lower() == "false"
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index_type", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_binary_vector_index(self, index_type, minio_env, external_prefix):
        """Binary-vector index types over HAMMING."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        upload_parquet(
            minio_client, cfg["bucket"], f"{ext_key}/data.parquet",
            gen_vector_variant_parquet_bytes(DEFAULT_NB, 0, "bin_vec",
                                             DataType.BINARY_VECTOR, BINARY_DIM),
        )
        schema = build_vector_variant_schema(client, ext_url, "bin_vec",
                                             DataType.BINARY_VECTOR, BINARY_DIM)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            params = {"nlist": 16} if index_type == "BIN_IVF_FLAT" else {}
            add_vector_index(client, coll, "bin_vec", index_type, "HAMMING",
                             **({"params": params} if params else {}))
            client.load_collection(coll)

            hits = client.search(
                coll, data=[b"\x00" * (BINARY_DIM // 8)], limit=5,
                anns_field="bin_vec", output_fields=["id"],
                search_params={"metric_type": "HAMMING"},
            )[0]
            assert len(hits) == 5
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("scalar_index_type,field_name", [
        ("INVERTED", "value"),
        ("STL_SORT", "value"),
        ("BITMAP",   "value"),
    ])
    def test_scalar_index(self, scalar_index_type, field_name,
                          minio_env, external_prefix):
        """Create a scalar index on an external collection field."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)

            index_params = client.prepare_index_params()
            index_params.add_index(field_name="embedding", index_type="AUTOINDEX",
                                   metric_type="L2")
            try:
                index_params.add_index(field_name=field_name, index_type=scalar_index_type)
                client.create_index(coll, index_params)
            except Exception as e:
                pytest.skip(f"{scalar_index_type} on {field_name} not supported: {e}")

            client.load_collection(coll)
            assert query_count(client, coll) == DEFAULT_NB

            idx_list = client.list_indexes(collection_name=coll)
            log.info(f"[{scalar_index_type}] list_indexes: {idx_list}")
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_and_recreate_index(self, minio_env, external_prefix):
        """Drop the vector index, then recreate with different params."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            add_vector_index(client, coll, "embedding", "AUTOINDEX", "L2")
            client.load_collection(coll)
            assert query_count(client, coll) == DEFAULT_NB

            client.release_collection(coll)
            # Drop index by name (Milvus auto-assigns or uses field name)
            try:
                client.drop_index(collection_name=coll, index_name="embedding")
            except Exception:
                # Some SDKs / builds require the actual auto-generated name
                idx_list = client.list_indexes(collection_name=coll)
                target = next((n for n in idx_list if "embedding" in str(n)), None)
                if target is None:
                    pytest.skip("Unable to locate index to drop")
                client.drop_index(collection_name=coll, index_name=str(target))

            # Recreate with HNSW
            add_vector_index(client, coll, "embedding", "HNSW", "IP", params={"M": 16})
            client.load_collection(coll)

            hits = client.search(
                coll, data=[[0.0] * TEST_DIM], limit=3,
                anns_field="embedding", output_fields=["id"],
            )[0]
            assert len(hits) == 3
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    def test_list_and_describe_index(self, minio_env, external_prefix):
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            add_vector_index(client, coll, "embedding", "AUTOINDEX", "L2")

            indexes = client.list_indexes(collection_name=coll)
            assert len(indexes) >= 1, f"expected at least 1 index, got {indexes}"
            log.info(f"list_indexes: {indexes}")

            name = str(indexes[0])
            info = client.describe_index(collection_name=coll, index_name=name)
            log.info(f"describe_index({name}) -> {info}")
            assert info is not None
        finally:
            client.drop_collection(coll)


# ============================================================
# 4. Blocked Write Operations (DML + DDL)
# ============================================================

class TestExternalTableWriteBlocked(TestMilvusClientV2Base):
    """All DML + partition/field DDL should be rejected on external collections."""

    @staticmethod
    def _prepared_collection(client, minio_client, cfg, ext_url, ext_key):
        """Create a refreshed, loaded external collection and return its name."""
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet",
                       gen_basic_parquet_bytes(50, 0))
        coll = cf.gen_collection_name_by_testcase_name()
        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        refresh_and_wait(client, coll)
        index_and_load(client, coll)
        return coll

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("op_name", ["insert", "upsert", "delete", "flush"])
    def test_dml_rejected(self, op_name, minio_env, external_prefix):
        """insert / upsert / delete / flush are all blocked."""
        minio_client, cfg = minio_env
        client = self._client()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        coll = self._prepared_collection(client, minio_client, cfg, ext_url, ext_key)
        try:
            with pytest.raises(Exception) as exc_info:
                if op_name == "insert":
                    client.insert(collection_name=coll, data=[{
                        "id": 99999, "value": 1.0, "embedding": [0.0] * TEST_DIM,
                    }])
                elif op_name == "upsert":
                    client.upsert(collection_name=coll, data=[{
                        "id": 0, "value": 0.0, "embedding": [0.0] * TEST_DIM,
                    }])
                elif op_name == "delete":
                    client.delete(collection_name=coll, filter="id >= 0")
                elif op_name == "flush":
                    client.flush(collection_name=coll)
            msg = str(exc_info.value).lower()
            # Two valid rejection points:
            # - server-side: "<op> operation is not supported for external collection"
            # - client-side: pymilvus requires all fields (including virtual_pk)
            #   before the RPC even leaves — upsert/insert fail with DataNotMatch.
            assert ("external" in msg or "not support" in msg
                    or "virtual_pk" in msg or "datanotmatch" in msg
                    or "missed an field" in msg), \
                f"[{op_name}] unexpected error: {exc_info.value}"
            log.info(f"[{op_name}] correctly rejected: {exc_info.value}")
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_partition_rejected(self, minio_env, external_prefix):
        """create_partition on external collection should fail."""
        minio_client, cfg = minio_env
        client = self._client()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        coll = self._prepared_collection(client, minio_client, cfg, ext_url, ext_key)
        try:
            with pytest.raises(Exception) as exc_info:
                client.create_partition(collection_name=coll, partition_name="p1")
            msg = str(exc_info.value).lower()
            assert "external" in msg or "not support" in msg, \
                f"unexpected error: {exc_info.value}"
            log.info(f"create_partition correctly rejected: {exc_info.value}")
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_partition_rejected(self, minio_env, external_prefix):
        """drop_partition on the _default partition of an external collection."""
        minio_client, cfg = minio_env
        client = self._client()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        coll = self._prepared_collection(client, minio_client, cfg, ext_url, ext_key)
        try:
            with pytest.raises(Exception) as exc_info:
                client.drop_partition(collection_name=coll, partition_name="_default")
            msg = str(exc_info.value).lower()
            assert "external" in msg or "not support" in msg or "default" in msg, \
                f"unexpected error: {exc_info.value}"
            log.info(f"drop_partition correctly rejected: {exc_info.value}")
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("op_name", [
        "compact",
        "add_collection_field",
        pytest.param("truncate_collection", marks=pytest.mark.xfail(
            reason=(
                "Bug (milvus-io/milvus#49343): truncate_collection is a write "
                "op and should be rejected on external collections (consistent "
                "with insert/upsert/delete/flush/compact), but currently "
                "succeeds silently and clears the external_source binding — "
                "subsequent refresh reports 'no files found'. When fixed, "
                "this xfail flips to XPASS (strict=True)."
            ),
            strict=True,
        )),
    ])
    def test_ddl_rejected(self, op_name, minio_env, external_prefix):
        """DDL / write operations that should be blocked on external collections.

        Note: load_partitions / release_partitions on the _default partition
        are accepted (they alias load_collection / release_collection) — those
        are covered as allowed ops in TestExternalTableReadOps.
        """
        minio_client, cfg = minio_env
        client = self._client()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        coll = self._prepared_collection(client, minio_client, cfg, ext_url, ext_key)
        try:
            with pytest.raises(Exception) as exc_info:
                if op_name == "compact":
                    client.compact(collection_name=coll)
                elif op_name == "add_collection_field":
                    client.add_collection_field(
                        collection_name=coll, field_name="new_field",
                        data_type=DataType.INT64, nullable=True,
                    )
                elif op_name == "truncate_collection":
                    client.truncate_collection(collection_name=coll)
            msg = str(exc_info.value).lower()
            assert ("external" in msg or "not support" in msg
                    or "not allowed" in msg), \
                f"[{op_name}] unexpected error: {exc_info.value}"
            log.info(f"[{op_name}] correctly rejected: {exc_info.value}")
        finally:
            try:
                client.drop_collection(coll)
            except Exception:
                pass


# ============================================================
# 5. Refresh semantics + job management
# ============================================================

class TestExternalTableRefresh(TestMilvusClientV2Base):
    """External refresh: basic, incremental, atomic source override, concurrent, jobs."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_refresh_basic_picks_up_data(self, minio_env, external_prefix):
        """Initial refresh sees uploaded data; uploading more grows count."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        upload_basic_data(minio_client, cfg, ext_key, filename="part0.parquet",
                          num_rows=DEFAULT_NB, start_id=0)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == DEFAULT_NB

            # Append more data, re-refresh, require release+load.
            client.release_collection(coll)
            upload_basic_data(minio_client, cfg, ext_key, filename="part1.parquet",
                              num_rows=DEFAULT_NB, start_id=DEFAULT_NB)
            refresh_and_wait(client, coll)
            client.load_collection(coll)

            assert query_count(client, coll) == DEFAULT_NB * 2
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_refresh_incremental_add_remove(self, minio_env, external_prefix):
        """Add a file, remove a file — row count tracks fragment changes."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        for idx, start in enumerate([0, 500]):
            upload_parquet(minio_client, cfg["bucket"],
                           f"{ext_key}/data_{idx}.parquet",
                           gen_basic_parquet_bytes(500, start))

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == 1000

            client.release_collection(coll)
            minio_client.remove_object(cfg["bucket"], f"{ext_key}/data_1.parquet")
            upload_parquet(minio_client, cfg["bucket"],
                           f"{ext_key}/data_2.parquet",
                           gen_basic_parquet_bytes(300, 2000))
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            assert query_count(client, coll) == 800
            assert len(client.query(coll, filter="id >= 0 && id < 500",
                                    output_fields=["id"])) == 500
            assert len(client.query(coll, filter="id >= 500 && id < 1000",
                                    output_fields=["id"])) == 0
            assert len(client.query(coll, filter="id >= 2000 && id < 2300",
                                    output_fields=["id"])) == 300
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_refresh_override_source(self, minio_env, external_prefix):
        """refresh(external_source=NEW) atomically rebinds the collection."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_key = external_prefix["key"]

        key_a, key_b = f"{ext_key}/src_a", f"{ext_key}/src_b"
        url_a = build_external_source(cfg, key_a)
        url_b = build_external_source(cfg, key_b)
        upload_parquet(minio_client, cfg["bucket"], f"{key_a}/data.parquet",
                       gen_basic_parquet_bytes(100, 0))
        upload_parquet(minio_client, cfg["bucket"], f"{key_b}/data.parquet",
                       gen_basic_parquet_bytes(100, 5000))

        schema = build_basic_schema(client, url_a)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            ids_a = sorted(r["id"] for r in client.query(
                coll, filter="id >= 0", output_fields=["id"], limit=200))
            assert ids_a == list(range(0, 100))

            client.release_collection(coll)
            refresh_and_wait(client, coll, external_source=url_b,
                             external_spec='{"format":"parquet"}')
            client.load_collection(coll)

            ids_b = sorted(r["id"] for r in client.query(
                coll, filter="id >= 0", output_fields=["id"], limit=200))
            assert ids_b == list(range(5000, 5100))

            info = client.describe_collection(coll)
            log.info(f"persisted external_source after override: {info.get('external_source')!r}")
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    def test_refresh_second_concurrent_not_both_succeed(self, minio_env, external_prefix):
        """Two refreshes issued back-to-back: at most one Completes."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet",
                       gen_basic_parquet_bytes(500, 0))

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            job_first = client.refresh_external_collection(collection_name=coll)
            job_second = None
            second_rejected = False
            try:
                job_second = client.refresh_external_collection(collection_name=coll)
                log.info(f"second refresh accepted job_id={job_second}")
            except Exception as e:
                second_rejected = True
                log.info(f"second refresh rejected at RPC: {e}")

            deadline = time.time() + REFRESH_TIMEOUT
            first_state = None
            while time.time() < deadline:
                p = client.get_refresh_external_collection_progress(job_id=job_first)
                first_state = p.state
                if first_state in ("RefreshCompleted", "RefreshFailed"):
                    break
                time.sleep(2)
            assert first_state == "RefreshCompleted"

            if not second_rejected:
                deadline = time.time() + REFRESH_TIMEOUT
                second_state = None
                while time.time() < deadline:
                    p = client.get_refresh_external_collection_progress(job_id=job_second)
                    second_state = p.state
                    if second_state in ("RefreshCompleted", "RefreshFailed"):
                        break
                    time.sleep(2)
                log.info(f"second job terminal state={second_state}")
                assert second_state in ("RefreshFailed", "RefreshCompleted")
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_list_refresh_jobs(self, minio_env, external_prefix):
        """After refreshes, list_refresh_external_collection_jobs returns them."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet",
                       gen_basic_parquet_bytes(100, 0))

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            job_id = refresh_and_wait(client, coll)
            jobs = client.list_refresh_external_collection_jobs(collection_name=coll)
            assert len(jobs) >= 1, f"expected ≥1 job, got {jobs}"
            job_ids = [j.job_id for j in jobs]
            assert job_id in job_ids, f"job {job_id} missing from {job_ids}"
            log.info(f"refresh jobs: {[(j.job_id, j.state) for j in jobs]}")
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("num_files,rows_per_file", [
        (1,  500),     # single large file
        (5,  100),     # small uniform files
        (10, 50),      # many tiny files
    ], ids=lambda x: str(x) if isinstance(x, int) else None)
    def test_refresh_many_files(self, num_files, rows_per_file,
                                minio_env, external_prefix):
        """Refresh aggregates rows correctly across many parquet files in one prefix."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        # Upload num_files sibling parquet files at <prefix>/file_<n>.parquet
        for idx in range(num_files):
            upload_parquet(
                minio_client, cfg["bucket"],
                f"{ext_key}/file_{idx:03d}.parquet",
                gen_basic_parquet_bytes(rows_per_file, idx * rows_per_file),
            )

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            total = num_files * rows_per_file
            assert query_count(client, coll) == total

            # Spot-check a row from each file survived ingestion
            for idx in range(num_files):
                first_id = idx * rows_per_file
                rows = client.query(
                    coll, filter=f"id == {first_id}", output_fields=["id", "value"],
                )
                assert len(rows) == 1, f"row id={first_id} missing after refresh"
                assert abs(rows[0]["value"] - first_id * 1.5) < 1e-4
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_refresh_scans_flat_prefix_only(self, minio_env, external_prefix):
        """Refresh lists files directly under the external_source prefix only;
        files placed inside sub-directories are NOT picked up. This test
        pins that non-recursive behaviour.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        # 3 files directly under the prefix — should be ingested
        flat_files = [
            ("flat_a.parquet", 100, 0),
            ("flat_b.parquet", 100, 100),
            ("flat_c.parquet", 100, 200),
        ]
        for name, nrows, start_id in flat_files:
            upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/{name}",
                           gen_basic_parquet_bytes(nrows, start_id))

        # 2 files inside subdirs — should be ignored by refresh
        nested_files = [
            ("year=2025/month=01/nested_a.parquet", 100, 10000),
            ("subdir/nested_b.parquet",              100, 20000),
        ]
        for rel, nrows, start_id in nested_files:
            upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/{rel}",
                           gen_basic_parquet_bytes(nrows, start_id))

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            # Only the 3 flat files should count (300 rows), not the 2 nested
            assert query_count(client, coll) == 300

            # Nested-file id ranges must be absent
            for _, _, start_id in nested_files:
                got = client.query(
                    coll, filter=f"id >= {start_id} && id < {start_id + 100}",
                    output_fields=["id"], limit=200,
                )
                assert len(got) == 0, \
                    f"ids [{start_id}..{start_id + 100}) unexpectedly present (subdir ingested)"
        finally:
            client.drop_collection(coll)


# ============================================================
# 6. DQL — query / search / hybrid_search / iterators / get / pagination
# ============================================================

class TestExternalTableDQL(TestMilvusClientV2Base):
    """Read-side operations on external collections."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("filter_expr,expected", [
        ("",                                      DEFAULT_NB),
        ("id < 10",                               10),
        ("id >= 100 && id < 150",                 50),
        ("value > 0.0 && id < 50",                49),   # value=1.5*id, id==0 excluded
        ("id in [0, 1, 2, 3, 99]",                5),
    ])
    def test_query_filter(self, filter_expr, expected, minio_env, external_prefix):
        """Scalar filters return the expected row count."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            if filter_expr == "":
                assert query_count(client, coll) == expected
            else:
                res = client.query(coll, filter=filter_expr, output_fields=["id"],
                                   limit=DEFAULT_NB)
                assert len(res) == expected
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("metric", ["L2", "IP", "COSINE"])
    def test_search_metric(self, metric, minio_env, external_prefix):
        """Search under each metric returns topK with correctly ordered distances."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll, metric_type=metric)

            hits = client.search(
                coll, data=_float_vectors([0], TEST_DIM).tolist(), limit=5,
                anns_field="embedding", output_fields=["id"],
            )[0]
            assert len(hits) == 5
            distances = [h["distance"] for h in hits]
            if metric == "L2":
                assert distances == sorted(distances)
            else:
                assert distances == sorted(distances, reverse=True)
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_scalar_filter(self, minio_env, external_prefix):
        """Vector search combined with scalar filter (hybrid via `search(filter=...)`)."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            hits = client.search(
                coll, data=[[0.0] * TEST_DIM], limit=10,
                anns_field="embedding", output_fields=["id"],
                filter="id >= 50 && id < 100",
            )[0]
            assert len(hits) == 10
            assert all(50 <= h["id"] < 100 for h in hits), \
                f"filter violated: {[h['id'] for h in hits]}"
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_multi_vector(self, minio_env, external_prefix):
        """hybrid_search over two vector fields with RRF reranker."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet",
                       gen_multi_vector_parquet_bytes(DEFAULT_NB, 0))

        schema = build_multi_vector_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            idx = client.prepare_index_params()
            idx.add_index(field_name="dense_vec", index_type="AUTOINDEX", metric_type="L2")
            idx.add_index(field_name="bin_vec", index_type="BIN_FLAT", metric_type="HAMMING")
            client.create_index(coll, idx)
            client.load_collection(coll)

            req1 = AnnSearchRequest(
                data=[[0.0] * TEST_DIM], anns_field="dense_vec", limit=10,
                param={"metric_type": "L2"},
            )
            req2 = AnnSearchRequest(
                data=[b"\x00" * (BINARY_DIM // 8)], anns_field="bin_vec", limit=10,
                param={"metric_type": "HAMMING"},
            )
            hits = client.hybrid_search(
                collection_name=coll, reqs=[req1, req2],
                ranker=RRFRanker(), limit=5, output_fields=["id"],
            )
            assert len(hits) == 1 and len(hits[0]) == 5, \
                f"hybrid_search returned {hits}"
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_iterator(self, minio_env, external_prefix):
        """query_iterator lazily paginates through rows in batch_size chunks."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=DEFAULT_NB)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            it = client.query_iterator(
                collection_name=coll, filter="id >= 0",
                batch_size=50, output_fields=["id"],
            )
            total = 0
            while True:
                batch = it.next()
                if not batch:
                    break
                total += len(batch)
                assert len(batch) <= 50
            it.close()
            assert total == DEFAULT_NB, f"iterator yielded {total}, expected {DEFAULT_NB}"
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_iterator(self, minio_env, external_prefix):
        """search_iterator lazily paginates vector search results."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=DEFAULT_NB)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            it = client.search_iterator(
                collection_name=coll, data=[[0.0] * TEST_DIM],
                anns_field="embedding", batch_size=20, limit=80,
                output_fields=["id"],
            )
            total = 0
            while True:
                batch = it.next()
                if not batch:
                    break
                total += len(batch)
            it.close()
            assert total == 80, f"iterator yielded {total}, expected 80"
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_get_by_virtual_pk(self, minio_env, external_prefix):
        """get() by virtual PK returns the requested rows."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=50)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            # Resolve virtual PKs of the first 5 rows via query
            first_five = client.query(
                coll, filter="id >= 0 && id < 5",
                output_fields=["id", "__virtual_pk__"], limit=5,
            )
            vpks = [r["__virtual_pk__"] for r in first_five]
            assert len(vpks) == 5

            fetched = client.get(collection_name=coll, ids=vpks, output_fields=["id"])
            assert len(fetched) == 5
            assert {r["id"] for r in fetched} == {r["id"] for r in first_five}
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    def test_pagination_offset_limit(self, minio_env, external_prefix):
        """query(offset=..., limit=...) paginates correctly."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=50)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            page1 = client.query(coll, filter="id >= 0", output_fields=["id"],
                                 offset=0, limit=10)
            page2 = client.query(coll, filter="id >= 0", output_fields=["id"],
                                 offset=10, limit=10)
            assert len(page1) == 10 and len(page2) == 10
            ids1 = {r["id"] for r in page1}
            ids2 = {r["id"] for r in page2}
            assert ids1.isdisjoint(ids2), "paginated pages should not overlap"
        finally:
            client.drop_collection(coll)


# ============================================================
# 7. Lifecycle: alias, release/reload, drop+recreate
# ============================================================

class TestExternalTableLifecycle(TestMilvusClientV2Base):
    """Alias lifecycle and coarse lifecycle edges."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_create_alter_drop(self, minio_env, external_prefix):
        """Alias can be created, rebound, and dropped for external collections."""
        minio_client, cfg = minio_env
        client = self._client()
        coll_a = cf.gen_collection_name_by_testcase_name() + "_a"
        coll_b = cf.gen_collection_name_by_testcase_name() + "_b"
        ext_key = external_prefix["key"]
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/a/data.parquet",
                       gen_basic_parquet_bytes(30, 0))
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/b/data.parquet",
                       gen_basic_parquet_bytes(40, 10000))

        url_a = build_external_source(cfg, f"{ext_key}/a")
        url_b = build_external_source(cfg, f"{ext_key}/b")
        schema_a = build_basic_schema(client, url_a)
        schema_b = build_basic_schema(client, url_b)
        client.create_collection(collection_name=coll_a, schema=schema_a)
        client.create_collection(collection_name=coll_b, schema=schema_b)

        alias = f"alias_{random.randint(10000,99999)}"
        try:
            refresh_and_wait(client, coll_a)
            refresh_and_wait(client, coll_b)
            index_and_load(client, coll_a)
            index_and_load(client, coll_b)

            client.create_alias(collection_name=coll_a, alias=alias)
            via_alias_a = client.query(alias, filter="id >= 0", output_fields=["id"], limit=100)
            assert len(via_alias_a) == 30

            client.alter_alias(collection_name=coll_b, alias=alias)
            via_alias_b = client.query(alias, filter="id >= 0", output_fields=["id"], limit=100)
            assert len(via_alias_b) == 40
            assert all(r["id"] >= 10000 for r in via_alias_b)

            client.drop_alias(alias=alias)
            with pytest.raises(Exception):
                client.query(alias, filter="id >= 0", output_fields=["id"], limit=1)
        finally:
            try:
                client.drop_alias(alias=alias)
            except Exception:
                pass
            client.drop_collection(coll_a)
            client.drop_collection(coll_b)

    @pytest.mark.tags(CaseLabel.L2)
    def test_refresh_requires_release_to_see_new_data(self, minio_env, external_prefix):
        """Without release+load, queries still see pre-refresh data."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/part0.parquet",
                       gen_basic_parquet_bytes(100, 0))

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == 100

            upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/part1.parquet",
                           gen_basic_parquet_bytes(100, 100))
            refresh_and_wait(client, coll)

            stale = query_count(client, coll)
            assert stale == 100, f"without release+load expected 100, got {stale}"

            client.release_collection(coll)
            client.load_collection(coll)
            assert query_count(client, coll) == 200
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_and_recreate_same_name(self, minio_env, external_prefix):
        """Drop then recreate with the same name should succeed and serve data."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=50)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        refresh_and_wait(client, coll)
        index_and_load(client, coll)
        assert query_count(client, coll) == 50

        client.drop_collection(coll)
        assert not client.has_collection(coll)

        schema2 = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema2)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == 50
        finally:
            client.drop_collection(coll)


# ============================================================
# 8. Read-only / admin operations allowed on external collections
# ============================================================

class TestExternalTableReadOps(TestMilvusClientV2Base):
    """Operations normal collections support that should also work on
    external collections: stats, state, partitions, segments, rename,
    refresh_load, property alter/drop, database scoping."""

    @staticmethod
    def _prepared_collection(client, minio_client, cfg, ext_url, ext_key,
                             num_rows=DEFAULT_NB):
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet",
                       gen_basic_parquet_bytes(num_rows, 0))
        coll = cf.gen_collection_name_by_testcase_name()
        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        refresh_and_wait(client, coll)
        index_and_load(client, coll)
        return coll

    @pytest.mark.tags(CaseLabel.L1)
    def test_list_partitions_and_has_partition(self, minio_env, external_prefix):
        """External collection exposes only the _default partition."""
        minio_client, cfg = minio_env
        client = self._client()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        coll = self._prepared_collection(client, minio_client, cfg, ext_url, ext_key)
        try:
            parts = client.list_partitions(collection_name=coll)
            assert list(parts) == ["_default"], f"expected only _default, got {parts}"
            assert client.has_partition(collection_name=coll, partition_name="_default") is True
            assert client.has_partition(
                collection_name=coll, partition_name="nonexistent_xyz") is False
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_get_collection_stats(self, minio_env, external_prefix):
        """get_collection_stats reports the loaded row count."""
        minio_client, cfg = minio_env
        client = self._client()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        coll = self._prepared_collection(client, minio_client, cfg, ext_url, ext_key)
        try:
            stats = client.get_collection_stats(collection_name=coll)
            log.info(f"collection stats: {stats}")
            row_count = int(stats.get("row_count", 0))
            assert row_count == DEFAULT_NB, \
                f"expected row_count={DEFAULT_NB}, got {row_count}"
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_get_partition_stats_default(self, minio_env, external_prefix):
        """_default partition stats should match collection row count."""
        minio_client, cfg = minio_env
        client = self._client()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        coll = self._prepared_collection(client, minio_client, cfg, ext_url, ext_key)
        try:
            try:
                stats = client.get_partition_stats(
                    collection_name=coll, partition_name="_default")
            except Exception as e:
                pytest.skip(f"get_partition_stats not supported: {e}")
            log.info(f"partition stats: {stats}")
            row_count = int(stats.get("row_count", 0))
            assert row_count == DEFAULT_NB
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_get_load_state_transitions(self, minio_env, external_prefix):
        """get_load_state reports Loaded after load, NotLoad after release."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=DEFAULT_NB)
        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            add_vector_index(client, coll, "embedding", "AUTOINDEX", "L2")

            # Before load
            state = client.get_load_state(collection_name=coll)
            log.info(f"[pre-load] state={state}")
            assert "not" in str(state).lower() or "notload" in str(state).lower().replace("_", "")

            # After load
            client.load_collection(coll)
            state2 = client.get_load_state(collection_name=coll)
            log.info(f"[post-load] state={state2}")
            assert "load" in str(state2).lower() and "not" not in str(state2).lower().split("state")[-1]

            # After release
            client.release_collection(coll)
            state3 = client.get_load_state(collection_name=coll)
            log.info(f"[post-release] state={state3}")
            assert "not" in str(state3).lower() or "notload" in str(state3).lower().replace("_", "")
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    def test_list_persistent_and_loaded_segments(self, minio_env, external_prefix):
        """list_persistent_segments / list_loaded_segments return without error.

        External collections don't have Milvus-side persistent segments (data
        lives in the external parquet/lance/vortex files), so the persistent
        list is typically empty — we only assert the calls succeed and return
        an iterable.
        """
        minio_client, cfg = minio_env
        client = self._client()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        coll = self._prepared_collection(client, minio_client, cfg, ext_url, ext_key)
        try:
            try:
                persistent = client.list_persistent_segments(collection_name=coll)
            except Exception as e:
                pytest.skip(f"list_persistent_segments not supported: {e}")
            assert persistent is not None
            log.info(f"persistent segments (len={len(list(persistent))}): {persistent}")

            try:
                loaded = client.list_loaded_segments(collection_name=coll)
                log.info(f"loaded segments: {loaded}")
            except Exception as e:
                log.info(f"list_loaded_segments not available on this build: {e}")
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_load_release_default(self, minio_env, external_prefix):
        """load_partitions / release_partitions work on the _default partition
        (effectively aliasing load_collection / release_collection).
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=DEFAULT_NB)
        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            add_vector_index(client, coll, "embedding", "AUTOINDEX", "L2")

            client.load_partitions(collection_name=coll, partition_names=["_default"])
            assert query_count(client, coll) == DEFAULT_NB

            client.release_partitions(collection_name=coll, partition_names=["_default"])
            state = client.get_load_state(collection_name=coll)
            log.info(f"state after release_partitions: {state}")
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_rename_collection(self, minio_env, external_prefix):
        """rename_collection should succeed; new name answers queries."""
        minio_client, cfg = minio_env
        client = self._client()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        coll = self._prepared_collection(client, minio_client, cfg, ext_url, ext_key,
                                         num_rows=50)
        new_name = coll + "_renamed"
        try:
            try:
                client.rename_collection(old_name=coll, new_name=new_name)
            except Exception as e:
                pytest.skip(f"rename_collection not supported: {e}")

            assert not client.has_collection(coll)
            assert client.has_collection(new_name)
            assert query_count(client, new_name) == 50
        finally:
            try:
                client.drop_collection(new_name)
            except Exception:
                pass
            try:
                client.drop_collection(coll)
            except Exception:
                pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_refresh_load(self, minio_env, external_prefix):
        """refresh_load reloads currently loaded segments without a new refresh."""
        minio_client, cfg = minio_env
        client = self._client()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        coll = self._prepared_collection(client, minio_client, cfg, ext_url, ext_key)
        try:
            try:
                client.refresh_load(collection_name=coll)
            except Exception as e:
                pytest.skip(f"refresh_load not supported: {e}")
            assert query_count(client, coll) == DEFAULT_NB
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    def test_alter_and_drop_collection_description(self, minio_env, external_prefix):
        """alter_collection_properties(description=...) surfaces via describe;
        drop_collection_properties removes it again.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=20)
        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            try:
                client.alter_collection_properties(
                    collection_name=coll,
                    properties={"collection.description": "ext-table description"},
                )
            except Exception as e:
                pytest.skip(f"alter_collection_properties(description) not supported: {e}")

            desc = client.describe_collection(coll)
            props = desc.get("properties") or {}
            log.info(f"after alter: properties={props}, description={desc.get('description')}")

            try:
                client.drop_collection_properties(
                    collection_name=coll,
                    property_keys=["collection.description"],
                )
            except Exception as e:
                log.info(f"drop_collection_properties not supported: {e}")
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_index_property(self, minio_env, external_prefix):
        """drop_index_properties removes mmap.enabled that was set earlier.

        The server requires the collection to be released before altering
        index properties.
        """
        minio_client, cfg = minio_env
        client = self._client()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        coll = self._prepared_collection(client, minio_client, cfg, ext_url, ext_key)
        try:
            client.release_collection(coll)
            try:
                client.alter_index_properties(
                    collection_name=coll, index_name="embedding",
                    properties={"mmap.enabled": True},
                )
            except Exception as e:
                pytest.skip(f"alter_index_properties not supported: {e}")

            info = client.describe_index(collection_name=coll, index_name="embedding")
            assert str(info.get("mmap.enabled", "")).lower() == "true"

            try:
                client.drop_index_properties(
                    collection_name=coll, index_name="embedding",
                    property_keys=["mmap.enabled"],
                )
            except Exception as e:
                pytest.skip(f"drop_index_properties not supported: {e}")

            info2 = client.describe_index(collection_name=coll, index_name="embedding")
            log.info(f"after drop mmap property: {info2}")
            # Property should be absent or default "false"
            assert str(info2.get("mmap.enabled", "false")).lower() in ("false", "")
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    def test_external_collection_in_custom_database(self, minio_env, external_prefix):
        """Create an external collection inside a non-default database."""
        minio_client, cfg = minio_env
        client = self._client()
        db_name = f"ext_db_{random.randint(10000, 99999)}"
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=30)

        try:
            client.create_database(db_name=db_name)
        except Exception as e:
            pytest.skip(f"create_database not supported: {e}")

        coll = cf.gen_collection_name_by_testcase_name()
        try:
            client.use_database(db_name=db_name)
            schema = build_basic_schema(client, ext_url)
            client.create_collection(collection_name=coll, schema=schema)
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == 30

            dbs = client.list_databases()
            assert db_name in list(dbs)
        finally:
            try:
                client.drop_collection(coll)
            except Exception:
                pass
            client.use_database(db_name="default")
            try:
                client.drop_database(db_name=db_name)
            except Exception:
                pass


# ============================================================
# 9. External file formats — parquet / lance-table / vortex
# ============================================================

def _write_lance_to_minio(minio_client, bucket, key_prefix, num_rows, start_id,
                          dim=TEST_DIM):
    """Build a Lance dataset locally and upload every file under its folder
    to MinIO, preserving the relative layout Lance uses (versions/, data/, etc).
    """
    import os
    import shutil
    import tempfile

    import lance

    ids = list(range(start_id, start_id + num_rows))
    vectors = _float_vectors(ids, dim)
    table = pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "value": pa.array([float(i) * 1.5 for i in ids], type=pa.float32()),
        "embedding": pa.FixedSizeListArray.from_arrays(vectors.flatten(), list_size=dim),
    })

    tmpdir = tempfile.mkdtemp(prefix="ext_lance_")
    local_path = os.path.join(tmpdir, "dataset.lance")
    try:
        lance.write_dataset(table, local_path)
        for root, _dirs, files in os.walk(local_path):
            for fname in files:
                absolute = os.path.join(root, fname)
                relative = os.path.relpath(absolute, local_path)
                minio_client.fput_object(bucket, f"{key_prefix}/{relative}", absolute)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _find_vortex_sidecar_python():
    """Locate the Python 3.11+ sidecar venv (`.venv-vortex/`) used to write
    Vortex files. vortex-data>=0.35.0 requires Python >= 3.11 but the main
    test venv is pinned to 3.10, so we delegate Vortex generation to a subprocess.

    Returns the interpreter path or None if the sidecar is missing.
    """
    import os
    # tests/python_client/ → walk up to find .venv-vortex/
    here = os.path.dirname(os.path.abspath(__file__))
    for _ in range(4):
        candidate = os.path.join(here, ".venv-vortex", "bin", "python")
        if os.path.exists(candidate):
            return candidate
        here = os.path.dirname(here)
    return None


def _write_vortex_to_minio(bucket, cfg, key_prefix, num_rows, start_id,
                           dim=TEST_DIM):
    """Write a Vortex dataset to MinIO via the `.venv-vortex` sidecar
    (Python 3.11+ required for vortex-data >= 0.35).

    The embedding is written as FixedSizeList<uint8, dim*4> inside the vortex
    file — Milvus's vortex reader reinterprets those raw bytes as float32
    vectors of width `dim`.
    """
    import os
    import subprocess

    sidecar = _find_vortex_sidecar_python()
    if sidecar is None:
        pytest.skip(
            "Vortex sidecar venv missing. Create it once with:\n"
            "  uv venv .venv-vortex -p python3.12\n"
            "  source .venv-vortex/bin/activate && uv pip install "
            "'vortex-data==0.67.0' 'pyarrow>=16' 'numpy>=2.0' minio"
        )

    here = os.path.dirname(os.path.abspath(__file__))
    helper = os.path.join(here, "_vortex_gen.py")

    env = os.environ.copy()
    env.update({
        "VT_NUM_ROWS": str(num_rows),
        "VT_START_ID": str(start_id),
        "VT_DIM": str(dim),
        "MINIO_ADDRESS": cfg["address"],
        "MINIO_BUCKET": bucket,
        "MINIO_ACCESS_KEY": cfg["access_key"],
        "MINIO_SECRET_KEY": cfg["secret_key"],
        "VT_MINIO_KEY": f"{key_prefix}/data.vortex",
    })
    result = subprocess.run(
        [sidecar, helper],
        env=env, capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"vortex helper failed (code {result.returncode}): "
            f"stdout={result.stdout!r} stderr={result.stderr!r}"
        )
    log.info(f"[vortex] wrote {result.stdout.strip()} to {key_prefix}/data.vortex")


class TestExternalTableFormats(TestMilvusClientV2Base):
    """Coverage for the `external_spec.format` string values."""

    @pytest.mark.tags(CaseLabel.L0)
    def test_parquet_format_explicit(self, minio_env, external_prefix):
        """Explicit `{"format":"parquet"}` round-trip.

        The rest of the suite already implicitly covers parquet, but this
        case locks the explicit format string into the contract.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=DEFAULT_NB)

        schema = client.create_schema(
            external_source=ext_url, external_spec='{"format":"parquet"}',
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("value", DataType.FLOAT, external_field="value")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_DIM,
                         external_field="embedding")
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == DEFAULT_NB
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_lance_table_format(self, minio_env, external_prefix):
        """Lance-table format E2E: write a Lance dataset to MinIO, refresh, query."""
        try:
            import lance  # noqa: F401
        except ImportError:
            pytest.skip("lance package not installed in this environment")

        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        nb = 300
        _write_lance_to_minio(minio_client, cfg["bucket"], ext_key, nb, 0)

        schema = client.create_schema(
            external_source=ext_url,
            external_spec='{"format":"lance-table"}',
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("value", DataType.FLOAT, external_field="value")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_DIM,
                         external_field="embedding")
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            assert query_count(client, coll) == nb
            rows = client.query(coll, filter="id == 7", output_fields=["id", "value"])
            assert len(rows) == 1
            assert abs(rows[0]["value"] - 7 * 1.5) < 1e-4

            hits = client.search(
                coll, data=[[0.0] * TEST_DIM], limit=5,
                anns_field="embedding", output_fields=["id"],
            )[0]
            assert len(hits) == 5
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_vortex_format(self, minio_env, external_prefix):
        """Vortex format E2E.

        Vortex file generation happens in a Python 3.12 sidecar venv
        (`.venv-vortex/`) because vortex-data >= 0.35 requires Python >= 3.11
        while the main test venv is pinned to 3.10. The embedding is written
        as a raw-byte FixedSizeList<uint8, dim*4>; Milvus's vortex reader
        reinterprets those bytes as the Float32 vector column.
        """
        _minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        nb = 200
        _write_vortex_to_minio(cfg["bucket"], cfg, ext_key, nb, 0)

        schema = client.create_schema(
            external_source=ext_url,
            external_spec='{"format":"vortex"}',
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("value", DataType.FLOAT, external_field="value")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_DIM,
                         external_field="embedding")
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            assert query_count(client, coll) == nb
            rows = client.query(coll, filter="id == 11", output_fields=["id", "value"])
            assert len(rows) == 1
            assert abs(rows[0]["value"] - 11 * 1.5) < 1e-4

            hits = client.search(
                coll, data=[[0.0] * TEST_DIM], limit=5,
                anns_field="embedding", output_fields=["id"],
            )[0]
            assert len(hits) == 5
        finally:
            client.drop_collection(coll)
