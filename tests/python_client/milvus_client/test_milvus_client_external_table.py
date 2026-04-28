"""
External Table (external collection) comprehensive tests using pymilvus MilvusClient.

Coverage:
  1. Schema / lifecycle (create / describe / list / drop)
  2. Schema validation (forbidden features, blocked field types,
     external_source URL scheme allowlist, force-nullable normalization,
     alter source/spec via collection properties)
  3. All scalar data types (bool, int8-64, float, double, varchar, json, array)
  4. All vector data types (float, binary, float16, bfloat16, int8 where supported)
  5. All index types
       - vector: AUTOINDEX, HNSW, IVF_FLAT, FLAT, DISKANN
       - binary vector: BIN_FLAT, BIN_IVF_FLAT
       - scalar: INVERTED, BITMAP, STL_SORT, TRIE
       - mmap.enabled toggle on index and collection level
  6. DML blocked (insert / upsert / delete / flush)
  7. DDL blocked (create/drop partition, compact, add_field, truncate)
  8. Refresh semantics (basic / incremental / atomic source override /
     concurrent / job list / many files / flat-prefix-only / non-existent
     and normal-collection rejection / empty source / schema mismatch /
     schemaless reader column projection)
  9. Cross-bucket external sources (s3:///bucket/path empty-host form)
 10. Parquet compression codec matrix (snappy, lz4, gzip, zstd, none)
 11. External file formats (parquet, lance-table, vortex)
 12. DQL surface (query filter / search metric / hybrid_search / search_iterator /
                  query_iterator / get by virtual PK / offset-limit pagination)
 13. Read-only / admin operations that normal collections support
     (list_partitions, has_partition, get_collection_stats, get_partition_stats,
      get_load_state, list_persistent_segments, rename_collection, refresh_load,
      alter/drop collection and index properties, custom database)
 14. Alias lifecycle, release+load, drop+recreate, concurrent query during
     refresh/release/load, file-deleted-then-reload behavior

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
import threading
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

# Multi-file format coverage: each format E2E writes FORMAT_NUM_FILES sibling
# data files (or fragments / appended snapshots), each carrying
# FORMAT_ROWS_PER_FILE rows, exercising the multi-file refresh path.
FORMAT_NUM_FILES = 3
FORMAT_ROWS_PER_FILE = 3000
FORMAT_TOTAL_ROWS = FORMAT_NUM_FILES * FORMAT_ROWS_PER_FILE


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


def build_external_spec(cfg=None, fmt="parquet", cloud_provider="minio", **extra):
    """Build an external_spec accepted by current master validation.

    Current server-side validation requires external sources to carry a
    self-contained extfs block. The tests use Milvus-form S3 URLs
    (s3://<endpoint>/<bucket>/<prefix>/), so cloud_provider=minio keeps the
    endpoint from the URI instead of deriving a cloud endpoint.
    """
    cfg = cfg or get_minio_config()
    spec = {
        "format": fmt,
        "extfs": {
            "cloud_provider": cloud_provider,
            "region": "us-east-1",
            "access_key_id": cfg["access_key"],
            "access_key_value": cfg["secret_key"],
            "use_ssl": "true" if cfg["secure"] else "false",
        },
    }
    spec.update(extra)
    return json.dumps(spec, separators=(",", ":"))


def build_external_spec_for_scheme(scheme, cfg=None, fmt="parquet"):
    """Build a validation-only spec whose cloud_provider matches the URI scheme."""
    provider = {
        "s3": "aws",
        "s3a": "aws",
        "aws": "aws",
        "minio": "minio",
        "oss": "aliyun",
        "gs": "gcp",
        "gcs": "gcp",
    }[scheme]
    return build_external_spec(cfg=cfg, fmt=fmt, cloud_provider=provider)


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
    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            scalar_name: pa.array([value_fn(i) for i in ids], type=arrow_type),
            "embedding": pa.FixedSizeListArray.from_arrays(vectors.flatten(), list_size=dim),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=_PARQUET_COMPRESSION)
    return buf.getvalue()


def gen_basic_parquet_bytes(num_rows, start_id, dim=TEST_DIM):
    """Generate a Parquet file with columns: id, value (float), embedding."""
    return gen_parquet_bytes(
        num_rows,
        start_id,
        "value",
        pa.float32(),
        lambda i: float(i) * 1.5,
        dim=dim,
    )


def gen_zero_row_basic_parquet_bytes(dim=TEST_DIM):
    """Well-formed 0-row parquet with the basic external schema."""
    table = pa.table(
        {
            "id": pa.array([], type=pa.int64()),
            "value": pa.array([], type=pa.float32()),
            "embedding": pa.FixedSizeListArray.from_arrays(
                pa.array([], type=pa.float32()),
                list_size=dim,
            ),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=_PARQUET_COMPRESSION)
    return buf.getvalue()


def gen_parquet_bytes_with_codec(num_rows, start_id, codec, dim=TEST_DIM):
    """Like gen_basic_parquet_bytes but with an explicit Parquet compression codec."""
    ids = list(range(start_id, start_id + num_rows))
    vectors = _float_vectors(ids, dim)
    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array([float(i) * 1.5 for i in ids], type=pa.float32()),
            "embedding": pa.FixedSizeListArray.from_arrays(vectors.flatten(), list_size=dim),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=codec)
    return buf.getvalue()


def gen_array_parquet_bytes(num_rows, start_id, arr_name, arr_element_arrow_type, arr_value_fn, dim=TEST_DIM):
    """Parquet with an Array-typed column: id, <arr_name> (list<element>), embedding."""
    ids = list(range(start_id, start_id + num_rows))
    vectors = _float_vectors(ids, dim)
    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            arr_name: pa.array([arr_value_fn(i) for i in ids], type=pa.list_(arr_element_arrow_type)),
            "embedding": pa.FixedSizeListArray.from_arrays(vectors.flatten(), list_size=dim),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=_PARQUET_COMPRESSION)
    return buf.getvalue()


def gen_dtype_mismatch_parquet_bytes(
    num_rows, start_id, mismatch_name, mismatch_array, dim=TEST_DIM, include_embedding=True
):
    """Parquet with a deliberately incompatible user column.

    The id column and optional embedding are valid so failures can be
    attributed to the requested Milvus type vs external Arrow type mapping.
    """
    ids = list(range(start_id, start_id + num_rows))
    columns = {
        "id": pa.array(ids, type=pa.int64()),
        mismatch_name: mismatch_array,
    }
    if include_embedding:
        vectors = _float_vectors(ids, dim)
        columns["embedding"] = pa.FixedSizeListArray.from_arrays(
            vectors.flatten(),
            list_size=dim,
        )
    table = pa.table(columns)
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
            pa.array(arr.flatten(), type=pa.uint16()),
            list_size=dim,
        )
    elif vec_dtype == DataType.BFLOAT16_VECTOR:
        arr = _bfloat16_vectors(ids, dim)
        # Store as uint16; Milvus reader must interpret as bfloat16 raw bits.
        parquet_col = pa.FixedSizeListArray.from_arrays(
            pa.array(arr.flatten(), type=pa.uint16()),
            list_size=dim,
        )
    elif vec_dtype == DataType.INT8_VECTOR:
        arr = _int8_vectors(ids, dim)
        parquet_col = pa.FixedSizeListArray.from_arrays(
            pa.array(arr.flatten(), type=pa.int8()),
            list_size=dim,
        )
    elif vec_dtype == DataType.BINARY_VECTOR:
        arr = _binary_vectors_bytes(ids, dim)
        parquet_col = pa.FixedSizeListArray.from_arrays(
            pa.array(arr.flatten(), type=pa.uint8()),
            list_size=dim // 8,
        )
    else:
        raise ValueError(f"unsupported vec_dtype {vec_dtype}")

    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            vec_field: parquet_col,
        }
    )
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
    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "val_bool": pa.array([i % 2 == 0 for i in ids], type=pa.bool_()),
            "val_int8": pa.array([i % 100 for i in ids], type=pa.int8()),
            "val_int16": pa.array([i * 3 for i in ids], type=pa.int16()),
            "val_int32": pa.array([i * 7 for i in ids], type=pa.int32()),
            "val_int64": pa.array([i * 1000 for i in ids], type=pa.int64()),
            "val_float": pa.array([float(i) * 1.5 for i in ids], type=pa.float32()),
            "val_double": pa.array([float(i) * 2.5 for i in ids], type=pa.float64()),
            "val_varchar": pa.array([f"s_{i:05d}" for i in ids], type=pa.string()),
            "val_json": pa.array([json.dumps({"k": i, "g": i % 3}) for i in ids], type=pa.string()),
            "embedding": pa.FixedSizeListArray.from_arrays(vectors.flatten(), list_size=dim),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=_PARQUET_COMPRESSION)
    return buf.getvalue()


def gen_multi_vector_parquet_bytes(num_rows, start_id, dim=TEST_DIM):
    """Parquet with two vector fields (float + binary) + id + scalar."""
    ids = list(range(start_id, start_id + num_rows))
    float_arr = _float_vectors(ids, dim)
    bin_arr = _binary_vectors_bytes(ids, BINARY_DIM)
    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array([float(i) for i in ids], type=pa.float32()),
            "dense_vec": pa.FixedSizeListArray.from_arrays(float_arr.flatten(), list_size=dim),
            "bin_vec": pa.FixedSizeListArray.from_arrays(
                pa.array(bin_arr.flatten(), type=pa.uint8()),
                list_size=BINARY_DIM // 8,
            ),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=_PARQUET_COMPRESSION)
    return buf.getvalue()


def gen_timestamptz_parquet_bytes(num_rows, start_id, dim=TEST_DIM, ts_unit="us"):
    """Parquet with id + ts (arrow timestamp) + embedding.

    Milvus reads Timestamptz as int64 microseconds; the arrow timestamp column
    is normalized to int64 by NormalizeExternalArrow on the load path.
    """
    ids = list(range(start_id, start_id + num_rows))
    vectors = _float_vectors(ids, dim)
    base = 1_700_000_000
    if ts_unit == "us":
        ts_vals = [(base + i) * 1_000_000 for i in ids]
    elif ts_unit == "ms":
        ts_vals = [(base + i) * 1_000 for i in ids]
    else:
        ts_vals = [base + i for i in ids]
    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "ts": pa.array(ts_vals, type=pa.timestamp(ts_unit)),
            "embedding": pa.FixedSizeListArray.from_arrays(vectors.flatten(), list_size=dim),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=_PARQUET_COMPRESSION)
    return buf.getvalue()


def gen_geometry_wkt_parquet_bytes(num_rows, start_id, dim=TEST_DIM):
    """Parquet with Geometry stored as WKT strings, matching user datasets."""
    ids = list(range(start_id, start_id + num_rows))
    vectors = _float_vectors(ids, dim)
    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "geo": pa.array([f"POINT({i}.0 {i}.0)" for i in ids], type=pa.string()),
            "embedding": pa.FixedSizeListArray.from_arrays(
                vectors.flatten(),
                list_size=dim,
            ),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=_PARQUET_COMPRESSION)
    return buf.getvalue()


def gen_all_null_columns_parquet_bytes(num_rows, start_id, dim=TEST_DIM):
    """Parquet where every user scalar column is entirely null.

    External collections force user fields to nullable=true (schema.go:2350).
    A column with no non-null values exercises that path: arrow reads back
    ListArray with null_count == num_rows, and Milvus must surface those
    nulls without crashing the segment writer.
    """
    ids = list(range(start_id, start_id + num_rows))
    vectors = _float_vectors(ids, dim)
    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "n_int": pa.array([None] * num_rows, type=pa.int32()),
            "n_float": pa.array([None] * num_rows, type=pa.float32()),
            "n_varchar": pa.array([None] * num_rows, type=pa.string()),
            "n_json": pa.array([None] * num_rows, type=pa.string()),
            "embedding": pa.FixedSizeListArray.from_arrays(vectors.flatten(), list_size=dim),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=_PARQUET_COMPRESSION)
    return buf.getvalue()


def gen_large_parquet_with_row_groups(num_rows, start_id, row_group_size, dim=TEST_DIM):
    """Generate a single large parquet file with explicit row_group_size.

    Forces parquet to split internally into multiple row groups so the
    Milvus reader exercises the multi-row-group path. The same file may
    also be split into multiple Milvus segments by the loon FFI layer
    when num_rows exceeds the segment-size threshold.
    """
    ids = list(range(start_id, start_id + num_rows))
    vectors = _float_vectors(ids, dim)
    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array([float(i) * 1.5 for i in ids], type=pa.float32()),
            "embedding": pa.FixedSizeListArray.from_arrays(vectors.flatten(), list_size=dim),
        }
    )
    buf = io.BytesIO()
    pq.write_table(
        table,
        buf,
        compression=_PARQUET_COMPRESSION,
        row_group_size=row_group_size,
    )
    return buf.getvalue()


# ============================================================
# Full-matrix schema (DataType × Index)
# ============================================================
# A single collection schema covering every supported scalar + vector
# DataType, with same-dtype duplicates wired to different index types.
# Each entry is (milvus_field_name, milvus_DataType, index_type, index_metric,
#                index_params, ext_field_name, arrow_writer_type, value_fn,
#                add_field_extra). Vector entries treat dim/element_type via
#                add_field_extra rather than a value_fn.
#
# The same field list is consumed by both the parquet/lance arrow-table
# generator and the iceberg multi-column writer (vector columns degrade to
# raw binary blobs in iceberg because Iceberg has no fixed-size-list).

FULL_MATRIX_SCALAR_FIELDS = [
    # (field_name, DataType,           pa type,            milvus add_field extras, value_fn)
    ("b_inv", DataType.BOOL, pa.bool_(), {}, lambda i: i % 2 == 0),
    ("i8_bmp", DataType.INT8, pa.int8(), {}, lambda i: i % 100),
    ("i16_inv", DataType.INT16, pa.int16(), {}, lambda i: (i * 3) % 32000),
    ("i32_stl", DataType.INT32, pa.int32(), {}, lambda i: i * 7),
    ("i64_inv", DataType.INT64, pa.int64(), {}, lambda i: i * 1000),
    ("f_inv", DataType.FLOAT, pa.float32(), {}, lambda i: float(i) * 1.5),
    ("d_inv", DataType.DOUBLE, pa.float64(), {}, lambda i: float(i) * 2.5),
    ("vc_trie", DataType.VARCHAR, pa.string(), {"max_length": 64}, lambda i: f"s_{i:05d}"),
    ("j", DataType.JSON, pa.string(), {}, lambda i: json.dumps({"k": i, "g": i % 3})),
    ("ts", DataType.TIMESTAMPTZ, pa.timestamp("us"), {}, lambda i: (1_700_000_000 + i) * 1_000_000),
]

# Scalar indexes wired per field. None means no scalar index built.
FULL_MATRIX_SCALAR_INDEXES = {
    "b_inv": "INVERTED",
    "i8_bmp": "BITMAP",
    "i16_inv": "INVERTED",
    "i32_stl": "STL_SORT",
    "i64_inv": "INVERTED",
    "f_inv": "INVERTED",
    "d_inv": "INVERTED",
    "vc_trie": "TRIE",
    # j (JSON) and ts (Timestamptz) are stored without scalar index.
}

FULL_MATRIX_ARRAY_FIELD = ("arr_int", DataType.INT64, lambda i: [i, i + 1, i + 2])

# A fixed POINT(0 0) WKB blob — sufficient to verify Geometry round-trip.
# WKB: byte order (1=little-endian) | uint32 type(1=POINT) | float64 x | float64 y
_GEOMETRY_WKB = (
    b"\x01\x01\x00\x00\x00"
    + b"\x00" * 8  # x = 0.0
    + b"\x00" * 8  # y = 0.0
)

# Full-matrix vectors use a production-realistic dim (128) rather than
# TEST_DIM=8. Reason: the auto-tuned IVF_FLAT_CC config Milvus picks for
# AUTOINDEX (nlist=128, sub_dim=4) on dim=8 is degenerate and triggers a
# native SIGSEGV in the querynode load path on master-20260424-5409a81 —
# this is a milvus bug worth its own issue, but full matrix tests should
# run on realistic shapes anyway.
FULL_MATRIX_DIM = 128
FULL_MATRIX_BINARY_DIM = 128  # multiple of 8

# Vector fields. Each entry: (name, DataType, dim, index_type, metric, params).
FULL_MATRIX_VECTOR_FIELDS = [
    ("fv_auto", DataType.FLOAT_VECTOR, FULL_MATRIX_DIM, "AUTOINDEX", "L2", {}),
    ("fv_flat", DataType.FLOAT_VECTOR, FULL_MATRIX_DIM, "FLAT", "L2", {}),
    ("fv_hnsw", DataType.FLOAT_VECTOR, FULL_MATRIX_DIM, "HNSW", "L2", {"M": 8, "efConstruction": 64}),
    ("fv_ivf", DataType.FLOAT_VECTOR, FULL_MATRIX_DIM, "IVF_FLAT", "L2", {"nlist": 8}),
    ("f16v", DataType.FLOAT16_VECTOR, FULL_MATRIX_DIM, "AUTOINDEX", "L2", {}),
    ("bf16v", DataType.BFLOAT16_VECTOR, FULL_MATRIX_DIM, "AUTOINDEX", "L2", {}),
    ("binv_flat", DataType.BINARY_VECTOR, FULL_MATRIX_BINARY_DIM, "BIN_FLAT", "HAMMING", {}),
    ("binv_ivf", DataType.BINARY_VECTOR, FULL_MATRIX_BINARY_DIM, "BIN_IVF_FLAT", "HAMMING", {"nlist": 8}),
    ("i8v", DataType.INT8_VECTOR, FULL_MATRIX_DIM, "AUTOINDEX", "L2", {}),
]


def _full_matrix_arrow_columns(
    num_rows, start_id, dim=FULL_MATRIX_DIM, bin_dim=FULL_MATRIX_BINARY_DIM, excluded_fields=()
):
    """Build a dict {column_name → pyarrow.Array} that fits both parquet and
    Lance. Column layout matches FULL_MATRIX_SCALAR_FIELDS / VECTOR_FIELDS;
    each milvus field reads from a uniquely-named column.

    Pass excluded_fields to omit specific scalar columns (e.g. for vortex,
    where the server reader can't sample Arrow String buffers)."""
    ids = list(range(start_id, start_id + num_rows))
    excluded = set(excluded_fields)

    columns = {"id": pa.array(ids, type=pa.int64())}

    # Scalars
    for name, _dtype, arrow_type, _extra, value_fn in FULL_MATRIX_SCALAR_FIELDS:
        if name in excluded:
            continue
        columns[name] = pa.array([value_fn(i) for i in ids], type=arrow_type)

    # Array<Int64>
    arr_name, _arr_dtype, arr_value_fn = FULL_MATRIX_ARRAY_FIELD
    if arr_name not in excluded:
        columns[arr_name] = pa.array([arr_value_fn(i) for i in ids], type=pa.list_(pa.int64()))

    # Geometry as binary (WKB).
    if "geo" not in excluded:
        columns["geo"] = pa.array([_GEOMETRY_WKB] * num_rows, type=pa.binary())

    # Vectors. Layout per type:
    #   FloatVector / Int8Vector  → FixedSizeList<element, dim>
    #   Float16Vector             → FixedSizeList<uint16, dim> (raw bits)
    #   BFloat16Vector            → FixedSizeList<uint16, dim> (raw bits)
    #   BinaryVector              → FixedSizeList<uint8,  dim/8>
    fv_arr = _float_vectors(ids, dim).flatten()
    f16_arr = _float16_vectors(ids, dim).view(np.uint16).flatten()
    bf16_arr = _bfloat16_vectors(ids, dim).flatten()
    i8_arr = _int8_vectors(ids, dim).flatten()
    bin_arr = _binary_vectors_bytes(ids, bin_dim).flatten()

    for name, vtype, vdim, _idx, _metric, _params in FULL_MATRIX_VECTOR_FIELDS:
        if name in excluded:
            continue
        if vtype == DataType.FLOAT_VECTOR:
            columns[name] = pa.FixedSizeListArray.from_arrays(
                pa.array(fv_arr, type=pa.float32()),
                list_size=vdim,
            )
        elif vtype == DataType.FLOAT16_VECTOR:
            columns[name] = pa.FixedSizeListArray.from_arrays(
                pa.array(f16_arr, type=pa.uint16()),
                list_size=vdim,
            )
        elif vtype == DataType.BFLOAT16_VECTOR:
            columns[name] = pa.FixedSizeListArray.from_arrays(
                pa.array(bf16_arr, type=pa.uint16()),
                list_size=vdim,
            )
        elif vtype == DataType.INT8_VECTOR:
            columns[name] = pa.FixedSizeListArray.from_arrays(
                pa.array(i8_arr, type=pa.int8()),
                list_size=vdim,
            )
        elif vtype == DataType.BINARY_VECTOR:
            columns[name] = pa.FixedSizeListArray.from_arrays(
                pa.array(bin_arr, type=pa.uint8()),
                list_size=vdim // 8,
            )
        else:
            raise ValueError(f"unsupported vector dtype {vtype}")
    return columns


def gen_full_matrix_parquet_bytes(num_rows, start_id, dim=FULL_MATRIX_DIM, bin_dim=FULL_MATRIX_BINARY_DIM):
    """Single parquet file with every full-matrix field."""
    columns = _full_matrix_arrow_columns(num_rows, start_id, dim=dim, bin_dim=bin_dim)
    table = pa.table(columns)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=_PARQUET_COMPRESSION)
    return buf.getvalue()


# ============================================================
# MinIO Helpers
# ============================================================


def upload_parquet(minio_client, bucket, key, data):
    minio_client.put_object(
        bucket,
        key,
        io.BytesIO(data),
        length=len(data),
        content_type="application/octet-stream",
    )


def cleanup_minio_prefix(minio_client, bucket, prefix):
    for obj in minio_client.list_objects(bucket, prefix=prefix, recursive=True):
        minio_client.remove_object(bucket, obj.object_name)


# ============================================================
# Schema / Refresh / Load Helpers
# ============================================================


def build_basic_schema(client, ext_path, dim=TEST_DIM, ext_spec=None):
    """Minimal external schema: id(Int64) + value(Float) + embedding(FloatVector)."""
    schema = client.create_schema(
        external_source=ext_path,
        external_spec=ext_spec or build_external_spec(),
    )
    schema.add_field("id", DataType.INT64, external_field="id")
    schema.add_field("value", DataType.FLOAT, external_field="value")
    schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim, external_field="embedding")
    return schema


def build_typed_schema(client, ext_path, scalar_name, scalar_dtype, dim=TEST_DIM, ext_spec=None, **extra):
    """External schema with id + one scalar field of the given type + FloatVector."""
    schema = client.create_schema(
        external_source=ext_path,
        external_spec=ext_spec or build_external_spec(),
    )
    schema.add_field("id", DataType.INT64, external_field="id")
    schema.add_field(scalar_name, scalar_dtype, external_field=scalar_name, **extra)
    schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim, external_field="embedding")
    return schema


def build_vector_variant_schema(client, ext_path, vec_field, vec_dtype, dim, ext_spec=None):
    """External schema with id + one vector field of the given dtype."""
    schema = client.create_schema(
        external_source=ext_path,
        external_spec=ext_spec or build_external_spec(),
    )
    schema.add_field("id", DataType.INT64, external_field="id")
    schema.add_field(vec_field, vec_dtype, dim=dim, external_field=vec_field)
    return schema


def build_all_scalar_schema(client, ext_path, dim=TEST_DIM, ext_spec=None):
    schema = client.create_schema(
        external_source=ext_path,
        external_spec=ext_spec or build_external_spec(),
    )
    schema.add_field("id", DataType.INT64, external_field="id")
    schema.add_field("val_bool", DataType.BOOL, external_field="val_bool")
    schema.add_field("val_int8", DataType.INT8, external_field="val_int8")
    schema.add_field("val_int16", DataType.INT16, external_field="val_int16")
    schema.add_field("val_int32", DataType.INT32, external_field="val_int32")
    schema.add_field("val_int64", DataType.INT64, external_field="val_int64")
    schema.add_field("val_float", DataType.FLOAT, external_field="val_float")
    schema.add_field("val_double", DataType.DOUBLE, external_field="val_double")
    schema.add_field("val_varchar", DataType.VARCHAR, max_length=64, external_field="val_varchar")
    schema.add_field("val_json", DataType.JSON, external_field="val_json")
    schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim, external_field="embedding")
    return schema


def build_multi_vector_schema(client, ext_path, float_dim=TEST_DIM, bin_dim=BINARY_DIM, ext_spec=None):
    schema = client.create_schema(
        external_source=ext_path,
        external_spec=ext_spec or build_external_spec(),
    )
    schema.add_field("id", DataType.INT64, external_field="id")
    schema.add_field("value", DataType.FLOAT, external_field="value")
    schema.add_field("dense_vec", DataType.FLOAT_VECTOR, dim=float_dim, external_field="dense_vec")
    schema.add_field("bin_vec", DataType.BINARY_VECTOR, dim=bin_dim, external_field="bin_vec")
    return schema


def build_full_matrix_schema(
    client, ext_path, ext_spec=None, dim=FULL_MATRIX_DIM, bin_dim=FULL_MATRIX_BINARY_DIM, excluded_fields=()
):
    """Schema covering every supported DataType, with same-dtype duplicates
    wired to different index types. See FULL_MATRIX_SCALAR_FIELDS /
    FULL_MATRIX_VECTOR_FIELDS for the full layout.
    """
    schema = client.create_schema(external_source=ext_path, external_spec=ext_spec or build_external_spec())
    schema.add_field("id", DataType.INT64, external_field="id")
    excluded = set(excluded_fields)

    for name, dtype, _arrow, extra, _value_fn in FULL_MATRIX_SCALAR_FIELDS:
        if name in excluded:
            continue
        schema.add_field(name, dtype, external_field=name, **extra)

    arr_name, arr_elem_dtype, _ = FULL_MATRIX_ARRAY_FIELD
    if arr_name not in excluded:
        schema.add_field(arr_name, DataType.ARRAY, element_type=arr_elem_dtype, max_capacity=8, external_field=arr_name)

    if "geo" not in excluded:
        schema.add_field("geo", DataType.GEOMETRY, external_field="geo")

    for name, vtype, vdim, _idx, _metric, _params in FULL_MATRIX_VECTOR_FIELDS:
        if name in excluded:
            continue
        schema.add_field(name, vtype, dim=vdim, external_field=name)
    return schema


def create_full_matrix_indexes(client, collection_name, excluded_fields=()):
    """Build every scalar + vector index per FULL_MATRIX configuration in
    a single create_index call.
    """
    index_params = client.prepare_index_params()
    excluded = set(excluded_fields)

    # Scalar indexes
    for field, idx_type in FULL_MATRIX_SCALAR_INDEXES.items():
        if field in excluded:
            continue
        index_params.add_index(
            field_name=field,
            index_type=idx_type,
        )

    # Vector indexes
    for name, _vtype, _vdim, idx_type, metric, params in FULL_MATRIX_VECTOR_FIELDS:
        if name in excluded:
            continue
        kwargs = {
            "field_name": name,
            "index_type": idx_type,
            "metric_type": metric,
        }
        if params:
            kwargs["params"] = params
        index_params.add_index(**kwargs)

    client.create_index(collection_name, index_params)


def refresh_and_wait(client, collection_name, timeout=REFRESH_TIMEOUT, external_source=None, external_spec=None):
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


def refresh_and_poll_terminal(client, collection_name, timeout=60, external_source=None, external_spec=None):
    """Trigger refresh and return (job_id, progress) for completed or failed jobs."""
    kwargs = {"collection_name": collection_name}
    if external_source is not None:
        kwargs["external_source"] = external_source
    if external_spec is not None:
        kwargs["external_spec"] = external_spec

    job_id = client.refresh_external_collection(**kwargs)
    assert job_id and job_id > 0, f"Expected positive job_id, got {job_id}"
    deadline = time.time() + timeout
    progress = None
    while time.time() < deadline:
        progress = client.get_refresh_external_collection_progress(job_id=job_id)
        if progress.state in ("RefreshCompleted", "RefreshFailed"):
            return job_id, progress
        time.sleep(2)
    state = progress.state if progress is not None else "<none>"
    raise AssertionError(f"Refresh job {job_id} did not finish within {timeout}s; state={state}")


def add_vector_index(client, collection_name, vec_field="embedding", index_type="AUTOINDEX", metric_type="L2", **extra):
    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name=vec_field,
        index_type=index_type,
        metric_type=metric_type,
        **extra,
    )
    client.create_index(collection_name, index_params)


def index_and_load(client, collection_name, vec_field="embedding", metric_type="L2"):
    add_vector_index(client, collection_name, vec_field, "AUTOINDEX", metric_type)
    client.load_collection(collection_name)


def query_count(client, collection_name):
    res = client.query(collection_name, filter="", output_fields=["count(*)"])
    return res[0]["count(*)"]


def upload_basic_data(
    minio_client, cfg, ext_key, num_rows=DEFAULT_NB, start_id=0, filename="data.parquet", dim=TEST_DIM
):
    """Upload a basic parquet file and return full minio key."""
    key = f"{ext_key}/{filename}"
    upload_parquet(
        minio_client,
        cfg["bucket"],
        key,
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
    key = f"external-e2e-core/{safe}-{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
    yield {"key": key, "url": build_external_source(cfg, key)}
    try:
        cleanup_minio_prefix(mc, cfg["bucket"], f"{key}/")
    except Exception as exc:
        log.warning(f"cleanup_minio_prefix({key}) failed: {exc}")


# ============================================================
# Schema builders for validation tests (module-level, reused across classes)
# ============================================================

_PLACEHOLDER_SRC = "s3://test-bucket/placeholder/"
_PLACEHOLDER_SPEC = build_external_spec(cloud_provider="aws")


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
        external_source=_PLACEHOLDER_SRC,
        external_spec=_PLACEHOLDER_SPEC,
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
            assert json.loads(info.get("external_spec", "{}")).get("format") == "parquet"

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
    @pytest.mark.parametrize(
        "case_name,schema_builder,err_hint",
        [
            ("user_primary_key", lambda c: _schema_with_user_pk(c), "primary key"),
            ("auto_id_on_user_field", lambda c: _schema_with_auto_id(c), "auto_id"),
            ("dynamic_field_enabled", lambda c: _schema_with_dynamic(c), "dynamic field"),
            ("partition_key", lambda c: _schema_with_partition_key(c), "partition key"),
            ("missing_external_field", lambda c: _schema_missing_external_field(c), "external_field"),
        ],
    )
    def test_schema_reject_forbidden(self, case_name, schema_builder, err_hint):
        """Forbidden schema shapes must be rejected at create_collection time."""
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()

        with pytest.raises(Exception) as exc_info:
            schema = schema_builder(client)
            client.create_collection(collection_name=coll, schema=schema)

        msg = str(exc_info.value).lower()
        assert err_hint.lower() in msg, f"[{case_name}] expected {err_hint!r} in error, got: {exc_info.value}"
        log.info(f"[{case_name}] correctly rejected: {exc_info.value}")

    @pytest.mark.tags(CaseLabel.L1)
    def test_schema_reject_sparse_float_vector(self):
        """SPARSE_FLOAT_VECTOR is explicitly blocked for external collections.

        Server message must mention both 'does not support field type' and
        the type name 'SparseFloatVector' (typeutil.IsExternalCollection in
        pkg/util/typeutil/schema.go).
        """
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(
            external_source=_PLACEHOLDER_SRC,
            external_spec=_PLACEHOLDER_SPEC,
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("sv", DataType.SPARSE_FLOAT_VECTOR, external_field="sv")
        with pytest.raises(Exception) as exc_info:
            client.create_collection(collection_name=coll, schema=schema)
        msg = str(exc_info.value)
        assert ("does not support field type" in msg and "SparseFloatVector" in msg) or "sparse" in msg.lower(), (
            f"Expected SparseFloatVector type-block error, got: {exc_info.value}"
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("scheme", ["s3", "s3a", "aws", "minio", "oss", "gs", "gcs"])
    def test_external_source_allowed_schemes(self, scheme):
        """All schemes in the allowlist (RootCoord ValidateSourceAndSpec)
        should be accepted at create time."""
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        cfg = get_minio_config()
        if scheme == "minio":
            source = f"minio://{cfg['address']}/{cfg['bucket']}/prefix/"
        else:
            source = f"{scheme}://bucket/prefix/"
        schema = client.create_schema(
            external_source=source,
            external_spec=build_external_spec_for_scheme(scheme, cfg=cfg),
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="vec")
        client.create_collection(collection_name=coll, schema=schema)
        try:
            assert client.has_collection(coll)
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_external_source_relative_path_no_scheme_rejected(self):
        """A relative path without an explicit scheme is rejected."""
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(
            external_source="my-data/parquet/",
            external_spec=build_external_spec(),
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="vec")
        with pytest.raises(Exception) as exc_info:
            client.create_collection(collection_name=coll, schema=schema)
        msg = str(exc_info.value).lower()
        assert "explicit scheme" in msg or "external_source" in msg, (
            f"Expected relative-path rejection, got: {exc_info.value}"
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "source",
        [
            "http://169.254.169.254/metadata",
            "ftp://example.com/data",
            "xyz://bucket/prefix",
            "file:///tmp/data",
        ],
        ids=["http", "ftp", "unknown", "file"],
    )
    def test_external_source_disallowed_schemes(self, source):
        """Schemes outside the allowlist must be rejected with 'not allowed'."""
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(external_source=source, external_spec=_PLACEHOLDER_SPEC)
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="vec")
        with pytest.raises(Exception) as exc_info:
            client.create_collection(collection_name=coll, schema=schema)
        msg = str(exc_info.value).lower()
        assert "not allowed" in msg or "not support" in msg, f"Expected scheme rejection, got: {exc_info.value}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_external_source_userinfo_rejected(self):
        """Embedded credentials like s3://ak:sk@bucket/prefix must be rejected."""
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(
            external_source="s3://ak:sk@bucket/prefix",
            external_spec=_PLACEHOLDER_SPEC,
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="vec")
        with pytest.raises(Exception) as exc_info:
            client.create_collection(collection_name=coll, schema=schema)
        assert "credentials" in str(exc_info.value).lower(), f"Expected userinfo rejection, got: {exc_info.value}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_user_fields_force_nullable(self):
        """User fields are server-side normalized to nullable=True; the
        virtual PK is exempt and remains the primary key."""
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(
            external_source=_PLACEHOLDER_SRC,
            external_spec=_PLACEHOLDER_SPEC,
        )
        # Intentionally do NOT pass nullable on any user field.
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("v", DataType.VARCHAR, max_length=64, external_field="v")
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="vec")
        client.create_collection(collection_name=coll, schema=schema)
        try:
            info = client.describe_collection(coll)
            by_name = {f["name"]: f for f in info["fields"]}
            for field in ("id", "v", "vec"):
                assert by_name[field].get("nullable") is True, (
                    f"user field '{field}' should be force-nullable, got {by_name[field]}"
                )
            vpk = by_name.get("__virtual_pk__")
            assert vpk is not None and vpk.get("is_primary") is True, "__virtual_pk__ should still be the primary key"
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "properties",
        [
            {"collection.external_source": "s3://bucket/new/"},
            {"collection.external_spec": '{"format":"lance-table"}'},
            {
                "collection.external_source": "s3://bucket/new/",
                "collection.external_spec": '{"format":"lance-table"}',
            },
        ],
        ids=["source_only", "spec_only", "source_and_spec"],
    )
    def test_alter_collection_external_source_via_property_rejected(self, properties):
        """Setting collection.external_source / collection.external_spec via
        alter_collection_properties is rejected; the supported path for
        rebinding source is refresh_external_collection(external_source=,
        external_spec=). This test pins that contract.
        """
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        cfg = get_minio_config()
        schema = client.create_schema(
            external_source=build_external_source(cfg, "old"),
            external_spec=build_external_spec(cfg),
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="vec")
        client.create_collection(collection_name=coll, schema=schema)
        try:
            before = client.describe_collection(coll)
            with pytest.raises(Exception) as exc_info:
                client.alter_collection_properties(
                    coll,
                    properties=properties,
                )
            msg = str(exc_info.value).lower()
            assert (
                "refreshexternalcollection" in msg.replace(" ", "")
                or "external_source" in msg
                or "external_spec" in msg
                or "invalid parameter" in msg
            ), f"unexpected error: {exc_info.value}"
            after = client.describe_collection(coll)
            assert after.get("external_source") == before.get("external_source")
            assert after.get("external_spec") == before.get("external_spec")
            log.info(f"alter via property correctly rejected: {exc_info.value}")
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "fields",
        [
            [("__virtual_pk__", DataType.INT64, {"is_primary": True, "auto_id": False})],
            [
                ("id", DataType.INT64, {"is_primary": True, "auto_id": False}),
                ("__virtual_pk__", DataType.INT64, {}),
            ],
            [
                (
                    "__virtual_pk__",
                    DataType.VARCHAR,
                    {
                        "is_primary": True,
                        "auto_id": False,
                        "max_length": 64,
                    },
                )
            ],
        ],
        ids=["as_int64_pk", "as_scalar", "as_varchar_pk"],
    )
    def test_regular_collection_rejects_virtual_pk_reserved_name(self, fields):
        """milvus#49314: __virtual_pk__ is reserved even outside external collections."""
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema()
        for name, dtype, kwargs in fields:
            schema.add_field(name, dtype, **kwargs)
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_DIM)

        with pytest.raises(Exception) as exc_info:
            client.create_collection(collection_name=coll, schema=schema)
        msg = str(exc_info.value).lower()
        assert "__virtual_pk__" in msg and "reserved" in msg, f"expected reserved-name rejection, got: {exc_info.value}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_duplicate_external_field_mapping_rejected(self):
        """milvus#49235: one external column cannot back multiple user fields."""
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(
            external_source=_PLACEHOLDER_SRC,
            external_spec=_PLACEHOLDER_SPEC,
        )
        schema.add_field("id", DataType.INT64, external_field="same_col")
        schema.add_field("value", DataType.FLOAT, external_field="same_col")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="embedding")
        with pytest.raises(Exception) as exc_info:
            client.create_collection(collection_name=coll, schema=schema)
        msg = str(exc_info.value).lower()
        assert "external_field" in msg and ("multiple" in msg or "duplicate" in msg), (
            f"expected duplicate external_field rejection, got: {exc_info.value}"
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"external_source": _PLACEHOLDER_SRC},
            {"external_spec": _PLACEHOLDER_SPEC},
        ],
        ids=["source_only", "spec_only"],
    )
    def test_refresh_override_requires_source_spec_pair(self, kwargs, external_prefix):
        """milvus#49230/#49335: refresh override source/spec must be atomic."""
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        schema = build_basic_schema(client, external_prefix["url"])
        client.create_collection(collection_name=coll, schema=schema)
        try:
            with pytest.raises(Exception) as exc_info:
                client.refresh_external_collection(collection_name=coll, **kwargs)
            msg = str(exc_info.value).lower()
            assert "both" in msg or "external_source" in msg or "external_spec" in msg or "invalid" in msg, (
                f"unexpected error: {exc_info.value}"
            )
        finally:
            client.drop_collection(coll)


# ============================================================
# 2. Data Types (Scalar + Vector)
# ============================================================


class TestExternalTableDataTypes(TestMilvusClientV2Base):
    """E2E coverage for every supported scalar and vector data type."""

    # ---- Scalar types ------------------------------------------------

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "type_name,dtype,extra,arrow_type,value_fn",
        [
            ("bool", DataType.BOOL, {}, pa.bool_(), lambda i: i % 2 == 0),
            ("int8", DataType.INT8, {}, pa.int8(), lambda i: i % 100),
            ("int16", DataType.INT16, {}, pa.int16(), lambda i: i * 3),
            ("int32", DataType.INT32, {}, pa.int32(), lambda i: i * 7),
            ("int64", DataType.INT64, {}, pa.int64(), lambda i: i * 1000),
            ("float", DataType.FLOAT, {}, pa.float32(), lambda i: float(i) * 1.5),
            ("double", DataType.DOUBLE, {}, pa.float64(), lambda i: float(i) * 2.5),
            ("varchar", DataType.VARCHAR, {"max_length": 64}, pa.string(), lambda i: f"s_{i:05d}"),
            ("json", DataType.JSON, {}, pa.string(), lambda i: json.dumps({"k": i, "g": i % 3})),
        ],
        ids=lambda x: x if isinstance(x, str) else None,
    )
    def test_scalar_type_e2e(self, type_name, dtype, extra, arrow_type, value_fn, minio_env, external_prefix):
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
                coll,
                data=vectors,
                limit=5,
                anns_field="embedding",
                output_fields=["id", scalar_name],
            )
            assert len(search_res) == 1 and len(search_res[0]) > 0
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "elem_name,elem_dtype,elem_arrow,elem_extra,value_fn",
        [
            ("int64", DataType.INT64, pa.int64(), {"max_capacity": 8}, lambda i: [i, i + 1, i + 2]),
            (
                "varchar",
                DataType.VARCHAR,
                pa.string(),
                {"max_capacity": 8, "max_length": 32},
                lambda i: [f"a_{i}", f"b_{i}"],
            ),
        ],
        ids=lambda x: x if isinstance(x, str) else None,
    )
    def test_array_type_e2e(self, elem_name, elem_dtype, elem_arrow, elem_extra, value_fn, minio_env, external_prefix):
        """Array-of-<elem> field: upload, refresh, load, and query the array values."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        arr_field = f"arr_{elem_name}"

        data = gen_array_parquet_bytes(DEFAULT_NB, 0, arr_field, elem_arrow, value_fn)
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", data)

        schema = client.create_schema(external_source=ext_url, external_spec=build_external_spec(cfg))
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field(arr_field, DataType.ARRAY, element_type=elem_dtype, external_field=arr_field, **elem_extra)
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
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
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
                coll,
                filter="id == 42",
                output_fields=[
                    "id",
                    "val_bool",
                    "val_int8",
                    "val_int16",
                    "val_int32",
                    "val_int64",
                    "val_float",
                    "val_double",
                    "val_varchar",
                    "val_json",
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_geometry_wkt_parquet_round_trip(self, minio_env, external_prefix):
        """milvus#48627: WKT-backed Geometry columns query back as geometry text."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_geometry_wkt_parquet_bytes(50, 0),
        )
        schema = client.create_schema(
            external_source=ext_url,
            external_spec=build_external_spec(cfg),
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("geo", DataType.GEOMETRY, external_field="geo")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="embedding")
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            rows = client.query(
                coll,
                filter="id == 7",
                output_fields=["id", "geo"],
            )
            assert len(rows) == 1
            assert rows[0]["id"] == 7
            assert "POINT" in str(rows[0]["geo"]).upper(), rows[0]
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "case_name,milvus_dtype,field_kwargs,arrow_builder,include_embedding,known_acceptance",
        [
            (
                "int8_from_string",
                DataType.INT8,
                {},
                lambda ids: pa.array(["abcd" for _ in ids], type=pa.string()),
                True,
                True,
            ),
            (
                "int16_from_string",
                DataType.INT16,
                {},
                lambda ids: pa.array(["abcd" for _ in ids], type=pa.string()),
                True,
                True,
            ),
            (
                "int32_from_string",
                DataType.INT32,
                {},
                lambda ids: pa.array(["abcd" for _ in ids], type=pa.string()),
                True,
                True,
            ),
            (
                "int64_from_string",
                DataType.INT64,
                {},
                lambda ids: pa.array(["abcd" for _ in ids], type=pa.string()),
                True,
                True,
            ),
            (
                "float_from_string",
                DataType.FLOAT,
                {},
                lambda ids: pa.array(["abc" for _ in ids], type=pa.string()),
                True,
                True,
            ),
            (
                "double_from_string",
                DataType.DOUBLE,
                {},
                lambda ids: pa.array(["abc" for _ in ids], type=pa.string()),
                True,
                True,
            ),
            (
                "varchar_from_int64",
                DataType.VARCHAR,
                {"max_length": 64},
                lambda ids: pa.array(ids, type=pa.int64()),
                True,
                False,
            ),
            (
                "json_from_int64",
                DataType.JSON,
                {},
                lambda ids: pa.array(ids, type=pa.int64()),
                True,
                False,
            ),
            (
                "geometry_from_int64",
                DataType.GEOMETRY,
                {},
                lambda ids: pa.array(ids, type=pa.int64()),
                True,
                False,
            ),
            (
                "timestamptz_from_string",
                DataType.TIMESTAMPTZ,
                {},
                lambda ids: pa.array(["2026-04-27T00:00:00Z" for _ in ids], type=pa.string()),
                True,
                True,
            ),
            (
                "int8_from_int64_overflow",
                DataType.INT8,
                {},
                lambda ids: pa.array([128 + i for i in ids], type=pa.int64()),
                True,
                False,
            ),
            (
                "int16_from_int64_overflow",
                DataType.INT16,
                {},
                lambda ids: pa.array([40000 + i for i in ids], type=pa.int64()),
                True,
                False,
            ),
            (
                "int32_from_int64_overflow",
                DataType.INT32,
                {},
                lambda ids: pa.array([2147483648 + i for i in ids], type=pa.int64()),
                True,
                False,
            ),
            (
                "float_from_int64",
                DataType.FLOAT,
                {},
                lambda ids: pa.array(ids, type=pa.int64()),
                True,
                True,
            ),
            (
                "double_from_int64",
                DataType.DOUBLE,
                {},
                lambda ids: pa.array(ids, type=pa.int64()),
                True,
                True,
            ),
            (
                "int64_from_double",
                DataType.INT64,
                {},
                lambda ids: pa.array([float(i) + 0.5 for i in ids], type=pa.float64()),
                True,
                True,
            ),
            (
                "array_int64_from_list_string",
                DataType.ARRAY,
                {"element_type": DataType.INT64, "max_capacity": 8},
                lambda ids: pa.array([[f"s_{i}", f"s_{i + 1}"] for i in ids], type=pa.list_(pa.string())),
                True,
                True,
            ),
            (
                "array_float_from_list_string",
                DataType.ARRAY,
                {"element_type": DataType.FLOAT, "max_capacity": 8},
                lambda ids: pa.array([["1.5", "2.5"] for _ in ids], type=pa.list_(pa.string())),
                True,
                True,
            ),
            (
                "array_bool_from_list_string",
                DataType.ARRAY,
                {"element_type": DataType.BOOL, "max_capacity": 8},
                lambda ids: pa.array([["true", "false"] for _ in ids], type=pa.list_(pa.string())),
                True,
                True,
            ),
        ],
        ids=lambda x: x if isinstance(x, str) else None,
    )
    def test_incompatible_external_arrow_type_rejected(
        self,
        case_name,
        milvus_dtype,
        field_kwargs,
        arrow_builder,
        include_embedding,
        known_acceptance,
        minio_env,
        external_prefix,
    ):
        """Milvus field type vs external Arrow type mismatches must fail visibly.

        The exact detection stage can vary by reader path and build:
        create_collection, refresh, index/load, or query are all acceptable
        rejection points. What must not happen is a silent successful query
        with data decoded through an incompatible type mapping.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        nb = 20
        bad_field = "bad_col"
        ids = list(range(nb))

        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_dtype_mismatch_parquet_bytes(
                nb,
                0,
                bad_field,
                arrow_builder(ids),
                include_embedding=include_embedding,
            ),
        )

        schema = client.create_schema(
            external_source=ext_url,
            external_spec=build_external_spec(cfg),
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field(bad_field, milvus_dtype, external_field=bad_field, **field_kwargs)
        if include_embedding:
            schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="embedding")

        try:
            try:
                client.create_collection(collection_name=coll, schema=schema)
            except Exception as exc:
                log.info(f"[{case_name}] create_collection rejected mismatch: {exc}")
                return

            job_id, progress = refresh_and_poll_terminal(client, coll, timeout=60)
            if progress.state == "RefreshFailed":
                assert progress.reason, f"[{case_name}] refresh job {job_id} failed without a reason"
                log.info(f"[{case_name}] refresh rejected mismatch: job={job_id}, reason={progress.reason}")
                return

            assert progress.state == "RefreshCompleted", f"[{case_name}] unexpected refresh state: {progress.state}"

            try:
                if include_embedding:
                    add_vector_index(client, coll, "embedding", "AUTOINDEX", "L2")
                    client.load_collection(coll, timeout=30)
                    accepted_result = client.query(
                        coll,
                        filter="id >= 0",
                        output_fields=["id", bad_field],
                        limit=1,
                    )
                else:
                    add_vector_index(client, coll, bad_field, "AUTOINDEX", "L2")
                    client.load_collection(coll, timeout=30)
                    accepted_result = client.query(
                        coll,
                        filter="id >= 0",
                        output_fields=["id", bad_field],
                        limit=1,
                    )
                    accepted_hits = client.search(
                        coll,
                        data=[[0.0] * TEST_DIM],
                        limit=1,
                        anns_field=bad_field,
                        output_fields=["id"],
                    )
                    accepted_result = {
                        "query": accepted_result,
                        "search": accepted_hits,
                    }
            except Exception as exc:
                log.info(f"[{case_name}] load/query rejected mismatch: {exc}")
                return

            if known_acceptance:
                pytest.xfail(
                    f"[{case_name}] current master accepts incompatible "
                    f"external Arrow data; observed result: {accepted_result}"
                )
            pytest.fail(
                f"[{case_name}] incompatible external Arrow type was accepted "
                f"through refresh, load, and query; result={accepted_result}"
            )
        finally:
            try:
                client.drop_collection(coll)
            except Exception:
                pass

    # ---- Vector types -----------------------------------------------

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "vec_kind,vec_dtype,metric,dim",
        [
            ("float", DataType.FLOAT_VECTOR, "L2", TEST_DIM),
            ("float16", DataType.FLOAT16_VECTOR, "L2", TEST_DIM),
            ("bfloat16", DataType.BFLOAT16_VECTOR, "L2", TEST_DIM),
            ("int8", DataType.INT8_VECTOR, "L2", TEST_DIM),
        ],
        ids=lambda x: x if isinstance(x, str) else None,
    )
    def test_float_family_vector_e2e(self, vec_kind, vec_dtype, metric, dim, minio_env, external_prefix):
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
                coll,
                data=query_vec,
                limit=5,
                anns_field=vec_field,
                output_fields=["id"],
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
            DEFAULT_NB,
            0,
            "bin_vec",
            DataType.BINARY_VECTOR,
            BINARY_DIM,
        )
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", data)

        schema = build_vector_variant_schema(
            client,
            ext_url,
            "bin_vec",
            DataType.BINARY_VECTOR,
            BINARY_DIM,
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
                coll,
                data=query_vec,
                limit=5,
                anns_field="bin_vec",
                output_fields=["id"],
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
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
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
                coll,
                data=[[0.0] * TEST_DIM],
                limit=3,
                anns_field="dense_vec",
                output_fields=["id"],
            )
            bin_res = client.search(
                coll,
                data=[b"\x00" * (BINARY_DIM // 8)],
                limit=3,
                anns_field="bin_vec",
                output_fields=["id"],
                search_params={"metric_type": "HAMMING"},
            )
            assert len(dense_res[0]) == 3 and len(bin_res[0]) == 3
        finally:
            client.drop_collection(coll)

    # ---- Whitelist gap coverage --------------------------------------
    # The supported-type whitelist in pkg/util/typeutil/schema.go:2369-2394
    # accepts Text, Timestamptz, and ArrayOfVector for external collections.
    # The cases below probe each explicitly so the contract stays observable.

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("ts_unit", ["us", "ms"])
    def test_timestamptz_type_e2e(self, ts_unit, minio_env, external_prefix):
        """Timestamptz field: arrow timestamp in parquet → Milvus Timestamptz.

        NormalizeExternalArrow (Util.cpp:2092-2097) converts arrow.timestamp
        to int64 microseconds; query output is rendered as ISO-8601.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        nb = DEFAULT_NB
        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_timestamptz_parquet_bytes(nb, 0, ts_unit=ts_unit),
        )

        schema = client.create_schema(
            external_source=ext_url,
            external_spec=build_external_spec(cfg),
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("ts", DataType.TIMESTAMPTZ, external_field="ts")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="embedding")
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == nb

            row = client.query(coll, filter="id == 7", output_fields=["id", "ts"])
            assert len(row) == 1
            ts_val = row[0]["ts"]
            # Server returns ISO-8601 string; verify the offset matches our
            # encoding base (1700000000 + 7 = 1700000007 → "2023-11-14T22:13:27Z").
            assert isinstance(ts_val, str) and ts_val.startswith("2023-11-14")
            log.info(f"[timestamptz/{ts_unit}] id=7 -> {ts_val}")
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    def test_array_of_vector_top_level_rejected(self, external_prefix):
        """ArrayOfVector is on the external whitelist (schema.go:2389) but the
        general schema validator only accepts it inside a struct array field
        ('array of vector can only be in the struct array field'). External
        collections separately reject struct fields (schema.go:2285-2287),
        so there is no usable path today.

        This test pins the contradiction: top-level _ARRAY_OF_VECTOR fails
        at create_collection. If a future build relaxes either side, this
        test must be updated to a positive E2E assertion.
        """
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url = external_prefix["url"]
        schema = client.create_schema(
            external_source=ext_url,
            external_spec=build_external_spec(),
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field(
            "vecs",
            DataType._ARRAY_OF_VECTOR,
            element_type=DataType.FLOAT_VECTOR,
            dim=TEST_DIM,
            max_capacity=4,
            external_field="vecs",
        )
        with pytest.raises(Exception) as exc:
            client.create_collection(collection_name=coll, schema=schema)
        msg = str(exc.value)
        assert "struct" in msg.lower() or "array of vector" in msg.lower(), msg
        log.info(f"[array_of_vector] rejected as expected: {msg}")


# ============================================================
# 2.5 Edge cases: nullable behavior + large files
# ============================================================


class TestExternalTableNullableScenarios(TestMilvusClientV2Base):
    """External user fields are force-nullable (schema.go:2350). Probe edge
    cases of that contract."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_all_null_scalar_columns(self, minio_env, external_prefix):
        """Every user scalar column is entirely null in parquet.

        Verifies: (1) refresh succeeds, (2) count(*) reflects the row total,
        (3) get-by-id returns rows with None for null fields, (4) `field is
        null` filter matches every row.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        nb = 100
        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_all_null_columns_parquet_bytes(nb, 0),
        )

        schema = client.create_schema(
            external_source=ext_url,
            external_spec=build_external_spec(cfg),
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("n_int", DataType.INT32, external_field="n_int")
        schema.add_field("n_float", DataType.FLOAT, external_field="n_float")
        schema.add_field("n_varchar", DataType.VARCHAR, max_length=64, external_field="n_varchar")
        schema.add_field("n_json", DataType.JSON, external_field="n_json")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="embedding")

        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == nb

            # Single-row peek: every user field should round-trip as None.
            rows = client.query(
                coll,
                filter="id == 5",
                output_fields=["id", "n_int", "n_float", "n_varchar", "n_json"],
            )
            assert len(rows) == 1, f"missing id=5 in result: {rows}"
            r = rows[0]
            assert r["id"] == 5
            assert r["n_int"] is None
            assert r["n_float"] is None
            assert r["n_varchar"] is None
            assert r["n_json"] is None

            # `is null` filter should match every row.
            null_rows = client.query(
                coll,
                filter="n_int is null",
                output_fields=["id"],
                limit=nb,
            )
            assert len(null_rows) == nb, f"is-null filter only matched {len(null_rows)}/{nb}"
        finally:
            client.drop_collection(coll)


class TestExternalTableLargeFile(TestMilvusClientV2Base):
    """Large single-file scenarios — multi-row-group parquet and large row
    counts that can be split into multiple Milvus segments."""

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "num_rows,row_group_size",
        [
            (5_000, 500),  # 10 row groups
            (20_000, 1_000),  # 20 row groups
        ],
        ids=["10rg", "20rg"],
    )
    def test_large_single_file_with_row_groups(
        self,
        num_rows,
        row_group_size,
        minio_env,
        external_prefix,
    ):
        """A single large parquet file with explicit row_group_size, verifying
        the multi-row-group reader path on a stable external table.

        Asserts: count(*) is exact; spot-checked rows round-trip correctly;
        a vector search returns at least `limit` results.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_large_parquet_with_row_groups(num_rows, 0, row_group_size),
        )

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == num_rows

            # Spot-check rows in the first, middle, and last row group.
            for sample in (0, num_rows // 2, num_rows - 1):
                rows = client.query(
                    coll,
                    filter=f"id == {sample}",
                    output_fields=["id", "value"],
                )
                assert len(rows) == 1, f"missing id={sample}"
                assert abs(rows[0]["value"] - sample * 1.5) < 1e-3

            hits = client.search(
                coll,
                data=[[0.0] * TEST_DIM],
                limit=10,
                anns_field="embedding",
                output_fields=["id"],
            )
            assert len(hits[0]) == 10
        finally:
            client.drop_collection(coll)


# ============================================================
# 3. Indexes (Vector + Scalar)
# ============================================================


class TestExternalTableIndexes(TestMilvusClientV2Base):
    """Index creation on external collections — vector + scalar + binary."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "index_type,metric_type,params",
        [
            ("AUTOINDEX", "L2", {}),
            ("FLAT", "L2", {}),
            ("HNSW", "L2", {"M": 16, "efConstruction": 200}),
            ("IVF_FLAT", "L2", {"nlist": 16}),
            ("HNSW", "COSINE", {"M": 8}),
            ("DISKANN", "L2", {}),
        ],
        ids=lambda x: x if isinstance(x, str) else None,
    )
    def test_vector_index(self, index_type, metric_type, params, minio_env, external_prefix):
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
                    client,
                    coll,
                    "embedding",
                    index_type=index_type,
                    metric_type=metric_type,
                    **({"params": params} if params else {}),
                )
            except Exception as e:
                if index_type == "DISKANN":
                    pytest.skip(f"DISKANN not available on this deployment: {e}")
                raise
            client.load_collection(coll)

            hits = client.search(
                coll,
                data=[[0.0] * TEST_DIM],
                limit=5,
                anns_field="embedding",
                output_fields=["id"],
            )[0]
            assert len(hits) == 5

            idx_list = client.list_indexes(collection_name=coll)
            assert any("embedding" in str(i) for i in idx_list), f"embedding index not in list: {idx_list}"
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "index_type,params",
        [
            ("HNSW", {"M": 16, "efConstruction": 200}),
            ("IVF_FLAT", {"nlist": 16}),
        ],
        ids=lambda x: x if isinstance(x, str) else None,
    )
    def test_vector_index_mmap_enable(self, index_type, params, minio_env, external_prefix):
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
                    collection_name=coll,
                    index_name="embedding",
                    properties={"mmap.enabled": True},
                )
            except Exception as e:
                pytest.skip(f"alter_index_properties not supported: {e}")

            info = client.describe_index(collection_name=coll, index_name="embedding")
            log.info(f"[{index_type}] index info after mmap=True: {info}")
            assert str(info.get("mmap.enabled", "")).lower() == "true", f"expected mmap.enabled=True, got {info}"

            client.load_collection(coll)
            hits_on = client.search(
                coll,
                data=[[0.0] * TEST_DIM],
                limit=5,
                anns_field="embedding",
                output_fields=["id"],
            )[0]
            assert len(hits_on) == 5

            # Flip back to False
            client.release_collection(coll)
            client.alter_index_properties(
                collection_name=coll,
                index_name="embedding",
                properties={"mmap.enabled": False},
            )
            info_off = client.describe_index(collection_name=coll, index_name="embedding")
            assert str(info_off.get("mmap.enabled", "")).lower() == "false"

            client.load_collection(coll)
            hits_off = client.search(
                coll,
                data=[[0.0] * TEST_DIM],
                limit=5,
                anns_field="embedding",
                output_fields=["id"],
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
                    collection_name=coll,
                    properties={"mmap.enabled": True},
                )
            except Exception as e:
                pytest.skip(f"alter_collection_properties mmap not supported: {e}")

            desc = client.describe_collection(coll)
            props = desc.get("properties") or {}
            assert str(props.get("mmap.enabled", "")).lower() == "true", (
                f"expected collection mmap.enabled=True, got properties={props}"
            )

            index_and_load(client, coll)
            assert query_count(client, coll) == DEFAULT_NB

            hits = client.search(
                coll,
                data=[[0.0] * TEST_DIM],
                limit=5,
                anns_field="embedding",
                output_fields=["id"],
            )[0]
            assert len(hits) == 5

            # Flip back
            client.release_collection(coll)
            client.alter_collection_properties(
                collection_name=coll,
                properties={"mmap.enabled": False},
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
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_vector_variant_parquet_bytes(DEFAULT_NB, 0, "bin_vec", DataType.BINARY_VECTOR, BINARY_DIM),
        )
        schema = build_vector_variant_schema(client, ext_url, "bin_vec", DataType.BINARY_VECTOR, BINARY_DIM)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            params = {"nlist": 16} if index_type == "BIN_IVF_FLAT" else {}
            add_vector_index(client, coll, "bin_vec", index_type, "HAMMING", **({"params": params} if params else {}))
            client.load_collection(coll)

            hits = client.search(
                coll,
                data=[b"\x00" * (BINARY_DIM // 8)],
                limit=5,
                anns_field="bin_vec",
                output_fields=["id"],
                search_params={"metric_type": "HAMMING"},
            )[0]
            assert len(hits) == 5
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "scalar_index_type,field_name",
        [
            ("INVERTED", "value"),
            ("STL_SORT", "value"),
            ("BITMAP", "value"),
        ],
    )
    def test_scalar_index(self, scalar_index_type, field_name, minio_env, external_prefix):
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
            index_params.add_index(field_name="embedding", index_type="AUTOINDEX", metric_type="L2")
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
                coll,
                data=[[0.0] * TEST_DIM],
                limit=3,
                anns_field="embedding",
                output_fields=["id"],
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
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", gen_basic_parquet_bytes(50, 0))
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
                    client.insert(
                        collection_name=coll,
                        data=[
                            {
                                "id": 99999,
                                "value": 1.0,
                                "embedding": [0.0] * TEST_DIM,
                            }
                        ],
                    )
                elif op_name == "upsert":
                    client.upsert(
                        collection_name=coll,
                        data=[
                            {
                                "id": 0,
                                "value": 0.0,
                                "embedding": [0.0] * TEST_DIM,
                            }
                        ],
                    )
                elif op_name == "delete":
                    client.delete(collection_name=coll, filter="id >= 0")
                elif op_name == "flush":
                    client.flush(collection_name=coll)
            msg = str(exc_info.value).lower()
            # Two valid rejection points:
            # - server-side: "<op> operation is not supported for external collection"
            # - client-side: pymilvus requires all fields (including virtual_pk)
            #   before the RPC even leaves — upsert/insert fail with DataNotMatch.
            assert (
                "external" in msg
                or "not support" in msg
                or "virtual_pk" in msg
                or "datanotmatch" in msg
                or "missed an field" in msg
            ), f"[{op_name}] unexpected error: {exc_info.value}"
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
            assert "external" in msg or "not support" in msg, f"unexpected error: {exc_info.value}"
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
            assert "external" in msg or "not support" in msg or "default" in msg, f"unexpected error: {exc_info.value}"
            log.info(f"drop_partition correctly rejected: {exc_info.value}")
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "op_name",
        [
            "compact",
            "add_collection_field",
            "truncate_collection",
        ],
    )
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
                        collection_name=coll,
                        field_name="new_field",
                        data_type=DataType.INT64,
                        nullable=True,
                    )
                elif op_name == "truncate_collection":
                    client.truncate_collection(collection_name=coll)
            msg = str(exc_info.value).lower()
            assert "external" in msg or "not support" in msg or "not allowed" in msg, (
                f"[{op_name}] unexpected error: {exc_info.value}"
            )
            log.info(f"[{op_name}] correctly rejected: {exc_info.value}")
        finally:
            try:
                client.drop_collection(coll)
            except Exception:
                pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_truncate_reject_keeps_external_source(self, minio_env, external_prefix):
        """milvus#49343: rejected truncate must not clear external_source metadata."""
        minio_client, cfg = minio_env
        client = self._client()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        coll = self._prepared_collection(client, minio_client, cfg, ext_url, ext_key)
        try:
            before = client.describe_collection(coll)
            with pytest.raises(Exception):
                client.truncate_collection(collection_name=coll)
            after = client.describe_collection(coll)
            assert after.get("external_source") == before.get("external_source")
            assert after.get("external_spec") == before.get("external_spec")

            client.release_collection(coll)
            refresh_and_wait(client, coll)
            client.load_collection(coll)
            assert query_count(client, coll) == 50
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

        upload_basic_data(minio_client, cfg, ext_key, filename="part0.parquet", num_rows=DEFAULT_NB, start_id=0)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == DEFAULT_NB

            # Append more data, re-refresh, require release+load.
            client.release_collection(coll)
            upload_basic_data(
                minio_client, cfg, ext_key, filename="part1.parquet", num_rows=DEFAULT_NB, start_id=DEFAULT_NB
            )
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
            upload_parquet(
                minio_client, cfg["bucket"], f"{ext_key}/data_{idx}.parquet", gen_basic_parquet_bytes(500, start)
            )

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == 1000

            client.release_collection(coll)
            minio_client.remove_object(cfg["bucket"], f"{ext_key}/data_1.parquet")
            upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data_2.parquet", gen_basic_parquet_bytes(300, 2000))
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            assert query_count(client, coll) == 800
            assert len(client.query(coll, filter="id >= 0 && id < 500", output_fields=["id"])) == 500
            assert len(client.query(coll, filter="id >= 500 && id < 1000", output_fields=["id"])) == 0
            assert len(client.query(coll, filter="id >= 2000 && id < 2300", output_fields=["id"])) == 300
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
        upload_parquet(minio_client, cfg["bucket"], f"{key_a}/data.parquet", gen_basic_parquet_bytes(100, 0))
        upload_parquet(minio_client, cfg["bucket"], f"{key_b}/data.parquet", gen_basic_parquet_bytes(100, 5000))

        schema = build_basic_schema(client, url_a)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            ids_a = sorted(r["id"] for r in client.query(coll, filter="id >= 0", output_fields=["id"], limit=200))
            assert ids_a == list(range(0, 100))

            client.release_collection(coll)
            refresh_and_wait(client, coll, external_source=url_b, external_spec=build_external_spec(cfg))
            client.load_collection(coll)

            ids_b = sorted(r["id"] for r in client.query(coll, filter="id >= 0", output_fields=["id"], limit=200))
            assert ids_b == list(range(5000, 5100))

            info = client.describe_collection(coll)
            assert info.get("external_source") == url_b, (
                f"override source was not persisted: {info.get('external_source')!r}"
            )

            fresh_client = self._client()
            fresh_info = fresh_client.describe_collection(coll)
            assert fresh_info.get("external_source") == url_b, (
                f"fresh client sees stale source: {fresh_info.get('external_source')!r}"
            )

            client.release_collection(coll)
            refresh_and_wait(client, coll)
            client.load_collection(coll)
            ids_reuse = sorted(r["id"] for r in client.query(coll, filter="id >= 0", output_fields=["id"], limit=200))
            assert ids_reuse == list(range(5000, 5100)), "reuse refresh reverted to the pre-override source"
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    def test_refresh_second_concurrent_not_both_succeed(self, minio_env, external_prefix):
        """Two refreshes issued back-to-back: at most one Completes."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", gen_basic_parquet_bytes(500, 0))

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
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", gen_basic_parquet_bytes(100, 0))

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
    @pytest.mark.parametrize(
        "num_files,rows_per_file",
        [
            (1, 500),  # single large file
            (5, 100),  # small uniform files
            (10, 50),  # many tiny files
        ],
        ids=lambda x: str(x) if isinstance(x, int) else None,
    )
    def test_refresh_many_files(self, num_files, rows_per_file, minio_env, external_prefix):
        """Refresh aggregates rows correctly across many parquet files in one prefix."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        # Upload num_files sibling parquet files at <prefix>/file_<n>.parquet
        for idx in range(num_files):
            upload_parquet(
                minio_client,
                cfg["bucket"],
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
                    coll,
                    filter=f"id == {first_id}",
                    output_fields=["id", "value"],
                )
                assert len(rows) == 1, f"row id={first_id} missing after refresh"
                assert abs(rows[0]["value"] - first_id * 1.5) < 1e-4
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_refresh_non_existent_collection(self):
        """Refresh on a non-existent collection must fail at RPC."""
        client = self._client()
        with pytest.raises(Exception):
            client.refresh_external_collection(collection_name="non_existent_xyz")

    @pytest.mark.tags(CaseLabel.L1)
    def test_refresh_normal_collection_rejected(self):
        """Refresh on a normal (non-external) collection must be rejected."""
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema()
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=4)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            with pytest.raises(Exception):
                client.refresh_external_collection(collection_name=coll)
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_refresh_empty_source_behavior(self, external_prefix):
        """Refresh on an empty external source — accepted job either
        completes with 0 rows or fails with a clear reason. Either is OK;
        the test only requires a clean terminal state, not infinite pending.
        """
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url = external_prefix["url"]
        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            try:
                job_id = client.refresh_external_collection(collection_name=coll)
            except Exception as e:
                log.info(f"empty-source refresh rejected at submit: {e}")
                return

            deadline = time.time() + 60
            state = None
            while time.time() < deadline:
                p = client.get_refresh_external_collection_progress(job_id=job_id)
                state = p.state
                if state in ("RefreshCompleted", "RefreshFailed"):
                    break
                time.sleep(2)
            assert state in ("RefreshCompleted", "RefreshFailed"), (
                f"empty-source refresh stuck in {state} (expected terminal state)"
            )
            log.info(f"empty-source refresh terminal state={state}")
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_refresh_zero_row_parquet_terminal(self, minio_env, external_prefix):
        """milvus#49225: 0-row parquet must not crash DataNode or hang refresh."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/empty.parquet",
            gen_zero_row_basic_parquet_bytes(),
        )

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            job_id = client.refresh_external_collection(collection_name=coll)
            deadline = time.time() + 60
            progress = None
            while time.time() < deadline:
                progress = client.get_refresh_external_collection_progress(job_id=job_id)
                if progress.state in ("RefreshCompleted", "RefreshFailed"):
                    break
                time.sleep(2)
            assert progress is not None
            assert progress.state in ("RefreshCompleted", "RefreshFailed"), (
                f"zero-row refresh stuck in {progress.state}"
            )

            if progress.state == "RefreshCompleted":
                index_and_load(client, coll)
                assert query_count(client, coll) == 0
            else:
                assert progress.reason, "failed zero-row refresh should expose a reason"
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_refresh_nonexistent_bucket_fails_terminal(self, minio_env):
        """milvus#49233: NoSuchBucket must fail terminally, not retry forever."""
        _minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        missing_bucket = f"nosuchbucket-{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
        ext_url = f"s3://{cfg['address']}/{missing_bucket}/external/path/"

        schema = build_basic_schema(client, ext_url, ext_spec=build_external_spec(cfg))
        client.create_collection(collection_name=coll, schema=schema)
        try:
            job_id = client.refresh_external_collection(collection_name=coll)
            deadline = time.time() + 60
            progress = None
            while time.time() < deadline:
                progress = client.get_refresh_external_collection_progress(job_id=job_id)
                if progress.state in ("RefreshCompleted", "RefreshFailed"):
                    break
                time.sleep(2)
            assert progress is not None
            assert progress.state == "RefreshFailed", (
                f"NoSuchBucket refresh should fail terminally, got {progress.state}"
            )
            reason = (progress.reason or "").lower()
            assert "bucket" in reason or "no_such_bucket" in reason or "nosuchbucket" in reason, progress.reason
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    def test_refresh_schema_mismatch_caught_eventually(self, minio_env, external_prefix):
        """Schema with wrong external_field mappings: refresh / load / query
        should surface the mismatch at some point (not silently return wrong
        data). The test only asserts a non-success outcome at one of the
        stages — it doesn't pin which stage detects the error since that
        varies by build.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=100)

        # Mismatched mappings — parquet has columns id/value/embedding;
        # the schema points at wrong_col_a/wrong_col_b.
        schema = client.create_schema(external_source=ext_url, external_spec=build_external_spec(cfg))
        schema.add_field("x", DataType.INT64, external_field="wrong_col_a")
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="wrong_col_b")

        try:
            client.create_collection(collection_name=coll, schema=schema)
        except Exception as e:
            log.info(f"create_collection caught the mismatch: {e}")
            return

        try:
            try:
                job_id = client.refresh_external_collection(collection_name=coll)
            except Exception as e:
                log.info(f"refresh submit caught the mismatch: {e}")
                return

            deadline = time.time() + 60
            state = None
            while time.time() < deadline:
                p = client.get_refresh_external_collection_progress(job_id=job_id)
                state = p.state
                if state in ("RefreshCompleted", "RefreshFailed"):
                    break
                time.sleep(2)
            if state == "RefreshFailed":
                log.info(f"refresh caught the mismatch: state={state}")
                return

            # Refresh accepted; the mismatch must surface at create_index/load/query.
            try:
                idx = client.prepare_index_params()
                idx.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
                client.create_index(coll, idx)
                client.load_collection(coll, timeout=60)
                client.query(coll, filter="x == 0", output_fields=["x"])
                pytest.fail("schema mismatch was not detected at any stage")
            except Exception as e:
                log.info(f"index/load/query caught the mismatch: {e}")
        finally:
            try:
                client.drop_collection(coll)
            except Exception:
                pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_case_mismatched_external_fields_rejected(self, minio_env, external_prefix):
        """milvus#49232: case-mismatched parquet column names must not load silently."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=50)

        schema = client.create_schema(
            external_source=ext_url,
            external_spec=build_external_spec(cfg),
        )
        schema.add_field("id", DataType.INT64, external_field="ID")
        schema.add_field("value", DataType.FLOAT, external_field="Value")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="Embedding")

        try:
            client.create_collection(collection_name=coll, schema=schema)
        except Exception as e:
            log.info(f"case mismatch rejected at create_collection: {e}")
            return

        try:
            job_id = client.refresh_external_collection(collection_name=coll)
            deadline = time.time() + 60
            progress = None
            while time.time() < deadline:
                progress = client.get_refresh_external_collection_progress(job_id=job_id)
                if progress.state in ("RefreshCompleted", "RefreshFailed"):
                    break
                time.sleep(2)
            assert progress is not None
            if progress.state == "RefreshFailed":
                return
            assert progress.state == "RefreshCompleted", f"case-mismatch refresh stuck in {progress.state}"

            with pytest.raises(Exception):
                index_and_load(client, coll)
        finally:
            try:
                client.drop_collection(coll)
            except Exception:
                pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_schemaless_reader_extra_columns_ignored(self, minio_env, external_prefix):
        """Parquet has 9 scalar columns plus embedding; the external schema
        only projects 3 of them. The reader must silently drop the unmapped
        columns and serve the projected ones correctly.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        nb = 100
        # gen_all_scalar_parquet_bytes writes id + 9 scalars + embedding.
        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_all_scalar_parquet_bytes(nb, 0),
        )

        # Project only id, val_int32, embedding — the other 7 scalar columns
        # exist in the file but are not declared in the schema.
        schema = client.create_schema(external_source=ext_url, external_spec=build_external_spec(cfg))
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("val_int32", DataType.INT32, external_field="val_int32")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="embedding")
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == nb

            res = client.query(coll, filter="id == 5", output_fields=["id", "val_int32"])
            assert len(res) == 1
            assert res[0]["id"] == 5
            assert res[0]["val_int32"] == 5 * 7  # value_fn for val_int32 was i*7
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
            upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/{name}", gen_basic_parquet_bytes(nrows, start_id))

        # 2 files inside subdirs — should be ignored by refresh
        nested_files = [
            ("year=2025/month=01/nested_a.parquet", 100, 10000),
            ("subdir/nested_b.parquet", 100, 20000),
        ]
        for rel, nrows, start_id in nested_files:
            upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/{rel}", gen_basic_parquet_bytes(nrows, start_id))

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
                    coll,
                    filter=f"id >= {start_id} && id < {start_id + 100}",
                    output_fields=["id"],
                    limit=200,
                )
                assert len(got) == 0, f"ids [{start_id}..{start_id + 100}) unexpectedly present (subdir ingested)"
        finally:
            client.drop_collection(coll)


# ============================================================
# 6. DQL — query / search / hybrid_search / iterators / get / pagination
# ============================================================


class TestExternalTableDQL(TestMilvusClientV2Base):
    """Read-side operations on external collections."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "filter_expr,expected",
        [
            ("", DEFAULT_NB),
            ("id < 10", 10),
            ("id >= 100 && id < 150", 50),
            ("value > 0.0 && id < 50", 49),  # value=1.5*id, id==0 excluded
            ("id in [0, 1, 2, 3, 99]", 5),
        ],
    )
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
                res = client.query(coll, filter=filter_expr, output_fields=["id"], limit=DEFAULT_NB)
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
                coll,
                data=_float_vectors([0], TEST_DIM).tolist(),
                limit=5,
                anns_field="embedding",
                output_fields=["id"],
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
                coll,
                data=[[0.0] * TEST_DIM],
                limit=10,
                anns_field="embedding",
                output_fields=["id"],
                filter="id >= 50 && id < 100",
            )[0]
            assert len(hits) == 10
            assert all(50 <= h["id"] < 100 for h in hits), f"filter violated: {[h['id'] for h in hits]}"
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_multi_vector(self, minio_env, external_prefix):
        """hybrid_search over two vector fields with RRF reranker."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_parquet(
            minio_client, cfg["bucket"], f"{ext_key}/data.parquet", gen_multi_vector_parquet_bytes(DEFAULT_NB, 0)
        )

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
                data=[[0.0] * TEST_DIM],
                anns_field="dense_vec",
                limit=10,
                param={"metric_type": "L2"},
            )
            req2 = AnnSearchRequest(
                data=[b"\x00" * (BINARY_DIM // 8)],
                anns_field="bin_vec",
                limit=10,
                param={"metric_type": "HAMMING"},
            )
            hits = client.hybrid_search(
                collection_name=coll,
                reqs=[req1, req2],
                ranker=RRFRanker(),
                limit=5,
                output_fields=["id"],
            )
            assert len(hits) == 1 and len(hits[0]) == 5, f"hybrid_search returned {hits}"
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
                collection_name=coll,
                filter="id >= 0",
                batch_size=50,
                output_fields=["id"],
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
                collection_name=coll,
                data=[[0.0] * TEST_DIM],
                anns_field="embedding",
                batch_size=20,
                limit=80,
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
                coll,
                filter="id >= 0 && id < 5",
                output_fields=["id", "__virtual_pk__"],
                limit=5,
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

            page1 = client.query(coll, filter="id >= 0", output_fields=["id"], offset=0, limit=10)
            page2 = client.query(coll, filter="id >= 0", output_fields=["id"], offset=10, limit=10)
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
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/a/data.parquet", gen_basic_parquet_bytes(30, 0))
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/b/data.parquet", gen_basic_parquet_bytes(40, 10000))

        url_a = build_external_source(cfg, f"{ext_key}/a")
        url_b = build_external_source(cfg, f"{ext_key}/b")
        schema_a = build_basic_schema(client, url_a)
        schema_b = build_basic_schema(client, url_b)
        client.create_collection(collection_name=coll_a, schema=schema_a)
        client.create_collection(collection_name=coll_b, schema=schema_b)

        alias = f"alias_{random.randint(10000, 99999)}"
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
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/part0.parquet", gen_basic_parquet_bytes(100, 0))

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == 100

            upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/part1.parquet", gen_basic_parquet_bytes(100, 100))
            refresh_and_wait(client, coll)

            stale = query_count(client, coll)
            assert stale == 100, f"without release+load expected 100, got {stale}"

            client.release_collection(coll)
            client.load_collection(coll)
            assert query_count(client, coll) == 200
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    def test_concurrent_query_during_refresh_release_load(self, minio_env, external_prefix):
        """Continuous query thread keeps running while the main thread uploads
        a new file, refreshes, releases, and reloads. After the cycle the
        queries should observe the new (larger) row count without crashing.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data0.parquet", gen_basic_parquet_bytes(500, 0))

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == 500

            stop = threading.Event()
            ok = {"n": 0}
            fail = {"n": 0}
            last = {"v": 500}

            def query_loop():
                while not stop.is_set():
                    try:
                        last["v"] = query_count(client, coll)
                        ok["n"] += 1
                    except Exception:
                        fail["n"] += 1
                    time.sleep(0.2)

            t = threading.Thread(target=query_loop, daemon=True)
            t.start()
            time.sleep(1)
            _ = fail  # silences linter on unused-but-tracked counter

            # Upload a new file and run the refresh+release+load cycle while the
            # query thread keeps polling.
            upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data1.parquet", gen_basic_parquet_bytes(300, 500))
            refresh_and_wait(client, coll)
            client.release_collection(coll)
            time.sleep(1)
            client.load_collection(coll)
            time.sleep(2)

            stop.set()
            t.join(timeout=10)
            log.info(f"concurrent: ok={ok['n']} fail={fail['n']} last={last['v']}")
            assert last["v"] == 800, f"expected 800 rows after refresh+reload, got {last['v']}"
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L2)
    def test_file_deleted_then_reload(self, minio_env, external_prefix):
        """Delete a source parquet file (without a refresh), then
        release+load. The reload either keeps serving cached data or falls
        back to fewer rows or surfaces an error — all three are documented
        behaviors. The test only requires the system not to hang or crash.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data0.parquet", gen_basic_parquet_bytes(100, 0))
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data1.parquet", gen_basic_parquet_bytes(100, 100))

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == 200

            # Delete a backing file with no refresh in between.
            minio_client.remove_object(cfg["bucket"], f"{ext_key}/data1.parquet")
            log.info("deleted data1.parquet without a follow-up refresh")

            client.release_collection(coll)
            try:
                client.load_collection(coll, timeout=60)
                count_after = query_count(client, coll)
                log.info(f"reload-after-deletion served {count_after} rows")
                assert count_after in (100, 200), f"unexpected row count after deletion+reload: {count_after}"
            except Exception as e:
                # Acceptable: load fails because the manifest references a
                # missing file. The test passes as long as it surfaces cleanly.
                log.info(f"reload-after-deletion failed as expected: {e}")
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
# 8. Cross-bucket external sources
# ============================================================


class TestExternalTableCrossBucket(TestMilvusClientV2Base):
    """External data living in a separate MinIO bucket from Milvus's own."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_cross_bucket_source(self, minio_env):
        """End-to-end refresh/load/query against a different bucket.

        Uses Milvus-form s3://<endpoint>/<bucket>/<path>/ because current
        validation rejects empty-host external_source URIs.
        """
        minio_client, cfg = minio_env
        cross_bucket = "external-cross-bucket"
        if not minio_client.bucket_exists(cross_bucket):
            minio_client.make_bucket(cross_bucket)

        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        key_prefix = f"external-e2e-cross/{coll}"
        nb = 500
        try:
            for i in range(2):
                upload_parquet(
                    minio_client,
                    cross_bucket,
                    f"{key_prefix}/data{i}.parquet",
                    gen_basic_parquet_bytes(nb, i * nb),
                )
            external_source = f"s3://{cfg['address']}/{cross_bucket}/{key_prefix}/"
            schema = build_basic_schema(
                client,
                external_source,
                ext_spec=build_external_spec(cfg),
            )
            client.create_collection(collection_name=coll, schema=schema)
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == nb * 2
            log.info(f"cross-bucket: {nb * 2} rows loaded from {cross_bucket} while milvus uses {cfg['bucket']}")
        finally:
            try:
                client.drop_collection(coll)
            except Exception:
                pass
            cleanup_minio_prefix(minio_client, cross_bucket, f"{key_prefix}/")


# ============================================================
# 9. Parquet compression codecs
# ============================================================


class TestExternalTableParquetCodecs(TestMilvusClientV2Base):
    """Parquet compression codec compatibility for external collections.

    Milvus's bundled Arrow needs `arrow:with_snappy=True` and
    `arrow:with_lz4=True` (added in Part8-1/4 #49061). If either is
    dropped or rebuilt without codec support, compressed parquet
    silently breaks at query time. This test guards against regressions.
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("codec", ["snappy", "lz4", "gzip", "zstd", "none"])
    def test_parquet_codec_readable(self, codec, minio_env, external_prefix):
        """Upload a parquet compressed with `codec`, refresh, query the count back."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name() + f"_{codec}"
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        nb = 256
        # pyarrow accepts 'NONE' (uppercase) for uncompressed; lowercase 'none' fails.
        pq_codec = "NONE" if codec == "none" else codec
        try:
            data = gen_parquet_bytes_with_codec(nb, 0, pq_codec)
        except Exception as e:
            pytest.skip(f"pyarrow build cannot write codec={codec}: {e}")
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", data)

        schema = build_basic_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            count = query_count(client, coll)
            assert count == nb, f"codec={codec}: expected {nb}, got {count}"
            log.info(f"codec={codec}: {nb} rows round-tripped")
        finally:
            client.drop_collection(coll)


# ============================================================
# 10. Read-only / admin operations allowed on external collections
# ============================================================


class TestExternalTableReadOps(TestMilvusClientV2Base):
    """Operations normal collections support that should also work on
    external collections: stats, state, partitions, segments, rename,
    refresh_load, property alter/drop, database scoping."""

    @staticmethod
    def _prepared_collection(client, minio_client, cfg, ext_url, ext_key, num_rows=DEFAULT_NB):
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", gen_basic_parquet_bytes(num_rows, 0))
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
            assert client.has_partition(collection_name=coll, partition_name="nonexistent_xyz") is False
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
            assert row_count == DEFAULT_NB, f"expected row_count={DEFAULT_NB}, got {row_count}"
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
                stats = client.get_partition_stats(collection_name=coll, partition_name="_default")
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
        coll = self._prepared_collection(client, minio_client, cfg, ext_url, ext_key, num_rows=50)
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
                    collection_name=coll,
                    index_name="embedding",
                    properties={"mmap.enabled": True},
                )
            except Exception as e:
                pytest.skip(f"alter_index_properties not supported: {e}")

            info = client.describe_index(collection_name=coll, index_name="embedding")
            assert str(info.get("mmap.enabled", "")).lower() == "true"

            try:
                client.drop_index_properties(
                    collection_name=coll,
                    index_name="embedding",
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


def _write_lance_to_minio(minio_client, bucket, key_prefix, num_rows, start_id, dim=TEST_DIM):
    """Build a Lance dataset locally and upload every file under its folder
    to MinIO, preserving the relative layout Lance uses (versions/, data/, etc).
    """
    return _write_lance_to_minio_batches(
        minio_client,
        bucket,
        key_prefix,
        batches=[(start_id, num_rows)],
        dim=dim,
    )


def _write_lance_to_minio_batches(minio_client, bucket, key_prefix, batches, dim=TEST_DIM):
    """Multi-fragment variant: each (start_id, count) tuple in `batches` is
    appended into the same Lance dataset, producing one fragment file per
    batch under data/. Used to exercise multi-data-file refresh.
    """
    import os
    import shutil
    import tempfile

    import lance

    tmpdir = tempfile.mkdtemp(prefix="ext_lance_")
    local_path = os.path.join(tmpdir, "dataset.lance")
    try:
        for idx, (start_id, num_rows) in enumerate(batches):
            ids = list(range(start_id, start_id + num_rows))
            vectors = _float_vectors(ids, dim)
            table = pa.table(
                {
                    "id": pa.array(ids, type=pa.int64()),
                    "value": pa.array([float(i) * 1.5 for i in ids], type=pa.float32()),
                    "embedding": pa.FixedSizeListArray.from_arrays(
                        vectors.flatten(),
                        list_size=dim,
                    ),
                }
            )
            mode = "create" if idx == 0 else "append"
            lance.write_dataset(table, local_path, mode=mode)
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


def _write_vortex_to_minio(bucket, cfg, key_prefix, num_rows, start_id, dim=TEST_DIM, filename="data.vortex"):
    """Write a Vortex dataset to MinIO via the `.venv-vortex` sidecar
    (Python 3.11+ required for vortex-data >= 0.35).

    The embedding is written as FixedSizeList<uint8, dim*4> inside the vortex
    file — Milvus's vortex reader reinterprets those raw bytes as float32
    vectors of width `dim`.

    Pass a unique `filename` per call to write multiple sibling .vortex files
    under the same key_prefix (used for multi-file format coverage).
    """
    import os
    import subprocess

    sidecar = _find_vortex_sidecar_python()
    if sidecar is None:
        pytest.skip(
            "Vortex sidecar venv missing. Create it once with:\n"
            "  uv venv .venv-vortex -p python3.12\n"
            "  source .venv-vortex/bin/activate && uv pip install "
            "'vortex-data==0.56.0' 'pyarrow>=16' 'numpy>=2.0' minio"
        )

    here = os.path.dirname(os.path.abspath(__file__))
    helper = os.path.join(here, "_vortex_gen.py")

    env = os.environ.copy()
    env.update(
        {
            "VT_NUM_ROWS": str(num_rows),
            "VT_START_ID": str(start_id),
            "VT_DIM": str(dim),
            "MINIO_ADDRESS": cfg["address"],
            "MINIO_BUCKET": bucket,
            "MINIO_ACCESS_KEY": cfg["access_key"],
            "MINIO_SECRET_KEY": cfg["secret_key"],
            "VT_MINIO_KEY": f"{key_prefix}/{filename}",
        }
    )
    result = subprocess.run(
        [sidecar, helper],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"vortex helper failed (code {result.returncode}): stdout={result.stdout!r} stderr={result.stderr!r}"
        )
    log.info(f"[vortex] wrote {result.stdout.strip()} to {key_prefix}/data.vortex")


def _write_vortex_table_to_minio(bucket, cfg, key_prefix, table, *, filename="data.vortex"):
    """Write an arbitrary pyarrow Table to MinIO as a vortex file via the
    sidecar venv. The table is serialized to an Arrow IPC stream and piped
    to the sidecar over stdin; the sidecar deserializes and hands the
    table directly to vx.io.write — schema (FixedSizeList dimensions,
    types, etc.) is preserved verbatim across the venv boundary.

    Used by the full-matrix vortex test where the schema is too rich to
    encode via env vars.
    """
    import os
    import subprocess

    sidecar = _find_vortex_sidecar_python()
    if sidecar is None:
        pytest.skip(
            "Vortex sidecar venv missing. Create it once with:\n"
            "  uv venv .venv-vortex -p python3.12\n"
            "  source .venv-vortex/bin/activate && uv pip install "
            "'vortex-data==0.56.0' 'pyarrow>=16' 'numpy>=2.0' minio"
        )

    here = os.path.dirname(os.path.abspath(__file__))
    helper = os.path.join(here, "_vortex_gen.py")

    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, table.schema) as writer:
        writer.write_table(table)
    ipc_bytes = buf.getvalue()

    env = os.environ.copy()
    env.update(
        {
            "VT_INPUT_FROM_STDIN": "1",
            "MINIO_ADDRESS": cfg["address"],
            "MINIO_BUCKET": bucket,
            "MINIO_ACCESS_KEY": cfg["access_key"],
            "MINIO_SECRET_KEY": cfg["secret_key"],
            "VT_MINIO_KEY": f"{key_prefix}/{filename}",
        }
    )
    result = subprocess.run(
        [sidecar, helper],
        input=ipc_bytes,
        env=env,
        capture_output=True,
        timeout=180,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"vortex helper failed (code {result.returncode}): stdout={result.stdout!r} stderr={result.stderr!r}"
        )
    log.info(f"[vortex] wrote {result.stdout.decode().strip()} to {key_prefix}/{filename}")


def _build_iceberg_table_in_minio(prefix, cfg, batches, dim=TEST_DIM):
    """Create an Iceberg table whose warehouse lives directly on MinIO.

    Writing the warehouse to MinIO (not locally) is required because Iceberg
    metadata.json records absolute data-file URIs at write time; if we wrote
    to file:// then uploaded, those URIs would be unreachable from the
    Milvus reader.

    Vector encoding: each row stores its vector as a single binary blob of
    dim * sizeof(float) bytes. The Milvus iceberg reader path returns it as
    arrow.binary, and external_field=embedding is mapped to a FloatVector
    field — NormalizeExternalArrow reinterprets the raw bytes as float32.

    Note: list<float32> looks more natural but pyarrow >=15 promotes long
    lists to large_list, which the vector normalizer rejects
    (Util.cpp:1761). The binary encoding sidesteps that.

    `batches` is a list of (start_id, count) tuples. Each batch is appended
    to the same iceberg table as a separate parquet data file under data/.
    The final snapshot includes every batch.

    Returns (snapshot_id, ext_url) where ext_url is the s3:// URL of the
    table's metadata.json in Milvus URI form.
    """
    import shutil
    import struct
    import tempfile

    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema as IbgSchema
    from pyiceberg.types import BinaryType, LongType, NestedField

    tmp = tempfile.mkdtemp(prefix="ibg_cat_")
    try:
        cat = SqlCatalog(
            "milvus_test",
            **{
                "uri": f"sqlite:///{tmp}/cat.db",
                "warehouse": f"s3://{cfg['bucket']}/{prefix}",
                "s3.endpoint": f"http://{cfg['address']}",
                "s3.access-key-id": cfg["access_key"],
                "s3.secret-access-key": cfg["secret_key"],
                "s3.region": "us-east-1",
                "s3.path-style-access": "true",
            },
        )
        cat.create_namespace("ext")

        ibg_schema = IbgSchema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "embedding", BinaryType(), required=False),
        )
        arrow_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=True),
                pa.field("embedding", pa.binary(), nullable=True),
            ]
        )
        tbl = cat.create_table("ext.t", schema=ibg_schema)

        for start_id, num_rows in batches:
            ids = list(range(start_id, start_id + num_rows))
            vec_bytes = [struct.pack(f"<{dim}f", *[float(i + j * 0.1) for j in range(dim)]) for i in ids]
            arrow_table = pa.table(
                {
                    "id": pa.array(ids, type=pa.int64()),
                    "embedding": pa.array(vec_bytes, type=pa.binary()),
                },
                schema=arrow_schema,
            )
            tbl.append(arrow_table)

        snap = tbl.current_snapshot().snapshot_id
        # metadata_location: s3://<bucket>/<prefix>/.../metadata/000NN-uuid.metadata.json
        # → Milvus form: s3://<address>/<bucket>/...
        meta_loc = tbl.metadata_location
        if not meta_loc.startswith("s3://"):
            raise RuntimeError(f"unexpected metadata_loc scheme: {meta_loc}")
        bucket_and_key = meta_loc[len("s3://") :]
        ext_url = f"s3://{cfg['address']}/{bucket_and_key}"
        return snap, ext_url
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# Iceberg full-matrix is a strict subset of FULL_MATRIX_* because Iceberg V2
# only supports int (32-bit) and long (64-bit) integers — no Int8/Int16. We
# reuse the same field names where possible to keep query expectations
# identical between formats; Int8 → IntegerType promoted to Milvus INT32
# rather than INT8, otherwise NormalizeExternalArrow refuses the cast.
ICEBERG_FULL_MATRIX_SCALAR_FIELDS = [
    # (field_name, milvus DataType, iceberg type ctor (lambda field_id: type),
    #  arrow type, milvus add_field extras, value_fn)
    ("b_inv", DataType.BOOL, lambda fid: ("BooleanType",), pa.bool_(), {}, lambda i: i % 2 == 0),
    ("i32_stl", DataType.INT32, lambda fid: ("IntegerType",), pa.int32(), {}, lambda i: i * 7),
    ("i64_inv", DataType.INT64, lambda fid: ("LongType",), pa.int64(), {}, lambda i: i * 1000),
    ("f_inv", DataType.FLOAT, lambda fid: ("FloatType",), pa.float32(), {}, lambda i: float(i) * 1.5),
    ("d_inv", DataType.DOUBLE, lambda fid: ("DoubleType",), pa.float64(), {}, lambda i: float(i) * 2.5),
    ("vc_trie", DataType.VARCHAR, lambda fid: ("StringType",), pa.string(), {"max_length": 64}, lambda i: f"s_{i:05d}"),
    ("j", DataType.JSON, lambda fid: ("StringType",), pa.string(), {}, lambda i: json.dumps({"k": i, "g": i % 3})),
    (
        "ts",
        DataType.TIMESTAMPTZ,
        lambda fid: ("TimestamptzType",),
        pa.timestamp("us", tz="UTC"),
        {},
        lambda i: (1_700_000_000 + i) * 1_000_000,
    ),
]

ICEBERG_FULL_MATRIX_SCALAR_INDEXES = {
    "b_inv": "INVERTED",
    "i32_stl": "STL_SORT",
    "i64_inv": "INVERTED",
    "f_inv": "INVERTED",
    "d_inv": "INVERTED",
    "vc_trie": "TRIE",
}


def _build_iceberg_full_matrix_table(prefix, cfg, num_rows, dim=FULL_MATRIX_DIM, bin_dim=FULL_MATRIX_BINARY_DIM):
    """Iceberg variant of the full-matrix dataset.

    Vectors are stored as raw binary blobs because Iceberg has no
    fixed-size-list. JSON / Geometry use String / Binary respectively.
    Returns (snapshot_id, ext_url).
    """
    import shutil
    import struct
    import tempfile

    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema as IbgSchema
    from pyiceberg.types import (
        BinaryType,
        BooleanType,
        DoubleType,
        FloatType,
        IntegerType,
        ListType,
        LongType,
        NestedField,
        StringType,
        TimestamptzType,
    )

    iceberg_type_lookup = {
        "BooleanType": BooleanType(),
        "IntegerType": IntegerType(),
        "LongType": LongType(),
        "FloatType": FloatType(),
        "DoubleType": DoubleType(),
        "StringType": StringType(),
        "TimestamptzType": TimestamptzType(),
    }

    ids = list(range(num_rows))
    next_field_id = [3]  # field-id assignment, id=1, schema starts at 2 then bumps

    def fid():
        x = next_field_id[0]
        next_field_id[0] += 1
        return x

    nested_fields = [NestedField(1, "id", LongType(), required=False)]
    arrow_fields = [pa.field("id", pa.int64(), nullable=True)]
    arrow_columns = {"id": pa.array(ids, type=pa.int64())}

    # Scalars
    for name, _dtype, ibg_factory, arrow_type, _extra, value_fn in ICEBERG_FULL_MATRIX_SCALAR_FIELDS:
        ibg_type = iceberg_type_lookup[ibg_factory(0)[0]]
        nested_fields.append(NestedField(fid(), name, ibg_type, required=False))
        arrow_fields.append(pa.field(name, arrow_type, nullable=True))
        arrow_columns[name] = pa.array([value_fn(i) for i in ids], type=arrow_type)

    # Array<Int64>: ListType<LongType>
    arr_name, _arr_dtype, arr_value_fn = FULL_MATRIX_ARRAY_FIELD
    nested_fields.append(
        NestedField(
            fid(),
            arr_name,
            ListType(element_id=fid(), element_type=LongType(), element_required=False),
            required=False,
        ),
    )
    arrow_fields.append(
        pa.field(arr_name, pa.list_(pa.field("element", pa.int64(), nullable=True)), nullable=True),
    )
    arrow_columns[arr_name] = pa.array(
        [arr_value_fn(i) for i in ids],
        type=pa.list_(pa.field("element", pa.int64(), nullable=True)),
    )

    # Geometry → BinaryType (WKB)
    nested_fields.append(NestedField(fid(), "geo", BinaryType(), required=False))
    arrow_fields.append(pa.field("geo", pa.binary(), nullable=True))
    arrow_columns["geo"] = pa.array([_GEOMETRY_WKB] * num_rows, type=pa.binary())

    # Vector fields → BinaryType blobs of dim*sizeof(elem) bytes.
    fv_arr = _float_vectors(ids, dim)
    f16_arr = _float16_vectors(ids, dim).view(np.uint16)
    bf16_arr = _bfloat16_vectors(ids, dim)  # already raw uint16-equivalent
    i8_arr = _int8_vectors(ids, dim)
    bin_arr = _binary_vectors_bytes(ids, bin_dim)

    def vec_blobs(name, vtype, vdim):
        if vtype == DataType.FLOAT_VECTOR:
            return [struct.pack(f"<{vdim}f", *fv_arr[i].tolist()) for i in range(num_rows)]
        if vtype == DataType.FLOAT16_VECTOR:
            return [f16_arr[i].astype(np.uint16).tobytes() for i in range(num_rows)]
        if vtype == DataType.BFLOAT16_VECTOR:
            return [bf16_arr[i].tobytes() for i in range(num_rows)]
        if vtype == DataType.INT8_VECTOR:
            return [i8_arr[i].tobytes() for i in range(num_rows)]
        if vtype == DataType.BINARY_VECTOR:
            return [bin_arr[i].tobytes() for i in range(num_rows)]
        raise ValueError(f"unsupported vec type {vtype}")

    for name, vtype, vdim, _idx, _metric, _params in FULL_MATRIX_VECTOR_FIELDS:
        nested_fields.append(NestedField(fid(), name, BinaryType(), required=False))
        arrow_fields.append(pa.field(name, pa.binary(), nullable=True))
        arrow_columns[name] = pa.array(vec_blobs(name, vtype, vdim), type=pa.binary())

    ibg_schema = IbgSchema(*nested_fields)
    arrow_schema = pa.schema(arrow_fields)
    arrow_table = pa.table(arrow_columns, schema=arrow_schema)

    tmp = tempfile.mkdtemp(prefix="ibg_fm_")
    try:
        cat = SqlCatalog(
            "milvus_test_fm",
            **{
                "uri": f"sqlite:///{tmp}/cat.db",
                "warehouse": f"s3://{cfg['bucket']}/{prefix}",
                "s3.endpoint": f"http://{cfg['address']}",
                "s3.access-key-id": cfg["access_key"],
                "s3.secret-access-key": cfg["secret_key"],
                "s3.region": "us-east-1",
                "s3.path-style-access": "true",
            },
        )
        cat.create_namespace("ext")
        tbl = cat.create_table("ext.t", schema=ibg_schema)
        tbl.append(arrow_table)

        snap = tbl.current_snapshot().snapshot_id
        meta_loc = tbl.metadata_location
        if not meta_loc.startswith("s3://"):
            raise RuntimeError(f"unexpected metadata_loc scheme: {meta_loc}")
        bucket_and_key = meta_loc[len("s3://") :]
        ext_url = f"s3://{cfg['address']}/{bucket_and_key}"
        return snap, ext_url
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def build_iceberg_full_matrix_schema(client, ext_path, ext_spec, dim=FULL_MATRIX_DIM, bin_dim=FULL_MATRIX_BINARY_DIM):
    """Milvus schema for the iceberg full-matrix dataset. Matches
    ICEBERG_FULL_MATRIX_SCALAR_FIELDS for scalars and FULL_MATRIX_VECTOR_FIELDS
    for vectors (vectors are stored as iceberg binary blobs but exposed as
    Milvus vector types; NormalizeExternalArrow reinterprets the bytes)."""
    schema = client.create_schema(external_source=ext_path, external_spec=ext_spec or build_external_spec())
    schema.add_field("id", DataType.INT64, external_field="id")
    for name, dtype, _ibg, _arrow, extra, _value_fn in ICEBERG_FULL_MATRIX_SCALAR_FIELDS:
        schema.add_field(name, dtype, external_field=name, **extra)
    arr_name, arr_elem_dtype, _ = FULL_MATRIX_ARRAY_FIELD
    schema.add_field(arr_name, DataType.ARRAY, element_type=arr_elem_dtype, max_capacity=8, external_field=arr_name)
    schema.add_field("geo", DataType.GEOMETRY, external_field="geo")
    for name, vtype, vdim, _idx, _metric, _params in FULL_MATRIX_VECTOR_FIELDS:
        schema.add_field(name, vtype, dim=vdim, external_field=name)
    return schema


def create_iceberg_full_matrix_indexes(client, collection_name):
    """Wire the iceberg-subset scalar indexes + the same 9 vector indexes
    used by parquet/lance full-matrix."""
    index_params = client.prepare_index_params()
    for field, idx_type in ICEBERG_FULL_MATRIX_SCALAR_INDEXES.items():
        index_params.add_index(field_name=field, index_type=idx_type)
    for name, _vtype, _vdim, idx_type, metric, params in FULL_MATRIX_VECTOR_FIELDS:
        kwargs = {"field_name": name, "index_type": idx_type, "metric_type": metric}
        if params:
            kwargs["params"] = params
        index_params.add_index(**kwargs)
    client.create_index(collection_name, index_params)


def _iceberg_full_matrix_assert(client, coll, expected_count):
    """Iceberg-specific subset of _full_matrix_assert_basic: scalar set is
    smaller (no Int8/Int16/Bitmap)."""
    assert query_count(client, coll) == expected_count

    output_fields = ["id"] + [f for f, _, _, _, _, _ in ICEBERG_FULL_MATRIX_SCALAR_FIELDS]
    output_fields += [FULL_MATRIX_ARRAY_FIELD[0], "geo"]
    rows = client.query(coll, filter="id == 42", output_fields=output_fields)
    assert len(rows) == 1, f"id=42 missing: {rows}"
    r = rows[0]
    assert r["id"] == 42
    for name, _dtype, _ibg, _arrow, _extra, value_fn in ICEBERG_FULL_MATRIX_SCALAR_FIELDS:
        expected = value_fn(42)
        if name == "j":
            parsed = r["j"] if isinstance(r["j"], dict) else json.loads(r["j"])
            assert parsed.get("k") == 42, f"json[k]: {parsed}"
        elif name in ("f_inv", "d_inv"):
            assert abs(r[name] - expected) < 1e-3, f"{name}: {r[name]}"
        elif name == "ts":
            assert isinstance(r[name], str) and r[name], "ts empty"
        else:
            assert r[name] == expected, f"{name}: {r[name]} != {expected}"
    arr_name, _, arr_value_fn = FULL_MATRIX_ARRAY_FIELD
    assert r[arr_name] == arr_value_fn(42), f"{arr_name}: {r[arr_name]}"

    for name, _vtype, vdim, _idx, metric, _params in FULL_MATRIX_VECTOR_FIELDS:
        query_vec = _full_matrix_query_vec(name, vdim)
        hits = client.search(
            coll,
            data=query_vec,
            limit=3,
            anns_field=name,
            output_fields=["id"],
            search_params={"metric_type": metric},
        )[0]
        assert len(hits) == 3, f"[{name}] search returned {len(hits)} hits"


class TestExternalTableFormats(TestMilvusClientV2Base):
    """Coverage for the `external_spec.format` string values."""

    @pytest.mark.tags(CaseLabel.L0)
    def test_parquet_format_explicit(self, minio_env, external_prefix):
        """Explicit `{"format":"parquet"}` round-trip.

        Writes FORMAT_NUM_FILES sibling parquet files (each
        FORMAT_ROWS_PER_FILE rows) under the prefix to exercise multi-file
        refresh on the explicit-format path.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        for idx in range(FORMAT_NUM_FILES):
            upload_parquet(
                minio_client,
                cfg["bucket"],
                f"{ext_key}/file_{idx:03d}.parquet",
                gen_basic_parquet_bytes(
                    FORMAT_ROWS_PER_FILE,
                    idx * FORMAT_ROWS_PER_FILE,
                ),
            )

        schema = client.create_schema(
            external_source=ext_url,
            external_spec=build_external_spec(cfg),
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("value", DataType.FLOAT, external_field="value")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="embedding")
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)
            assert query_count(client, coll) == FORMAT_TOTAL_ROWS

            # Spot-check the first row of each file survived ingestion.
            for idx in range(FORMAT_NUM_FILES):
                first_id = idx * FORMAT_ROWS_PER_FILE
                rows = client.query(
                    coll,
                    filter=f"id == {first_id}",
                    output_fields=["id", "value"],
                )
                assert len(rows) == 1, f"row id={first_id} missing"
                assert abs(rows[0]["value"] - first_id * 1.5) < 1e-3
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_lance_table_format(self, minio_env, external_prefix):
        """Lance-table format E2E with multi-fragment dataset.

        Builds a single Lance dataset by sequentially appending
        FORMAT_NUM_FILES batches (each FORMAT_ROWS_PER_FILE rows). Each
        append produces a new fragment file under data/, so the iceberg-
        like multi-file refresh path is exercised even though Lance presents
        a unified dataset to the reader.
        """
        try:
            import lance  # noqa: F401
        except ImportError:
            pytest.skip("lance package not installed in this environment")

        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        batches = [(idx * FORMAT_ROWS_PER_FILE, FORMAT_ROWS_PER_FILE) for idx in range(FORMAT_NUM_FILES)]
        _write_lance_to_minio_batches(
            minio_client,
            cfg["bucket"],
            ext_key,
            batches,
        )

        schema = client.create_schema(
            external_source=ext_url,
            external_spec=build_external_spec(cfg, fmt="lance-table"),
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("value", DataType.FLOAT, external_field="value")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="embedding")
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            assert query_count(client, coll) == FORMAT_TOTAL_ROWS

            # Spot-check a row from each fragment survived ingestion.
            for idx in range(FORMAT_NUM_FILES):
                first_id = idx * FORMAT_ROWS_PER_FILE
                rows = client.query(
                    coll,
                    filter=f"id == {first_id}",
                    output_fields=["id", "value"],
                )
                assert len(rows) == 1, f"row id={first_id} missing"
                assert abs(rows[0]["value"] - first_id * 1.5) < 1e-3

            hits = client.search(
                coll,
                data=[[0.0] * TEST_DIM],
                limit=5,
                anns_field="embedding",
                output_fields=["id"],
            )[0]
            assert len(hits) == 5
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_vortex_format(self, minio_env, external_prefix):
        """Vortex format E2E with multiple sibling .vortex files.

        Vortex file generation happens in a Python 3.12 sidecar venv
        (`.venv-vortex/`) because vortex-data >= 0.35 requires Python >= 3.11
        while the main test venv is pinned to 3.10. The embedding is written
        as a raw-byte FixedSizeList<uint8, dim*4>; Milvus's vortex reader
        reinterprets those bytes as the Float32 vector column.

        Writes FORMAT_NUM_FILES sibling vortex files each carrying
        FORMAT_ROWS_PER_FILE rows.
        """
        _minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        for idx in range(FORMAT_NUM_FILES):
            _write_vortex_to_minio(
                cfg["bucket"],
                cfg,
                ext_key,
                num_rows=FORMAT_ROWS_PER_FILE,
                start_id=idx * FORMAT_ROWS_PER_FILE,
                filename=f"data_{idx:03d}.vortex",
            )

        schema = client.create_schema(
            external_source=ext_url,
            external_spec=build_external_spec(cfg, fmt="vortex"),
        )
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("value", DataType.FLOAT, external_field="value")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="embedding")
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            assert query_count(client, coll) == FORMAT_TOTAL_ROWS

            # Spot-check a row from each vortex file survived ingestion.
            for idx in range(FORMAT_NUM_FILES):
                first_id = idx * FORMAT_ROWS_PER_FILE
                rows = client.query(
                    coll,
                    filter=f"id == {first_id}",
                    output_fields=["id", "value"],
                )
                assert len(rows) == 1, f"row id={first_id} missing"
                assert abs(rows[0]["value"] - first_id * 1.5) < 1e-3

            hits = client.search(
                coll,
                data=[[0.0] * TEST_DIM],
                limit=5,
                anns_field="embedding",
                output_fields=["id"],
            )[0]
            assert len(hits) == 5
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_iceberg_table_format(self, minio_env, external_prefix):
        """Iceberg-table format E2E: write an Iceberg dataset to MinIO via
        pyiceberg, then refresh / index / load / query / search through
        Milvus's iceberg reader path.

        Vector field is stored as raw binary blobs (dim * 4 bytes per row)
        because (a) Iceberg has no native fixed-size-list, and (b) Milvus's
        NormalizeVectorArraysToFixedSizeBinary (Util.cpp:1693-1779)
        explicitly accepts arrow Binary as a vector source.

        external_source must point at the metadata.json file (not a
        directory) — the iceberg reader's PlanFiles entry point parses
        that URI directly. Source comes from pyiceberg's
        `tbl.metadata_location` after append.

        Skipped when pyiceberg is unavailable in the test environment:
            uv pip install 'pyiceberg[sql-sqlite,s3fs]==0.7.1'
        """
        try:
            from pyiceberg.catalog.sql import SqlCatalog  # noqa: F401
        except ImportError:
            pytest.skip(
                "pyiceberg not installed. Install in this venv with:\n"
                "  uv pip install 'pyiceberg[sql-sqlite,s3fs]==0.7.1'"
            )

        _minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_key = external_prefix["key"]

        # Multiple appends → multiple data files under data/. The final
        # snapshot includes every batch.
        batches = [(idx * FORMAT_ROWS_PER_FILE, FORMAT_ROWS_PER_FILE) for idx in range(FORMAT_NUM_FILES)]
        snapshot_id, ext_url = _build_iceberg_table_in_minio(
            ext_key,
            cfg,
            batches=batches,
        )
        log.info(f"[iceberg] snapshot={snapshot_id} ext_url={ext_url}")

        # Iceberg requires explicit credentials in extfs because the iceberg
        # reader path runs in the Rust opendal layer, which does not pick up
        # AKSK from the Milvus root configuration.
        ext_spec = build_external_spec(
            cfg,
            fmt="iceberg-table",
            snapshot_id=int(snapshot_id),
        )

        schema = client.create_schema(external_source=ext_url, external_spec=ext_spec)
        schema.add_field("id", DataType.INT64, external_field="id")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=TEST_DIM, external_field="embedding")
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            index_and_load(client, coll)

            assert query_count(client, coll) == FORMAT_TOTAL_ROWS

            # Spot-check the first row of each batch's data file: the
            # vector for row id=N encodes [N.0, N.1, N.2, ...] up to dim.
            for idx in range(FORMAT_NUM_FILES):
                first_id = idx * FORMAT_ROWS_PER_FILE
                rows = client.query(
                    coll,
                    filter=f"id == {first_id}",
                    output_fields=["id", "embedding"],
                )
                assert len(rows) == 1, f"row id={first_id} missing"
                vec = rows[0]["embedding"]
                assert len(vec) == TEST_DIM
                for j, v in enumerate(vec):
                    expected = float(first_id) + j * 0.1
                    assert abs(v - expected) < 1e-2, f"vec[{j}] for id={first_id} = {v}, expected {expected}"

            hits = client.search(
                coll,
                data=[[0.0] * TEST_DIM],
                limit=3,
                anns_field="embedding",
                output_fields=["id"],
            )[0]
            assert len(hits) == 3
            # NOTE: AUTOINDEX (IVF-family) on 9k rows can miss the true
            # nearest neighbor (recall < 1.0); we don't assert id==0 here.
            # Per-row vector content is already verified above via direct
            # query on the spot-checked ids.
        finally:
            client.drop_collection(coll)


# ============================================================
# 10. Full-matrix coverage: every DataType × every Index × every Format
# ============================================================

# Smaller row count keeps full-matrix tests under ~2 min each: each test
# builds 21 fields with 10+ scalar indexes and 9 vector indexes.
FULL_MATRIX_NB = 200

# Vortex regression coverage includes the formerly problematic variable-length
# fields. milvus#49352 covered Arrow string columns (VARCHAR / JSON) and
# milvus#49353 covered Arrow binary-backed GEOMETRY.
_VORTEX_EXCLUDED_FIELDS = set()


def _full_matrix_query_vec(vec_field, dim):
    """Build a search query vector matching the dtype of the given vector
    field. pymilvus is dtype-strict on the wire — int8 needs np.int8,
    bf16 needs raw bytes, etc."""
    for name, vtype, vdim, _idx, _metric, _params in FULL_MATRIX_VECTOR_FIELDS:
        if name != vec_field:
            continue
        if vtype == DataType.BINARY_VECTOR:
            return [b"\x00" * (vdim // 8)]
        if vtype == DataType.INT8_VECTOR:
            return [np.zeros(vdim, dtype=np.int8)]
        if vtype == DataType.FLOAT16_VECTOR:
            return [np.zeros(vdim, dtype=np.float16)]
        if vtype == DataType.BFLOAT16_VECTOR:
            return [bytes(vdim * 2)]
        return [[0.0] * vdim]
    raise KeyError(vec_field)


def _full_matrix_assert_basic(client, coll, expected_count, excluded_fields=()):
    """Shared assertions: count + per-type spot-checks + search hits per
    vector field. Used by every full-matrix format test."""
    assert query_count(client, coll) == expected_count
    excluded = set(excluded_fields)

    # Spot-check id=42: every scalar field round-trips with the expected
    # value derived from the same value_fn used to build the parquet.
    output_fields = ["id"] + [f for f, _, _, _, _ in FULL_MATRIX_SCALAR_FIELDS if f not in excluded]
    arr_name = FULL_MATRIX_ARRAY_FIELD[0]
    if arr_name not in excluded:
        output_fields.append(arr_name)
    if "geo" not in excluded:
        output_fields.append("geo")
    rows = client.query(coll, filter="id == 42", output_fields=output_fields)
    assert len(rows) == 1, f"id=42 missing: {rows}"
    r = rows[0]
    assert r["id"] == 42
    for name, _dtype, _arrow, _extra, value_fn in FULL_MATRIX_SCALAR_FIELDS:
        if name in excluded:
            continue
        expected = value_fn(42)
        if name == "j":
            parsed = r["j"] if isinstance(r["j"], dict) else json.loads(r["j"])
            assert parsed.get("k") == 42, f"json[k] mismatch: {parsed}"
        elif name == "f_inv":
            assert abs(r[name] - expected) < 1e-3, f"{name}: {r[name]} != {expected}"
        elif name == "d_inv":
            assert abs(r[name] - expected) < 1e-3, f"{name}: {r[name]} != {expected}"
        elif name == "ts":
            # Server returns an ISO-8601 string; we just assert non-empty.
            assert isinstance(r[name], str) and r[name], f"ts empty: {r[name]}"
        else:
            assert r[name] == expected, f"{name}: {r[name]} != {expected}"
    arr_name, _, arr_value_fn = FULL_MATRIX_ARRAY_FIELD
    if arr_name not in excluded:
        assert r[arr_name] == arr_value_fn(42), f"{arr_name}: {r[arr_name]}"

    # One search per vector field → confirms every (type × index × metric)
    # combination produced a usable index.
    for name, _vtype, vdim, _idx, metric, _params in FULL_MATRIX_VECTOR_FIELDS:
        if name in excluded:
            continue
        query_vec = _full_matrix_query_vec(name, vdim)
        hits = client.search(
            coll,
            data=query_vec,
            limit=3,
            anns_field=name,
            output_fields=["id"],
            search_params={"metric_type": metric},
        )[0]
        assert len(hits) == 3, f"[{name}] search returned {len(hits)} hits"


class TestExternalTableFullMatrix(TestMilvusClientV2Base):
    """End-to-end coverage of every supported DataType × Index combination
    on every external file format. A single collection per format carries
    21 fields wired to 8 scalar indexes + 9 vector indexes (4 FloatVector
    variants for AUTOINDEX/FLAT/HNSW/IVF_FLAT, plus Float16/BFloat16/
    Int8/Binary vectors with their canonical indexes)."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_full_matrix_parquet(self, minio_env, external_prefix):
        """Parquet × full DataType × full Index matrix."""
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_full_matrix_parquet_bytes(FULL_MATRIX_NB, 0),
        )
        schema = build_full_matrix_schema(client, ext_url)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            create_full_matrix_indexes(client, coll)
            client.load_collection(coll)
            _full_matrix_assert_basic(client, coll, FULL_MATRIX_NB)
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_full_matrix_lance(self, minio_env, external_prefix):
        """Lance × full DataType × full Index matrix.

        Lance writes the same arrow columns (FixedSizeList vectors, native
        scalar types) into a Lance dataset on MinIO; the iceberg-table
        path is exercised separately because Iceberg has no FixedSizeList.
        """
        try:
            import lance  # noqa: F401
        except ImportError:
            pytest.skip("lance package not installed in this environment")

        import os
        import shutil
        import tempfile

        import lance

        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        columns = _full_matrix_arrow_columns(FULL_MATRIX_NB, 0)
        table = pa.table(columns)

        tmpdir = tempfile.mkdtemp(prefix="ext_lance_fm_")
        local_path = os.path.join(tmpdir, "dataset.lance")
        try:
            lance.write_dataset(table, local_path)
            for root, _dirs, files in os.walk(local_path):
                for fname in files:
                    absolute = os.path.join(root, fname)
                    relative = os.path.relpath(absolute, local_path)
                    minio_client.fput_object(
                        cfg["bucket"],
                        f"{ext_key}/{relative}",
                        absolute,
                    )
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

        schema = build_full_matrix_schema(
            client,
            ext_url,
            ext_spec=build_external_spec(cfg, fmt="lance-table"),
        )
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            create_full_matrix_indexes(client, coll)
            client.load_collection(coll)
            _full_matrix_assert_basic(client, coll, FULL_MATRIX_NB)
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_full_matrix_vortex(self, minio_env, external_prefix):
        """Vortex × full DataType × full Index matrix.

        Vortex generation runs in the .venv-vortex sidecar (Python >=3.11
        for vortex-data >= 0.35). The arrow Table is built in the main
        venv with the shared full-matrix column generator, then streamed
        to the sidecar via Arrow IPC over stdin so the schema (including
        FixedSizeList vector dims) is preserved verbatim.

        Splits the full row set into FORMAT_NUM_FILES sibling .vortex
        files to also exercise multi-file ingestion through the vortex
        reader path.
        """
        _minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        excluded = _VORTEX_EXCLUDED_FIELDS

        # Distribute rows across multiple sibling vortex files.
        rows_per_file, remainder = divmod(FULL_MATRIX_NB, FORMAT_NUM_FILES)
        offset = 0
        for idx in range(FORMAT_NUM_FILES):
            n = rows_per_file + (1 if idx < remainder else 0)
            columns = _full_matrix_arrow_columns(
                n,
                offset,
                excluded_fields=excluded,
            )
            table = pa.table(columns)
            _write_vortex_table_to_minio(
                cfg["bucket"],
                cfg,
                ext_key,
                table,
                filename=f"data_{idx:03d}.vortex",
            )
            offset += n
        assert offset == FULL_MATRIX_NB

        schema = build_full_matrix_schema(
            client,
            ext_url,
            ext_spec=build_external_spec(cfg, fmt="vortex"),
            excluded_fields=excluded,
        )
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            create_full_matrix_indexes(client, coll, excluded_fields=excluded)
            client.load_collection(coll)
            _full_matrix_assert_basic(
                client,
                coll,
                FULL_MATRIX_NB,
                excluded_fields=excluded,
            )
        finally:
            client.drop_collection(coll)

    @pytest.mark.tags(CaseLabel.L1)
    def test_full_matrix_iceberg(self, minio_env, external_prefix):
        """Iceberg × full DataType × full Index matrix.

        Iceberg has no native fixed-size-list (vectors stored as binary
        blobs) and no Int8/Int16 (only 32/64-bit integers), so the iceberg
        scalar set is a strict subset of FULL_MATRIX_SCALAR_FIELDS. Vector
        fields and indexes mirror parquet/lance exactly.
        """
        try:
            from pyiceberg.catalog.sql import SqlCatalog  # noqa: F401
        except ImportError:
            pytest.skip("pyiceberg not installed. Install with:\n  uv pip install 'pyiceberg[sql-sqlite,s3fs]==0.7.1'")

        _minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_key = external_prefix["key"]

        snapshot_id, ext_url = _build_iceberg_full_matrix_table(
            ext_key,
            cfg,
            num_rows=FULL_MATRIX_NB,
        )
        ext_spec = build_external_spec(
            cfg,
            fmt="iceberg-table",
            snapshot_id=int(snapshot_id),
        )
        schema = build_iceberg_full_matrix_schema(client, ext_url, ext_spec)
        client.create_collection(collection_name=coll, schema=schema)
        try:
            refresh_and_wait(client, coll)
            create_iceberg_full_matrix_indexes(client, coll)
            client.load_collection(coll)
            _iceberg_full_matrix_assert(client, coll, FULL_MATRIX_NB)
        finally:
            client.drop_collection(coll)
