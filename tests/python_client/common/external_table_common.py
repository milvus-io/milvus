"""Shared helpers for MilvusClient external table tests."""

import io
import json
import os

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from minio import Minio
from pymilvus import DataType

from common import common_func as cf
from common import common_type as ct

# ============================================================
# Constants & Configuration
# ============================================================

REFRESH_TIMEOUT = 180

# Multi-file format coverage: each format E2E writes FORMAT_NUM_FILES sibling
# data files (or fragments / appended snapshots), each carrying
# FORMAT_ROWS_PER_FILE rows, exercising the multi-file refresh path.
FORMAT_NUM_FILES = 3
FORMAT_ROWS_PER_FILE = 3000
FORMAT_TOTAL_ROWS = FORMAT_NUM_FILES * FORMAT_ROWS_PER_FILE

SCHEME_PROVIDER_CASES = {"minio": "minio"}
ALLOWED_EXTERNAL_SOURCE_SCHEMES = (pytest.param("minio", id="minio"),)

FORMAT_CASES = {
    "parquet": {"id": "parquet", "has_value": True},
    "lance-table": {"id": "lance", "has_value": True},
    "iceberg-table": {"id": "iceberg", "has_value": True},
}
BASIC_FORMATS = tuple(FORMAT_CASES)
BASIC_FORMAT_IDS = [FORMAT_CASES[fmt]["id"] for fmt in BASIC_FORMATS]


def _minio_address(minio_host):
    address = minio_host or "localhost"
    if address.startswith("http://"):
        address = address.removeprefix("http://")
    elif address.startswith("https://"):
        address = address.removeprefix("https://")
    if ":" not in address:
        address = f"{address}:9000"
    return address


def get_minio_config(minio_host=None, minio_bucket=None):
    return {
        "address": _minio_address(minio_host),
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "bucket": minio_bucket or cf.param_info.param_bucket_name or "milvus-bucket",
        "secure": str(minio_host or "").startswith("https://"),
    }


def build_external_source(cfg, key_prefix):
    """Build a full s3:// URL that Milvus's extfs resolver can use directly."""
    # Trailing slash matters: refresh lists objects under the prefix.
    return f"s3://{cfg['address']}/{cfg['bucket']}/{key_prefix}/"


def build_validation_source_for_scheme(scheme, cfg, key_prefix="prefix"):
    """Build a create-time validation source for the CI allowlisted scheme."""
    if scheme == "minio":
        return f"minio://{cfg['address']}/{cfg['bucket']}/{key_prefix}/"
    raise ValueError(f"unexpected external source scheme for CI: {scheme}")


def minio_endpoint_url(cfg):
    scheme = "https" if cfg["secure"] else "http"
    return f"{scheme}://{cfg['address']}"


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


def assert_external_spec_persisted(actual_spec, expected_spec, context):
    actual = json.loads(actual_spec or "{}")
    expected = json.loads(expected_spec or "{}")
    assert actual.get("format") == expected.get("format"), f"{context}: format mismatch: {actual}"

    actual_extfs = actual.get("extfs") or {}
    expected_extfs = expected.get("extfs") or {}
    for key in ("cloud_provider", "region", "use_ssl"):
        assert actual_extfs.get(key) == expected_extfs.get(key), f"{context}: extfs.{key} mismatch: {actual_extfs}"

    for key in ("access_key_id", "access_key_value"):
        value = actual_extfs.get(key)
        assert value, f"{context}: extfs.{key} missing: {actual_extfs}"
        assert value == "***" or value == expected_extfs.get(key), f"{context}: extfs.{key} mismatch: {actual_extfs}"


def build_external_spec_for_scheme(scheme, cfg=None, fmt="parquet"):
    """Build a validation-only spec whose cloud_provider matches the URI scheme."""
    provider = SCHEME_PROVIDER_CASES[scheme]
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

_PARQUET_COMPRESSION = "snappy"


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


def _vector_byte_width(vec_dtype, dim):
    if vec_dtype == DataType.FLOAT_VECTOR:
        return dim * 4
    if vec_dtype in (DataType.FLOAT16_VECTOR, DataType.BFLOAT16_VECTOR):
        return dim * 2
    if vec_dtype == DataType.INT8_VECTOR:
        return dim
    if vec_dtype == DataType.BINARY_VECTOR:
        return dim // 8
    raise ValueError(f"unsupported vec dtype {vec_dtype}")


def _fixed_size_binary_vector_array(raw_rows, byte_width):
    return pa.array(
        [np.ascontiguousarray(row).tobytes() for row in raw_rows],
        type=pa.binary(byte_width),
    )


def gen_parquet_bytes(num_rows, start_id, scalar_name, arrow_type, value_fn, dim=ct.default_dim):
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


def gen_basic_parquet_bytes(num_rows, start_id, dim=ct.default_dim):
    """Generate a Parquet file with columns: id, value (float), embedding."""
    return gen_parquet_bytes(
        num_rows,
        start_id,
        "value",
        pa.float32(),
        lambda i: float(i) * 1.5,
        dim=dim,
    )


def gen_parquet_bytes_with_codec(num_rows, start_id, codec, dim=ct.default_dim):
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


def gen_array_parquet_bytes(num_rows, start_id, arr_name, arr_element_arrow_type, arr_value_fn, dim=ct.default_dim):
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
    num_rows, start_id, mismatch_name, mismatch_array, dim=ct.default_dim, include_embedding=True
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
        parquet_col = _fixed_size_binary_vector_array(
            _float16_vectors(ids, dim),
            _vector_byte_width(vec_dtype, dim),
        )
    elif vec_dtype == DataType.BFLOAT16_VECTOR:
        parquet_col = _fixed_size_binary_vector_array(
            _bfloat16_vectors(ids, dim),
            _vector_byte_width(vec_dtype, dim),
        )
    elif vec_dtype == DataType.INT8_VECTOR:
        arr = _int8_vectors(ids, dim)
        parquet_col = pa.FixedSizeListArray.from_arrays(
            pa.array(arr.flatten(), type=pa.int8()),
            list_size=dim,
        )
    elif vec_dtype == DataType.BINARY_VECTOR:
        parquet_col = _fixed_size_binary_vector_array(
            _binary_vectors_bytes(ids, dim),
            _vector_byte_width(vec_dtype, dim),
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


def gen_all_scalar_parquet_bytes(num_rows, start_id, dim=ct.default_dim):
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


def gen_multi_vector_parquet_bytes(num_rows, start_id, dim=ct.default_dim):
    """Parquet with two vector fields (float + binary) + id + scalar."""
    ids = list(range(start_id, start_id + num_rows))
    float_arr = _float_vectors(ids, dim)
    bin_arr = _binary_vectors_bytes(ids, dim)
    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array([float(i) for i in ids], type=pa.float32()),
            "dense_vec": pa.FixedSizeListArray.from_arrays(float_arr.flatten(), list_size=dim),
            "bin_vec": _fixed_size_binary_vector_array(
                bin_arr,
                _vector_byte_width(DataType.BINARY_VECTOR, dim),
            ),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=_PARQUET_COMPRESSION)
    return buf.getvalue()


def gen_timestamptz_parquet_bytes(num_rows, start_id, dim=ct.default_dim, ts_unit="us"):
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


def gen_geometry_wkt_parquet_bytes(num_rows, start_id, dim=ct.default_dim):
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


def gen_all_null_columns_parquet_bytes(num_rows, start_id, dim=ct.default_dim):
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


def gen_large_parquet_with_row_groups(num_rows, start_id, row_group_size, dim=ct.default_dim):
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
# Full-matrix schema (DataType x Index)
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

# A fixed POINT(0 0) WKB blob, sufficient to verify Geometry round-trip.
# WKB: byte order (1=little-endian) | uint32 type(1=POINT) | float64 x | float64 y
_GEOMETRY_WKB = (
    b"\x01\x01\x00\x00\x00"
    + b"\x00" * 8  # x = 0.0
    + b"\x00" * 8  # y = 0.0
)

# Full-matrix vectors keep their own fixed production-realistic dim so the
# format-index matrix stays stable even if the common default changes.
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
    """Build a dict {column_name -> pyarrow.Array} that fits both parquet and
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
    #   FloatVector / Int8Vector        -> FixedSizeList<element, dim>
    #   Float16/BFloat16/BinaryVector   -> fixed_size_binary raw bytes
    fv_arr = _float_vectors(ids, dim).flatten()
    f16_arr = _float16_vectors(ids, dim)
    bf16_arr = _bfloat16_vectors(ids, dim)
    i8_arr = _int8_vectors(ids, dim).flatten()
    bin_arr = _binary_vectors_bytes(ids, bin_dim)

    for name, vtype, vdim, _idx, _metric, _params in FULL_MATRIX_VECTOR_FIELDS:
        if name in excluded:
            continue
        if vtype == DataType.FLOAT_VECTOR:
            columns[name] = pa.FixedSizeListArray.from_arrays(
                pa.array(fv_arr, type=pa.float32()),
                list_size=vdim,
            )
        elif vtype == DataType.FLOAT16_VECTOR:
            columns[name] = _fixed_size_binary_vector_array(
                f16_arr,
                _vector_byte_width(vtype, vdim),
            )
        elif vtype == DataType.BFLOAT16_VECTOR:
            columns[name] = _fixed_size_binary_vector_array(
                bf16_arr,
                _vector_byte_width(vtype, vdim),
            )
        elif vtype == DataType.INT8_VECTOR:
            columns[name] = pa.FixedSizeListArray.from_arrays(
                pa.array(i8_arr, type=pa.int8()),
                list_size=vdim,
            )
        elif vtype == DataType.BINARY_VECTOR:
            columns[name] = _fixed_size_binary_vector_array(
                bin_arr,
                _vector_byte_width(vtype, vdim),
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


def build_basic_schema(client, ext_path, dim=ct.default_dim, ext_spec=None):
    """Minimal external schema: id(Int64) + value(Float) + embedding(FloatVector)."""
    schema = client.create_schema(
        external_source=ext_path,
        external_spec=ext_spec or build_external_spec(),
    )
    schema.add_field("id", DataType.INT64, external_field="id")
    schema.add_field("value", DataType.FLOAT, external_field="value")
    schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim, external_field="embedding")
    return schema


def build_typed_schema(client, ext_path, scalar_name, scalar_dtype, dim=ct.default_dim, ext_spec=None, **extra):
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


def build_all_scalar_schema(client, ext_path, dim=ct.default_dim, ext_spec=None):
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


def build_multi_vector_schema(client, ext_path, float_dim=ct.default_dim, bin_dim=ct.default_dim, ext_spec=None):
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


def query_count(client, collection_name):
    res = client.query(collection_name, filter="", output_fields=["count(*)"])
    return res[0]["count(*)"]


def upload_basic_data(
    minio_client, cfg, ext_key, num_rows=ct.default_nb, start_id=0, filename="data.parquet", dim=ct.default_dim
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


def require_format_dependencies(fmt):
    if fmt == "lance-table":
        import lance  # noqa: F401
    elif fmt == "iceberg-table":
        from pyiceberg.catalog.sql import SqlCatalog  # noqa: F401


def write_basic_format_dataset(fmt, minio_client, cfg, ext_url, ext_key, batches, dim=ct.default_dim):
    """Write the basic id/value/embedding dataset for one external format.

    Returns (external_source, external_spec). Iceberg rewrites external_source
    to the metadata.json URI.
    """
    require_format_dependencies(fmt)
    if fmt == "parquet":
        for idx, (start_id, num_rows) in enumerate(batches):
            upload_parquet(
                minio_client,
                cfg["bucket"],
                f"{ext_key}/file_{idx:03d}.parquet",
                gen_basic_parquet_bytes(num_rows, start_id, dim=dim),
            )
        return ext_url, build_external_spec(cfg, fmt=fmt)

    if fmt == "lance-table":
        _write_lance_to_minio_batches(minio_client, cfg["bucket"], ext_key, batches, dim=dim)
        return ext_url, build_external_spec(cfg, fmt=fmt)

    if fmt == "iceberg-table":
        snapshot_id, iceberg_url = _build_iceberg_table_in_minio(ext_key, cfg, batches=batches, dim=dim)
        return iceberg_url, build_external_spec(cfg, fmt=fmt, snapshot_id=int(snapshot_id))

    raise AssertionError(f"unsupported format: {fmt}")


def build_basic_format_schema(client, fmt, ext_url, ext_spec, dim=ct.default_dim):
    schema = client.create_schema(external_source=ext_url, external_spec=ext_spec)
    schema.add_field("id", DataType.INT64, external_field="id")
    if FORMAT_CASES[fmt]["has_value"]:
        schema.add_field("value", DataType.FLOAT, external_field="value")
    schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim, external_field="embedding")
    return schema


def assert_basic_format_rows(client, coll, fmt, batches, dim=ct.default_dim):
    total = sum(num_rows for _start_id, num_rows in batches)
    assert query_count(client, coll) == total
    for start_id, num_rows in batches:
        if num_rows == 0:
            continue
        rows = client.query(coll, filter=f"id == {start_id}", output_fields=["id", "embedding"])
        assert len(rows) == 1, f"[{fmt}] row id={start_id} missing"
        vec = rows[0]["embedding"]
        assert len(vec) == dim
        if fmt == "iceberg-table":
            for j, v in enumerate(vec):
                expected = float(start_id) + j * 0.1
                assert abs(v - expected) < 1e-2, f"[{fmt}] vec[{j}] for id={start_id} = {v}"
        if FORMAT_CASES[fmt]["has_value"]:
            rows = client.query(coll, filter=f"id == {start_id}", output_fields=["id", "value"])
            assert len(rows) == 1, f"[{fmt}] row id={start_id} missing"
            assert abs(rows[0]["value"] - start_id * 1.5) < 1e-3


# ============================================================
# Schema builders for validation tests (module-level, reused across classes)
# ============================================================

_PLACEHOLDER_SRC = "s3://localhost:9000/milvus-bucket/placeholder/"
_PLACEHOLDER_SPEC = build_external_spec()
_PLACEHOLDER_NEW_SOURCE = "minio://localhost:9000/milvus-bucket/new/"
_PLACEHOLDER_NEW_SPEC = build_external_spec()


def _schema_with_user_pk(client):
    schema = client.create_schema(external_source=_PLACEHOLDER_SRC, external_spec=_PLACEHOLDER_SPEC)
    schema.add_field("pk", DataType.INT64, is_primary=True, external_field="pk")
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="vec")
    return schema


def _schema_with_auto_id(client):
    schema = client.create_schema(external_source=_PLACEHOLDER_SRC, external_spec=_PLACEHOLDER_SPEC)
    schema.add_field("id", DataType.INT64, auto_id=True, external_field="id")
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="vec")
    return schema


def _schema_with_dynamic(client):
    schema = client.create_schema(
        external_source=_PLACEHOLDER_SRC,
        external_spec=_PLACEHOLDER_SPEC,
        enable_dynamic_field=True,
    )
    schema.add_field("id", DataType.INT64, external_field="id")
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="vec")
    return schema


def _schema_with_partition_key(client):
    schema = client.create_schema(external_source=_PLACEHOLDER_SRC, external_spec=_PLACEHOLDER_SPEC)
    schema.add_field("cat", DataType.VARCHAR, max_length=64, is_partition_key=True, external_field="cat")
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="vec")
    return schema


def _schema_missing_external_field(client):
    schema = client.create_schema(external_source=_PLACEHOLDER_SRC, external_spec=_PLACEHOLDER_SPEC)
    schema.add_field("plain_field", DataType.INT64)  # missing external_field
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="vec")
    return schema


def _schema_with_sparse_float_vector(client):
    schema = client.create_schema(external_source=_PLACEHOLDER_SRC, external_spec=_PLACEHOLDER_SPEC)
    schema.add_field("id", DataType.INT64, external_field="id")
    schema.add_field("sv", DataType.SPARSE_FLOAT_VECTOR, external_field="sv")
    return schema


def _schema_with_duplicate_external_field(client):
    schema = client.create_schema(external_source=_PLACEHOLDER_SRC, external_spec=_PLACEHOLDER_SPEC)
    schema.add_field("id", DataType.INT64, external_field="same_col")
    schema.add_field("value", DataType.FLOAT, external_field="same_col")
    schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="embedding")
    return schema


# ============================================================
# External file format helpers
# ============================================================


def _write_lance_to_minio_batches(minio_client, bucket, key_prefix, batches, dim=ct.default_dim):
    """Multi-fragment variant: each (start_id, count) tuple in `batches` is
    appended into the same Lance dataset, producing one fragment file per
    batch under data/. Used to exercise multi-data-file refresh.
    """
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


def _build_iceberg_table_in_minio(prefix, cfg, batches, dim=ct.default_dim):
    """Create an Iceberg table whose warehouse lives directly on MinIO.

    Writing the warehouse to MinIO (not locally) is required because Iceberg
    metadata.json records absolute data-file URIs at write time; if we wrote
    to file:// then uploaded, those URIs would be unreachable from the
    Milvus reader.

    Vector encoding: each row stores its vector as a fixed-size binary blob of
    dim * sizeof(float) bytes. Milvus maps external_field=embedding to a
    FloatVector field and reinterprets the raw bytes as float32.

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
    from pyiceberg.types import FixedType, FloatType, LongType, NestedField

    tmp = tempfile.mkdtemp(prefix="ibg_cat_")
    try:
        cat = SqlCatalog(
            "milvus_test",
            **{
                "uri": f"sqlite:///{tmp}/cat.db",
                "warehouse": f"s3://{cfg['bucket']}/{prefix}",
                "s3.endpoint": minio_endpoint_url(cfg),
                "s3.access-key-id": cfg["access_key"],
                "s3.secret-access-key": cfg["secret_key"],
                "s3.region": "us-east-1",
                "s3.path-style-access": "true",
            },
        )
        cat.create_namespace("ext")

        ibg_schema = IbgSchema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "value", FloatType(), required=False),
            NestedField(3, "embedding", FixedType(_vector_byte_width(DataType.FLOAT_VECTOR, dim)), required=False),
        )
        arrow_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=True),
                pa.field("value", pa.float32(), nullable=True),
                pa.field("embedding", pa.binary(_vector_byte_width(DataType.FLOAT_VECTOR, dim)), nullable=True),
            ]
        )
        tbl = cat.create_table("ext.t", schema=ibg_schema)

        for start_id, num_rows in batches:
            ids = list(range(start_id, start_id + num_rows))
            vec_bytes = [struct.pack(f"<{dim}f", *[float(i + j * 0.1) for j in range(dim)]) for i in ids]
            arrow_table = pa.table(
                {
                    "id": pa.array(ids, type=pa.int64()),
                    "value": pa.array([float(i) * 1.5 for i in ids], type=pa.float32()),
                    "embedding": pa.array(vec_bytes, type=pa.binary(_vector_byte_width(DataType.FLOAT_VECTOR, dim))),
                },
                schema=arrow_schema,
            )
            tbl.append(arrow_table)

        snap = tbl.current_snapshot().snapshot_id
        # metadata_location: s3://<bucket>/<prefix>/.../metadata/000NN-uuid.metadata.json
        # -> Milvus form: s3://<address>/<bucket>/...
        meta_loc = tbl.metadata_location
        if not meta_loc.startswith("s3://"):
            raise RuntimeError(f"unexpected metadata_loc scheme: {meta_loc}")
        bucket_and_key = meta_loc[len("s3://") :]
        ext_url = f"s3://{cfg['address']}/{bucket_and_key}"
        return snap, ext_url
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# Iceberg full-matrix is a strict subset of FULL_MATRIX_* because Iceberg V2
# only supports int (32-bit) and long (64-bit) integers; no Int8/Int16. We
# reuse the same field names where possible to keep query expectations
# identical between formats; Int8 -> IntegerType promoted to Milvus INT32
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
        FixedType,
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

    # Geometry -> BinaryType (WKB)
    nested_fields.append(NestedField(fid(), "geo", BinaryType(), required=False))
    arrow_fields.append(pa.field("geo", pa.binary(), nullable=True))
    arrow_columns["geo"] = pa.array([_GEOMETRY_WKB] * num_rows, type=pa.binary())

    # Vector fields -> FixedType blobs of dim*sizeof(elem) bytes.
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
        byte_width = _vector_byte_width(vtype, vdim)
        nested_fields.append(NestedField(fid(), name, FixedType(byte_width), required=False))
        arrow_fields.append(pa.field(name, pa.binary(byte_width), nullable=True))
        arrow_columns[name] = pa.array(vec_blobs(name, vtype, vdim), type=pa.binary(byte_width))

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
                "s3.endpoint": minio_endpoint_url(cfg),
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


# Smaller row count keeps full-matrix tests under ~2 min each: each test
# builds 21 fields with 10+ scalar indexes and 9 vector indexes.
FULL_MATRIX_NB = 200


def _full_matrix_query_vec(vec_field, dim):
    """Build a search query vector matching the dtype of the given vector
    field. pymilvus is dtype-strict on the wire: int8 needs np.int8,
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

    # One search per vector field confirms every (type x index x metric)
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
