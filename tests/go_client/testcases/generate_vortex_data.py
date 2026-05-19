#!/usr/bin/env python3
"""Generate Vortex datasets on MinIO for external table e2e tests.

Usage:
    python3 generate_vortex_data.py --schema basic <output_path> <num_rows> --vec-dim 4
    python3 generate_vortex_data.py --schema multi <output_path> <num_rows> \
        --vec-dim 4 --bin-vec-dim 8

    Schemas:
    basic:
        id/value/embedding plus varchar/json/geometry regression columns.
    multi:
        scalar/array/json/geometry/timestamp columns plus a float vector.

Environment variables:
    MINIO_ADDRESS, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
"""

import argparse
import json
import os
import struct
import tempfile

import obstore
import pyarrow as pa
import vortex as vx
import vortex.io  # noqa: F401
from obstore.store import S3Store


def encode_wkb_point(x: float, y: float) -> bytes:
    # WKB layout (little-endian POINT): 1B order + 4B type + 8B x + 8B y.
    return struct.pack("<BIdd", 1, 1, x, y)


def create_basic_table(num_rows: int, start_id: int, vec_dim: int) -> pa.Table:
    ids = list(range(start_id, start_id + num_rows))

    embedding_flat = []
    for i in ids:
        for d in range(vec_dim):
            embedding_flat.append(float(i) * 0.1 + d)

    embedding_arr = pa.FixedSizeListArray.from_arrays(
        pa.array(embedding_flat, type=pa.float32()),
        vec_dim,
    )

    # VARCHAR/JSON/GEOMETRY columns cover Vortex regressions from
    # https://github.com/milvus-io/milvus/issues/49352 and
    # https://github.com/milvus-io/milvus/issues/49353.
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array([float(i) * 1.5 for i in ids], type=pa.float32()),
            "embedding": embedding_arr,
            "varchar_val": pa.array([f"vc_{i}" for i in ids], type=pa.string()),
            "json_val": pa.array(
                [json.dumps({"k": i, "name": f"item_{i}"}) for i in ids],
                type=pa.string(),
            ),
            "geo_val": pa.array(
                [encode_wkb_point(float(i), float(i) * 0.1) for i in ids],
                type=pa.binary(),
            ),
        }
    )


def create_multi_table(num_rows: int, start_id: int, vec_dim: int, bin_vec_dim: int) -> pa.Table:
    ids = list(range(start_id, start_id + num_rows))

    embedding_flat = []
    for i in ids:
        for d in range(vec_dim):
            embedding_flat.append(float(i) * 0.1 + d)
    embedding_arr = pa.FixedSizeListArray.from_arrays(
        pa.array(embedding_flat, type=pa.float32()),
        vec_dim,
    )

    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "bool_val": pa.array([i % 2 == 0 for i in ids], type=pa.bool_()),
            "int8_val": pa.array([i % 100 for i in ids], type=pa.int8()),
            "int16_val": pa.array([i * 10 for i in ids], type=pa.int16()),
            "int32_val": pa.array([i * 100 for i in ids], type=pa.int32()),
            "float_val": pa.array([float(i) * 1.5 for i in ids], type=pa.float32()),
            "double_val": pa.array([float(i) * 0.01 for i in ids], type=pa.float64()),
            "varchar_val": pa.array([f"str_{i:04d}" for i in ids], type=pa.string()),
            "json_val": pa.array(
                [json.dumps({"key": i, "name": f"item_{i}"}, separators=(",", ":")) for i in ids],
                type=pa.string(),
            ),
            "array_int": pa.array([[i, i * 2, i * 3] for i in ids], type=pa.list_(pa.int32())),
            "array_str": pa.array(
                [[f"tag_{i}_a", f"tag_{i}_b"] for i in ids],
                type=pa.list_(pa.string()),
            ),
            "ts_val": pa.array(
                [1735689600000000 + i * 3600000000 for i in ids],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "geo_val": pa.array([f"POINT({i} {i * 0.1:.1f})" for i in ids], type=pa.string()),
            "embedding": embedding_arr,
        }
    )


def write_vortex_table(table: pa.Table, output_path: str, bucket: str) -> str:
    with tempfile.NamedTemporaryFile(suffix=".vortex", delete=False) as tmp:
        tmp_path = tmp.name
    vx.io.write(table, tmp_path)
    with open(tmp_path, "rb") as f:
        data = f.read()
    os.unlink(tmp_path)

    store = S3Store(
        bucket=bucket,
        config={
            "access_key_id": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            "secret_access_key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
            "endpoint_url": f"http://{os.environ.get('MINIO_ADDRESS', 'localhost:9000')}",
            "region": "us-east-1",
            "allow_http": "true",
        },
    )

    file_key = f"{output_path}/data.vortex"
    obstore.put(store, file_key, data)
    return file_key


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Vortex e2e data on MinIO")
    parser.add_argument("--schema", choices=("basic", "multi"), default="basic")
    parser.add_argument("output_path")
    parser.add_argument("num_rows", type=int)
    parser.add_argument("legacy_vec_dim", nargs="?", type=int)
    parser.add_argument("--start-id", type=int, default=0)
    parser.add_argument("--vec-dim", type=int, default=None)
    parser.add_argument("--bin-vec-dim", type=int, default=8)
    args = parser.parse_args()

    vec_dim = args.vec_dim
    if vec_dim is None:
        vec_dim = args.legacy_vec_dim if args.legacy_vec_dim is not None else 4

    if args.schema == "basic":
        table = create_basic_table(args.num_rows, args.start_id, vec_dim)
    else:
        table = create_multi_table(args.num_rows, args.start_id, vec_dim, args.bin_vec_dim)

    bucket = os.environ.get("MINIO_BUCKET", "a-bucket")
    file_key = write_vortex_table(table, args.output_path, bucket)
    print(f"OK schema={args.schema} rows={args.num_rows} file={file_key}")


if __name__ == "__main__":
    main()
