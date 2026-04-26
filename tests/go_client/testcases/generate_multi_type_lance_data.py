#!/usr/bin/env python3
"""Generate Lance dataset matching the multi-type schema used by the
parquet/vortex TestExternalCollectionMultipleDataTypes tests.

Usage:
    python3 generate_multi_type_lance_data.py <s3_uri> <num_rows> <vec_dim> <bin_vec_dim>

Example:
    python3 generate_multi_type_lance_data.py \
        s3://a-bucket/files/external-e2e-test/lance_multi 100 4 8

Schema mirrors generateMultiTypeParquetBytes() / generate_multi_type_vortex_data.py:
    - id            (Int64)
    - bool_val      (Bool)
    - int8_val      (Int8)
    - int16_val     (Int16)
    - int32_val     (Int32)
    - float_val     (Float32)
    - double_val    (Float64)
    - varchar_val   (String)
    - json_val      (String, JSON text)
    - array_int     (List<Int32>)
    - array_str     (List<String>)
    - ts_val        (Timestamp[us, UTC])
    - geo_val       (String, WKT POINT)
    - embedding     (FixedSizeBinary[vec_dim * 4])
    - bin_vec       (FixedSizeBinary[bin_vec_dim/8])
    - fp16_vec      (FixedSizeBinary[vec_dim*2])
    - bf16_vec      (FixedSizeBinary[vec_dim*2])
    - int8_vec      (FixedSizeBinary[vec_dim])

Lance bridge in milvus-storage uses FixedSizeBinary for vectors (see
lance_bridgeimpl.rs note in generate_lance_data.py).

Environment variables:
    MINIO_ADDRESS, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
"""
import json
import os
import struct
import sys

import lance
import pyarrow as pa


def main():
    if len(sys.argv) < 5:
        print(
            f"Usage: {sys.argv[0]} <s3_uri> <num_rows> <vec_dim> <bin_vec_dim>",
            file=sys.stderr,
        )
        sys.exit(1)

    s3_uri = sys.argv[1]
    num_rows = int(sys.argv[2])
    vec_dim = int(sys.argv[3])
    bin_vec_dim = int(sys.argv[4])

    endpoint = os.environ.get("MINIO_ADDRESS", "localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    embedding_byte_width = vec_dim * 4
    bin_vec_byte_width = bin_vec_dim // 8
    fp16_byte_width = vec_dim * 2
    bf16_byte_width = vec_dim * 2
    int8_vec_byte_width = vec_dim

    ids = list(range(num_rows))

    bool_vals = [i % 2 == 0 for i in ids]
    int8_vals = [i % 100 for i in ids]
    int16_vals = [i * 10 for i in ids]
    int32_vals = [i * 100 for i in ids]
    float_vals = [float(i) * 1.5 for i in ids]
    double_vals = [float(i) * 0.01 for i in ids]
    varchar_vals = [f"str_{i:04d}" for i in ids]
    # Compact JSON (no spaces) — matches verifier substring assertions.
    json_vals = [
        json.dumps({"key": i, "name": f"item_{i}"}, separators=(",", ":"))
        for i in ids
    ]

    array_int_vals = [[i, i * 2, i * 3] for i in ids]
    array_str_vals = [[f"tag_{i}_a", f"tag_{i}_b"] for i in ids]

    # Timestamp[us]: 2025-01-01T00:00:00Z = 1735689600000000us + i hours
    ts_vals = [1735689600000000 + i * 3600000000 for i in ids]

    # Geometry: WKT POINT
    geo_vals = [f"POINT({i} {i * 0.1:.1f})" for i in ids]

    # FloatVector packed as float32 bytes
    embedding_rows = [
        struct.pack(f"{vec_dim}f", *[float(i) * 0.1 + d for d in range(vec_dim)])
        for i in ids
    ]

    def gen_bytes_block(byte_width):
        return [
            bytes((i + b) % 256 for b in range(byte_width)) for i in ids
        ]

    bin_vec_rows = gen_bytes_block(bin_vec_byte_width)
    fp16_vec_rows = gen_bytes_block(fp16_byte_width)
    bf16_vec_rows = gen_bytes_block(bf16_byte_width)
    int8_vec_rows = gen_bytes_block(int8_vec_byte_width)

    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("bool_val", pa.bool_()),
        pa.field("int8_val", pa.int8()),
        pa.field("int16_val", pa.int16()),
        pa.field("int32_val", pa.int32()),
        pa.field("float_val", pa.float32()),
        pa.field("double_val", pa.float64()),
        pa.field("varchar_val", pa.string()),
        pa.field("json_val", pa.string()),
        pa.field("array_int", pa.list_(pa.int32())),
        pa.field("array_str", pa.list_(pa.string())),
        pa.field("ts_val", pa.timestamp("us", tz="UTC")),
        pa.field("geo_val", pa.string()),
        pa.field("embedding", pa.binary(embedding_byte_width)),
        pa.field("bin_vec", pa.binary(bin_vec_byte_width)),
        pa.field("fp16_vec", pa.binary(fp16_byte_width)),
        pa.field("bf16_vec", pa.binary(bf16_byte_width)),
        pa.field("int8_vec", pa.binary(int8_vec_byte_width)),
    ])

    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "bool_val": pa.array(bool_vals, type=pa.bool_()),
            "int8_val": pa.array(int8_vals, type=pa.int8()),
            "int16_val": pa.array(int16_vals, type=pa.int16()),
            "int32_val": pa.array(int32_vals, type=pa.int32()),
            "float_val": pa.array(float_vals, type=pa.float32()),
            "double_val": pa.array(double_vals, type=pa.float64()),
            "varchar_val": pa.array(varchar_vals, type=pa.string()),
            "json_val": pa.array(json_vals, type=pa.string()),
            "array_int": pa.array(array_int_vals, type=pa.list_(pa.int32())),
            "array_str": pa.array(array_str_vals, type=pa.list_(pa.string())),
            "ts_val": pa.array(ts_vals, type=pa.timestamp("us", tz="UTC")),
            "geo_val": pa.array(geo_vals, type=pa.string()),
            "embedding": pa.array(embedding_rows, type=pa.binary(embedding_byte_width)),
            "bin_vec": pa.array(bin_vec_rows, type=pa.binary(bin_vec_byte_width)),
            "fp16_vec": pa.array(fp16_vec_rows, type=pa.binary(fp16_byte_width)),
            "bf16_vec": pa.array(bf16_vec_rows, type=pa.binary(bf16_byte_width)),
            "int8_vec": pa.array(int8_vec_rows, type=pa.binary(int8_vec_byte_width)),
        },
        schema=schema,
    )

    storage_options = {
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "aws_endpoint": f"http://{endpoint}",
        "aws_region": "us-east-1",
        "allow_http": "true",
    }

    ds = lance.write_dataset(
        table,
        s3_uri,
        mode="overwrite",
        storage_options=storage_options,
    )
    print(
        f"OK rows={ds.count_rows()} fragments={len(ds.get_fragments())} "
        f"uri={s3_uri}"
    )


if __name__ == "__main__":
    main()
