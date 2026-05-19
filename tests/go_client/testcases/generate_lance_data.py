#!/usr/bin/env python3
"""Generate Lance datasets on MinIO for external table e2e tests.

Usage:
    python3 generate_lance_data.py --schema basic <s3_uri> <num_rows> [--start-id 0]
    python3 generate_lance_data.py --schema multi <s3_uri> <num_rows> --vec-dim 4 --bin-vec-dim 8

Schemas:
    basic:
        id (Int64), value (Float32), embedding (FixedSizeBinary[vec_dim * 4])
    multi:
        the 18-column schema used by TestExternalCollectionMultipleDataTypes

Environment variables:
    MINIO_ADDRESS, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
"""

import argparse
import json
import os
import struct

import lance
import pyarrow as pa


def create_basic_table(num_rows: int, start_id: int, vec_dim: int) -> pa.Table:
    byte_width = vec_dim * 4

    # TODO: Switch to FixedSizeList<Float32, dim> (Lance's native vector format)
    # once milvus-storage's lance bridge supports automatic schema resolution.
    #
    # Currently BlockingFragmentReader (lance_bridgeimpl.rs) uses the caller's
    # schema as the FFI output schema (self.projection), but Lance reads data in
    # its native types. When Milvus passes FixedSizeBinary but Lance stores
    # FixedSizeList, ImportChunkedArray fails due to n_children mismatch.
    # Storing vectors as FixedSizeBinary avoids the type conflict.
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("value", pa.float32()),
            pa.field("embedding", pa.binary(byte_width)),
        ]
    )

    ids = list(range(start_id, start_id + num_rows))
    embeddings = [struct.pack(f"{vec_dim}f", *[float(i) * 0.1 + j for j in range(vec_dim)]) for i in ids]
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array([float(i) * 1.5 for i in ids], type=pa.float32()),
            "embedding": pa.array(embeddings, type=pa.binary(byte_width)),
        },
        schema=schema,
    )


def create_multi_table(num_rows: int, start_id: int, vec_dim: int, bin_vec_dim: int) -> pa.Table:
    embedding_byte_width = vec_dim * 4
    bin_vec_byte_width = bin_vec_dim // 8
    fp16_byte_width = vec_dim * 2
    bf16_byte_width = vec_dim * 2
    int8_vec_byte_width = vec_dim

    ids = list(range(start_id, start_id + num_rows))
    json_vals = [json.dumps({"key": i, "name": f"item_{i}"}, separators=(",", ":")) for i in ids]

    def gen_bytes_block(byte_width: int) -> list[bytes]:
        return [bytes((i + b) % 256 for b in range(byte_width)) for i in ids]

    schema = pa.schema(
        [
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
        ]
    )

    embedding_rows = [struct.pack(f"{vec_dim}f", *[float(i) * 0.1 + d for d in range(vec_dim)]) for i in ids]
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
            "json_val": pa.array(json_vals, type=pa.string()),
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
            "embedding": pa.array(embedding_rows, type=pa.binary(embedding_byte_width)),
            "bin_vec": pa.array(
                gen_bytes_block(bin_vec_byte_width),
                type=pa.binary(bin_vec_byte_width),
            ),
            "fp16_vec": pa.array(gen_bytes_block(fp16_byte_width), type=pa.binary(fp16_byte_width)),
            "bf16_vec": pa.array(gen_bytes_block(bf16_byte_width), type=pa.binary(bf16_byte_width)),
            "int8_vec": pa.array(
                gen_bytes_block(int8_vec_byte_width),
                type=pa.binary(int8_vec_byte_width),
            ),
        },
        schema=schema,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Lance e2e data on MinIO")
    parser.add_argument("--schema", choices=("basic", "multi"), default="basic")
    parser.add_argument("s3_uri")
    parser.add_argument("num_rows", type=int)
    parser.add_argument("legacy_start_id", nargs="?", type=int)
    parser.add_argument("--start-id", type=int, default=None)
    parser.add_argument("--vec-dim", type=int, default=4)
    parser.add_argument("--bin-vec-dim", type=int, default=8)
    args = parser.parse_args()

    start_id = args.start_id
    if start_id is None:
        start_id = args.legacy_start_id if args.legacy_start_id is not None else 0

    if args.schema == "basic":
        table = create_basic_table(args.num_rows, start_id, args.vec_dim)
    else:
        table = create_multi_table(args.num_rows, start_id, args.vec_dim, args.bin_vec_dim)

    storage_options = {
        "aws_access_key_id": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        "aws_secret_access_key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        "aws_endpoint": f"http://{os.environ.get('MINIO_ADDRESS', 'localhost:9000')}",
        "aws_region": "us-east-1",
        "allow_http": "true",
    }

    ds = lance.write_dataset(
        table,
        args.s3_uri,
        mode="overwrite",
        storage_options=storage_options,
    )
    print(f"OK schema={args.schema} rows={ds.count_rows()} fragments={len(ds.get_fragments())} uri={args.s3_uri}")


if __name__ == "__main__":
    main()
