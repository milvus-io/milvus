#!/usr/bin/env python3
"""Create Iceberg tables on MinIO for external table e2e tests.

Usage:
    python3 generate_iceberg_data.py --schema basic --num-rows 1000 --vec-dim 128
    python3 generate_iceberg_data.py --schema multi --num-rows 100 --vec-dim 4 --bin-vec-dim 8
"""

import argparse
import json
import os
import random
import struct

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
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


def create_basic_data(num_rows: int, vec_dim: int) -> pa.Table:
    random.seed(42)

    pks = list(range(num_rows))
    vectors = [[random.uniform(-1.0, 1.0) for _ in range(vec_dim)] for _ in range(num_rows)]
    schema = pa.schema(
        [
            pa.field("pk", pa.int64()),
            pa.field("label", pa.string()),
            pa.field("vector", pa.list_(pa.float32(), vec_dim)),
        ]
    )
    return pa.table(
        {
            "pk": pa.array(pks, type=pa.int64()),
            "label": pa.array([f"label_{i}" for i in range(num_rows)], type=pa.string()),
            "vector": pa.array(vectors, type=pa.list_(pa.float32(), vec_dim)),
        },
        schema=schema,
    )


def create_multi_data(num_rows: int, vec_dim: int, bin_vec_dim: int) -> pa.Table:
    embedding_byte_width = vec_dim * 4
    bin_vec_byte_width = bin_vec_dim // 8
    fp16_byte_width = vec_dim * 2
    bf16_byte_width = vec_dim * 2
    int8_vec_byte_width = vec_dim

    ids = list(range(num_rows))
    json_vals = [json.dumps({"key": i, "name": f"item_{i}"}, separators=(",", ":")) for i in ids]
    embedding_rows = [struct.pack(f"{vec_dim}f", *[float(i) * 0.1 + d for d in range(vec_dim)]) for i in ids]

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


def basic_iceberg_schema() -> Schema:
    return Schema(
        NestedField(1, "pk", LongType(), required=False),
        NestedField(2, "label", StringType(), required=False),
        NestedField(
            3,
            "vector",
            ListType(4, FloatType(), element_required=False),
            required=False,
        ),
    )


def multi_iceberg_schema(vec_dim: int, bin_vec_dim: int) -> Schema:
    embedding_byte_width = vec_dim * 4
    bin_vec_byte_width = bin_vec_dim // 8
    fp16_byte_width = vec_dim * 2
    bf16_byte_width = vec_dim * 2
    int8_vec_byte_width = vec_dim

    return Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "bool_val", BooleanType(), required=False),
        NestedField(3, "int8_val", IntegerType(), required=False),
        NestedField(4, "int16_val", IntegerType(), required=False),
        NestedField(5, "int32_val", IntegerType(), required=False),
        NestedField(6, "float_val", FloatType(), required=False),
        NestedField(7, "double_val", DoubleType(), required=False),
        NestedField(8, "varchar_val", StringType(), required=False),
        NestedField(9, "json_val", StringType(), required=False),
        NestedField(
            10,
            "array_int",
            ListType(20, IntegerType(), element_required=False),
            required=False,
        ),
        NestedField(
            11,
            "array_str",
            ListType(21, StringType(), element_required=False),
            required=False,
        ),
        NestedField(12, "ts_val", TimestamptzType(), required=False),
        NestedField(13, "geo_val", StringType(), required=False),
        NestedField(14, "embedding", FixedType(embedding_byte_width), required=False),
        NestedField(15, "bin_vec", FixedType(bin_vec_byte_width), required=False),
        NestedField(16, "fp16_vec", FixedType(fp16_byte_width), required=False),
        NestedField(17, "bf16_vec", FixedType(bf16_byte_width), required=False),
        NestedField(18, "int8_vec", FixedType(int8_vec_byte_width), required=False),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Create Iceberg e2e table on MinIO")
    parser.add_argument("--schema", choices=("basic", "multi"), default="basic")
    parser.add_argument("--endpoint", default="http://localhost:9000")
    parser.add_argument("--access-key", default=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"))
    parser.add_argument("--secret-key", default=os.environ.get("MINIO_SECRET_KEY", "minioadmin"))
    parser.add_argument("--bucket", default="a-bucket")
    parser.add_argument("--table-path", default="")
    parser.add_argument("--num-rows", type=int, default=None)
    parser.add_argument("--vec-dim", "--dim", dest="vec_dim", type=int, default=None)
    parser.add_argument("--bin-vec-dim", type=int, default=8)
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    if args.schema == "basic":
        table_path = args.table_path or "iceberg-test/e2e_test_table"
        table_name = "default.e2e_test"
        num_rows = args.num_rows if args.num_rows is not None else 1000
        vec_dim = args.vec_dim if args.vec_dim is not None else 128
        iceberg_schema = basic_iceberg_schema()
        data = create_basic_data(num_rows, vec_dim)
    else:
        table_path = args.table_path or "iceberg-test/multi_type_table"
        table_name = "default.multi_type"
        num_rows = args.num_rows if args.num_rows is not None else 100
        vec_dim = args.vec_dim if args.vec_dim is not None else 4
        iceberg_schema = multi_iceberg_schema(vec_dim, args.bin_vec_dim)
        data = create_multi_data(num_rows, vec_dim, args.bin_vec_dim)

    catalog = SqlCatalog(
        "test_catalog",
        **{
            "uri": "sqlite:///:memory:",
            "s3.endpoint": args.endpoint,
            "s3.access-key-id": args.access_key,
            "s3.secret-access-key": args.secret_key,
            "s3.region": "us-east-1",
            "warehouse": f"s3://{args.bucket}/{table_path}",
        },
    )

    try:
        catalog.create_namespace("default")
    except Exception:
        pass

    try:
        catalog.drop_table(table_name)
    except Exception:
        pass

    table = catalog.create_table(table_name, schema=iceberg_schema)
    table.append(data)

    table = catalog.load_table(table_name)
    snapshot = table.current_snapshot()
    result = {
        "table_location": table.location(),
        "metadata_location": table.metadata_location,
        "snapshot_id": snapshot.snapshot_id,
        "num_rows": num_rows,
        "dim": vec_dim,
        "vec_dim": vec_dim,
        "bin_vec_dim": args.bin_vec_dim,
    }

    print(json.dumps(result, indent=2))

    default_output = "iceberg_table_info.json" if args.schema == "basic" else "iceberg_multi_type_info.json"
    output_path = args.output or os.path.join(os.path.dirname(__file__), default_output)
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nTable info written to: {output_path}")


if __name__ == "__main__":
    main()
