#!/usr/bin/env python3
"""Create Iceberg table mirroring the 18-column multi-type schema used by
parquet/vortex/lance TestExternalCollectionMultipleDataTypes tests.

Schema names + semantics match generate_multi_type_vortex_data.py and
generate_multi_type_lance_data.py. Exercises the same milvus type matrix
across the iceberg-table source path.

Iceberg type mapping notes:
  - Int8/Int16 use IntegerType (iceberg lacks narrow ints); milvus narrows.
  - Vector fields use FixedType(byte_width) (iceberg's fixed-length binary).
  - Geometry uses StringType (WKT) — milvus converts WKT->WKB.

Usage:
    python3 create_iceberg_multi_type_table.py \
        [--endpoint http://localhost:9000] [--num-rows 100] \
        [--vec-dim 4] [--bin-vec-dim 8] [--output info.json]
"""

import argparse
import json
import os
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


def create_test_data(num_rows: int, vec_dim: int, bin_vec_dim: int) -> pa.Table:
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
    json_vals = [json.dumps({"key": i, "name": f"item_{i}"}, separators=(",", ":")) for i in ids]
    array_int_vals = [[i, i * 2, i * 3] for i in ids]
    array_str_vals = [[f"tag_{i}_a", f"tag_{i}_b"] for i in ids]
    ts_vals = [1735689600000000 + i * 3600000000 for i in ids]
    geo_vals = [f"POINT({i} {i * 0.1:.1f})" for i in ids]

    embedding_rows = [struct.pack(f"{vec_dim}f", *[float(i) * 0.1 + d for d in range(vec_dim)]) for i in ids]

    def gen_bytes_block(byte_width):
        return [bytes((i + b) % 256 for b in range(byte_width)) for i in ids]

    bin_vec_rows = gen_bytes_block(bin_vec_byte_width)
    fp16_vec_rows = gen_bytes_block(fp16_byte_width)
    bf16_vec_rows = gen_bytes_block(bf16_byte_width)
    int8_vec_rows = gen_bytes_block(int8_vec_byte_width)

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


def main():
    parser = argparse.ArgumentParser(description="Create Iceberg multi-type table on MinIO")
    parser.add_argument("--endpoint", default="http://localhost:9000")
    parser.add_argument("--access-key", default="minioadmin")
    parser.add_argument("--secret-key", default="minioadmin")
    parser.add_argument("--bucket", default="a-bucket")
    parser.add_argument("--table-path", default="iceberg-test/multi_type_table")
    parser.add_argument("--num-rows", type=int, default=100)
    parser.add_argument("--vec-dim", type=int, default=4)
    parser.add_argument("--bin-vec-dim", type=int, default=8)
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    embedding_byte_width = args.vec_dim * 4
    bin_vec_byte_width = args.bin_vec_dim // 8
    fp16_byte_width = args.vec_dim * 2
    bf16_byte_width = args.vec_dim * 2
    int8_vec_byte_width = args.vec_dim

    warehouse_path = f"s3://{args.bucket}/{args.table_path}"

    catalog = SqlCatalog(
        "test_catalog",
        **{
            "uri": "sqlite:///:memory:",
            "s3.endpoint": args.endpoint,
            "s3.access-key-id": args.access_key,
            "s3.secret-access-key": args.secret_key,
            "s3.region": "us-east-1",
            "warehouse": warehouse_path,
        },
    )

    try:
        catalog.create_namespace("default")
    except Exception:
        pass

    iceberg_schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "bool_val", BooleanType(), required=False),
        NestedField(3, "int8_val", IntegerType(), required=False),
        NestedField(4, "int16_val", IntegerType(), required=False),
        NestedField(5, "int32_val", IntegerType(), required=False),
        NestedField(6, "float_val", FloatType(), required=False),
        NestedField(7, "double_val", DoubleType(), required=False),
        NestedField(8, "varchar_val", StringType(), required=False),
        NestedField(9, "json_val", StringType(), required=False),
        NestedField(10, "array_int", ListType(20, IntegerType(), element_required=False), required=False),
        NestedField(11, "array_str", ListType(21, StringType(), element_required=False), required=False),
        NestedField(12, "ts_val", TimestamptzType(), required=False),
        NestedField(13, "geo_val", StringType(), required=False),
        NestedField(14, "embedding", FixedType(embedding_byte_width), required=False),
        NestedField(15, "bin_vec", FixedType(bin_vec_byte_width), required=False),
        NestedField(16, "fp16_vec", FixedType(fp16_byte_width), required=False),
        NestedField(17, "bf16_vec", FixedType(bf16_byte_width), required=False),
        NestedField(18, "int8_vec", FixedType(int8_vec_byte_width), required=False),
    )

    table_name = "default.multi_type"
    try:
        catalog.drop_table(table_name)
    except Exception:
        pass

    table = catalog.create_table(table_name, schema=iceberg_schema)

    data = create_test_data(args.num_rows, args.vec_dim, args.bin_vec_dim)
    table.append(data)

    table = catalog.load_table(table_name)
    snapshot = table.current_snapshot()

    result = {
        "table_location": table.location(),
        "metadata_location": table.metadata_location,
        "snapshot_id": snapshot.snapshot_id,
        "num_rows": args.num_rows,
        "vec_dim": args.vec_dim,
        "bin_vec_dim": args.bin_vec_dim,
    }
    print(json.dumps(result, indent=2))

    output_path = (
        args.output if args.output else os.path.join(os.path.dirname(__file__), "iceberg_multi_type_info.json")
    )
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nTable info written to: {output_path}")


if __name__ == "__main__":
    main()
