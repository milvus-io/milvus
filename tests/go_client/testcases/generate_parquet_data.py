#!/usr/bin/env python3
"""Generate Parquet files for external table e2e tests.

Usage:
    python3 generate_parquet_data.py --schema basic <output_file> <num_rows>
    python3 generate_parquet_data.py --schema multi <output_file> <num_rows>
    python3 generate_parquet_data.py --schema large <output_file> <num_rows> --vec-dim 128
    python3 generate_parquet_data.py --schema nullable_vector <output_file> 3
    python3 generate_parquet_data.py --schema snapshot_restore <output_file> <num_rows>
"""

from __future__ import annotations

import argparse
import json
import random
import struct
from collections.abc import Iterator

import pyarrow as pa
import pyarrow.parquet as pq


def fixed_float_list(values: list[float], dim: int) -> pa.FixedSizeListArray:
    return pa.FixedSizeListArray.from_arrays(pa.array(values, type=pa.float32()), dim)


def vector_values(ids: range, dim: int) -> list[float]:
    values = []
    for row_id in ids:
        for d in range(dim):
            values.append(float(row_id) * 0.1 + d)
    return values


def byte_rows(ids: range, byte_width: int, multiplier: int = 1) -> list[bytes]:
    return [bytes((row_id * multiplier + b) % 256 for b in range(byte_width)) for row_id in ids]


def create_basic_table(num_rows: int, start_id: int, vec_dim: int) -> pa.Table:
    ids = range(start_id, start_id + num_rows)
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array([float(i) * 1.5 for i in ids], type=pa.float32()),
            "embedding": fixed_float_list(vector_values(ids, vec_dim), vec_dim),
        }
    )


def create_multi_table(num_rows: int, start_id: int, vec_dim: int, bin_vec_dim: int) -> pa.Table:
    ids = range(start_id, start_id + num_rows)
    bin_vec_byte_width = bin_vec_dim // 8
    fp16_byte_width = vec_dim * 2
    bf16_byte_width = vec_dim * 2
    int8_vec_byte_width = vec_dim

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
            "embedding": fixed_float_list(vector_values(ids, vec_dim), vec_dim),
            "bin_vec": pa.array(
                byte_rows(ids, bin_vec_byte_width),
                type=pa.binary(bin_vec_byte_width),
            ),
            "fp16_vec": pa.array(byte_rows(ids, fp16_byte_width), type=pa.binary(fp16_byte_width)),
            "bf16_vec": pa.array(
                byte_rows(ids, bf16_byte_width, multiplier=2),
                type=pa.binary(bf16_byte_width),
            ),
            "int8_vec": pa.array(
                byte_rows(ids, int8_vec_byte_width, multiplier=3),
                type=pa.binary(int8_vec_byte_width),
            ),
        }
    )


def create_large_table(num_rows: int, start_id: int, vec_dim: int) -> pa.Table:
    ids = range(start_id, start_id + num_rows)
    rng = random.Random(start_id)
    embedding_values = [rng.random() for _ in range(num_rows * vec_dim)]

    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "score": pa.array([float(i) * 0.01 for i in ids], type=pa.float64()),
            "label": pa.array([i % 100 for i in ids], type=pa.int32()),
            "tag": pa.array([f"item_{i}_category_{i % 50}" for i in ids], type=pa.string()),
            "value": pa.array([float(i) * 0.001 for i in ids], type=pa.float32()),
            "embedding": fixed_float_list(embedding_values, vec_dim),
        }
    )


def create_nullable_vector_table(num_rows: int, start_id: int, vec_dim: int) -> pa.Table:
    ids = range(start_id, start_id + num_rows)
    byte_width = vec_dim * 4
    rows = []
    for i, row_id in enumerate(ids):
        if i == 1:
            rows.append(None)
            continue
        values = [float(row_id * vec_dim + d) for d in range(vec_dim)]
        rows.append(struct.pack(f"<{vec_dim}f", *values))

    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("embedding", pa.binary(byte_width), nullable=True),
        ]
    )
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "embedding": pa.array(rows, type=pa.binary(byte_width)),
        },
        schema=schema,
    )


def create_snapshot_restore_table(num_rows: int, start_id: int, vec_dim: int) -> pa.Table:
    ids = range(start_id, start_id + num_rows)
    byte_width = vec_dim * 4
    rows = [struct.pack(f"<{vec_dim}f", *[float(row_id) * 0.1 + d for d in range(vec_dim)]) for row_id in ids]

    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array([float(i) * 1.5 for i in ids], type=pa.float32()),
            "embedding": pa.array(rows, type=pa.binary(byte_width)),
        }
    )


def make_table(
    schema: str,
    num_rows: int,
    start_id: int,
    vec_dim: int,
    bin_vec_dim: int,
) -> pa.Table:
    if schema == "basic":
        return create_basic_table(num_rows, start_id, vec_dim)
    if schema == "multi":
        return create_multi_table(num_rows, start_id, vec_dim, bin_vec_dim)
    if schema == "large":
        return create_large_table(num_rows, start_id, vec_dim)
    if schema == "nullable_vector":
        return create_nullable_vector_table(num_rows, start_id, vec_dim)
    if schema == "snapshot_restore":
        return create_snapshot_restore_table(num_rows, start_id, vec_dim)
    raise ValueError(f"unknown parquet data schema: {schema}")


def iter_tables(
    schema: str,
    num_rows: int,
    start_id: int,
    vec_dim: int,
    bin_vec_dim: int,
    batch_size: int,
) -> Iterator[pa.Table]:
    if num_rows == 0:
        yield make_table(schema, 0, start_id, vec_dim, bin_vec_dim)
        return

    for offset in range(0, num_rows, batch_size):
        rows = min(batch_size, num_rows - offset)
        yield make_table(schema, rows, start_id + offset, vec_dim, bin_vec_dim)


def write_parquet(
    output_file: str,
    schema: str,
    num_rows: int,
    start_id: int,
    vec_dim: int,
    bin_vec_dim: int,
    compression: str | None,
    batch_size: int,
) -> None:
    writer = None
    try:
        for table in iter_tables(schema, num_rows, start_id, vec_dim, bin_vec_dim, batch_size):
            if writer is None:
                writer = pq.ParquetWriter(output_file, table.schema, compression=compression)
            writer.write_table(table, row_group_size=max(table.num_rows, 1))
    finally:
        if writer is not None:
            writer.close()



def write_struct_dataset(output_file, shape, fmt, prefix, epoch, scenario="basic"):
    """Create isolated nested source files; credentials are supplied via env."""
    import os
    import tempfile
    from pathlib import Path
    from pyarrow.fs import S3FileSystem
    endpoint = os.environ["MINIO_ADDRESS"]
    bucket = os.environ["MINIO_BUCKET"]
    access = os.environ["MINIO_ACCESS_KEY"]
    secret = os.environ["MINIO_SECRET_KEY"]
    store = S3FileSystem(access_key=access, secret_key=secret, endpoint_override=endpoint, scheme="http", region="us-east-1")
    vector_type = pa.binary() if fmt in ("vortex", "lance-table") else pa.binary(8)
    count = 4 + epoch
    nested_type = pa.struct([("score", pa.int64()), ("ignored", pa.string())])
    clips = []
    for i in range(count):
        value = {"score": i + epoch * 100, "ignored": "unselected"}
        clips.append(None if i == 0 else (value if shape == "struct" else ([] if i == 1 else [value, {"score": -i, "ignored": "unselected"}])))
    if scenario == "missing_child":
        nested_type = pa.struct([("ignored", pa.string())])
    elif scenario == "null_element":
        assert shape == "list_struct"
        clips = [[None, {"score": i, "ignored": "valid"}] for i in range(count)]
    elif scenario == "rich":
        assert shape == "list_struct"
        nested_type = pa.struct([("score", pa.int64()), ("tags", pa.list_(pa.int64())),
                                 ("vec", vector_type), ("ignored", pa.string())])
        clips = [None, [],
                 [{"score": None, "tags": [], "vec": struct.pack("<2f", 2., 1.), "ignored": "a"},
                  {"score": 20, "tags": [1, 2], "vec": struct.pack("<2f", 3., 1.), "ignored": "b"},
                  {"score": 40, "tags": [], "vec": None, "ignored": "d"}],
                 [{"score": 30, "tags": None, "vec": struct.pack("<2f", 4., 1.), "ignored": "c"}]]
        count = len(clips)
    table = pa.table({"id": pa.array(range(count), type=pa.int64()),
                      "embedding": pa.array([struct.pack("<2f", float(i), 1.0) for i in range(count)], type=vector_type),
                      "clips": pa.array(clips, type=nested_type if shape == "struct" else pa.list_(nested_type))})
    source = prefix
    with tempfile.TemporaryDirectory() as tmp:
        if fmt == "iceberg-table":
            from pyiceberg.catalog.sql import SqlCatalog
            from pyiceberg.schema import Schema
            from pyiceberg.types import NestedField, LongType, StringType, FixedType, StructType, ListType
            child = StructType(NestedField(6, "ignored", StringType())) if scenario == "missing_child" else StructType(NestedField(5, "score", LongType()), NestedField(6, "ignored", StringType()))
            if scenario == "rich":
                child = StructType(NestedField(5, "score", LongType()),
                    NestedField(7, "tags", ListType(8, LongType(), element_required=False)),
                    NestedField(9, "vec", FixedType(8)), NestedField(6, "ignored", StringType()))
            schema = Schema(NestedField(1, "id", LongType()), NestedField(2, "embedding", FixedType(8)),
                            NestedField(3, "clips", child if shape == "struct" else ListType(4, child, element_required=False)))
            catalog = SqlCatalog("test", **{"uri": f"sqlite:///{tmp}/catalog.db", "warehouse": f"s3://{bucket}/{prefix}",
                "s3.endpoint": f"http://{endpoint}", "s3.access-key-id": access, "s3.secret-access-key": secret,
                "s3.region": "us-east-1", "s3.path-style-access": "true"})
            catalog.create_namespace("test")
            target = catalog.create_table("test.nested", schema=schema)
            # Register Arrow-written Parquet without PyIceberg's nested-array
            # projection, which drops List validity in the pinned writer.
            data_path = f"{bucket}/{prefix}/source.parquet"
            with store.open_output_stream(data_path) as stream:
                pq.write_table(table, stream, row_group_size=2)
            with store.open_input_file(data_path) as stream:
                restored = pq.read_table(stream)
            assert restored.equals(table), "Iceberg data file roundtrip changed nested values"
            target.add_files([f"s3://{data_path}"])
            source = target.metadata_location.removeprefix(f"s3://{bucket}/")
        else:
            if fmt == "parquet":
                pq.write_table(table, f"{tmp}/data.parquet", row_group_size=2)
            elif fmt == "vortex":
                import vortex.io
                vortex.io.write(table, f"{tmp}/data.vortex")
            else:
                import lance
                lance.write_dataset(table, f"{tmp}/dataset.lance", data_storage_version="2.1")
                restored = lance.dataset(f"{tmp}/dataset.lance").to_table()
                assert restored.equals(table), f"Lance source roundtrip changed data: expected={table.to_pylist()} actual={restored.to_pylist()}"
                source = f"{prefix}/dataset.lance"
            for path in Path(tmp).rglob("*"):
                if path.is_file():
                    with store.open_output_stream(f"{bucket}/{prefix}/{path.relative_to(tmp)}") as stream:
                        stream.write(path.read_bytes())
    Path(output_file).write_text(json.dumps({"source": source}))


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Parquet e2e data")
    parser.add_argument(
        "--schema",
        choices=("basic", "multi", "large", "nullable_vector", "snapshot_restore", "struct", "list_struct"),
        default="basic",
    )
    parser.add_argument("output_file")
    parser.add_argument("num_rows", type=int)
    parser.add_argument("--start-id", type=int, default=0)
    parser.add_argument("--vec-dim", type=int, default=4)
    parser.add_argument("--bin-vec-dim", type=int, default=8)
    parser.add_argument("--compression", default=None)
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--format", default="parquet", choices=("parquet", "vortex", "lance-table", "iceberg-table"))
    parser.add_argument("--prefix", default="")
    parser.add_argument("--struct-case", choices=("basic", "missing_child", "null_element", "rich"), default="basic")
    args = parser.parse_args()
    if args.schema in ("struct", "list_struct"):
        write_struct_dataset(args.output_file, args.schema, args.format, args.prefix, args.start_id, args.struct_case)
        return

    write_parquet(
        args.output_file,
        args.schema,
        args.num_rows,
        args.start_id,
        args.vec_dim,
        args.bin_vec_dim,
        args.compression,
        args.batch_size,
    )
    print(
        f"OK schema={args.schema} rows={args.num_rows} compression={args.compression or 'none'} file={args.output_file}"
    )


if __name__ == "__main__":
    main()
