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


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Parquet e2e data")
    parser.add_argument(
        "--schema",
        choices=("basic", "multi", "large", "nullable_vector", "snapshot_restore"),
        default="basic",
    )
    parser.add_argument("output_file")
    parser.add_argument("num_rows", type=int)
    parser.add_argument("--start-id", type=int, default=0)
    parser.add_argument("--vec-dim", type=int, default=4)
    parser.add_argument("--bin-vec-dim", type=int, default=8)
    parser.add_argument("--compression", default=None)
    parser.add_argument("--batch-size", type=int, default=10000)
    args = parser.parse_args()

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
