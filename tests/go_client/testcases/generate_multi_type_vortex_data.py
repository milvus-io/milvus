#!/usr/bin/env python3
"""Generate Vortex dataset matching the multi-type schema used by the
parquet TestExternalCollectionMultipleDataTypes test.

Usage:
    python3 generate_multi_type_vortex_data.py <output_path> <num_rows> <vec_dim> <bin_vec_dim>

Schema mirrors generateMultiTypeParquetBytes():
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
    - embedding     (FixedSizeList<Float32, vec_dim>)
    - bin_vec       (FixedSizeList<UInt8, bin_vec_dim/8>)
    - fp16_vec      (FixedSizeList<UInt8, vec_dim*2>)
    - bf16_vec      (FixedSizeList<UInt8, vec_dim*2>)
    - int8_vec      (FixedSizeList<UInt8, vec_dim>)

vortex-data 0.56.0 doesn't support FixedSizeBinary directly; binary
vectors are written as FixedSizeList<UInt8>, matching milvus's internal
representation (NormalizeVectorArrays handles the conversion).

Environment variables:
    MINIO_ADDRESS, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
"""

import json
import os
import sys
import tempfile

import obstore
import pyarrow as pa
import vortex as vx
import vortex.io  # noqa: F401
from obstore.store import S3Store


def main():
    if len(sys.argv) < 5:
        print(f"Usage: {sys.argv[0]} <output_path> <num_rows> <vec_dim> <bin_vec_dim>", file=sys.stderr)
        sys.exit(1)

    output_path = sys.argv[1]
    num_rows = int(sys.argv[2])
    vec_dim = int(sys.argv[3])
    bin_vec_dim = int(sys.argv[4])

    endpoint = os.environ.get("MINIO_ADDRESS", "localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    bucket = os.environ.get("MINIO_BUCKET", "a-bucket")

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

    # Array<Int32>: [i*1, i*2, i*3]
    array_int_vals = [[i, i * 2, i * 3] for i in ids]
    # Array<String>: ["tag_<i>_a", "tag_<i>_b"]
    array_str_vals = [[f"tag_{i}_a", f"tag_{i}_b"] for i in ids]

    # Timestamp[us]: 2025-01-01T00:00:00Z = 1735689600000000us + i hours
    ts_vals = [1735689600000000 + i * 3600000000 for i in ids]

    # Geometry: WKT POINT
    geo_vals = [f"POINT({i} {i * 0.1:.1f})" for i in ids]

    # FloatVector
    embedding_flat = []
    for i in ids:
        for d in range(vec_dim):
            embedding_flat.append(float(i) * 0.1 + d)
    embedding_arr = pa.FixedSizeListArray.from_arrays(
        pa.array(embedding_flat, type=pa.float32()),
        vec_dim,
    )

    # BinaryVector / FP16 / BF16 / Int8Vector — all FixedSizeList<UInt8>
    def gen_uint8_block(byte_width):
        flat = []
        for i in ids:
            for b in range(byte_width):
                flat.append((i + b) % 256)
        return pa.FixedSizeListArray.from_arrays(
            pa.array(flat, type=pa.uint8()),
            byte_width,
        )

    bin_vec_arr = gen_uint8_block(bin_vec_byte_width)
    fp16_vec_arr = gen_uint8_block(fp16_byte_width)
    bf16_vec_arr = gen_uint8_block(bf16_byte_width)
    int8_vec_arr = gen_uint8_block(int8_vec_byte_width)

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
            "embedding": embedding_arr,
            "bin_vec": bin_vec_arr,
            "fp16_vec": fp16_vec_arr,
            "bf16_vec": bf16_vec_arr,
            "int8_vec": int8_vec_arr,
        }
    )

    with tempfile.NamedTemporaryFile(suffix=".vortex", delete=False) as tmp:
        tmp_path = tmp.name
    vx.io.write(table, tmp_path)
    data = open(tmp_path, "rb").read()
    os.unlink(tmp_path)

    store = S3Store(
        bucket=bucket,
        config={
            "access_key_id": access_key,
            "secret_access_key": secret_key,
            "endpoint_url": f"http://{endpoint}",
            "region": "us-east-1",
            "allow_http": "true",
        },
    )

    file_key = f"{output_path}/data.vortex"
    obstore.put(store, file_key, data)

    print(f"OK rows={num_rows} file={file_key}")


if __name__ == "__main__":
    main()
