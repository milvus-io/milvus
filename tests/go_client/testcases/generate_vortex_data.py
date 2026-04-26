#!/usr/bin/env python3
"""Generate Vortex dataset on MinIO for e2e tests.

Usage:
    python3 generate_vortex_data.py <output_path> <num_rows> [dim]

Example:
    python3 generate_vortex_data.py external-e2e-test/vortex_test 2000 4

Schema:
    - id        (Int64)
    - value     (Float32)
    - embedding (FixedSizeList<UInt8>[dim * sizeof(float32)])
    - varchar_val (String)            # exercises Arrow string -> VARCHAR
    - json_val    (String)            # exercises Arrow string -> JSON
    - geo_val     (Binary, WKB POINT) # exercises Arrow binary -> GEOMETRY

The string + binary columns are required to repro vortex-specific bugs:
    - issue #49352: vortex VARCHAR/JSON refresh "Expected 2 buffers, got 3"
    - issue #49353: vortex GEOMETRY load SIGSEGV

Environment variables:
    MINIO_ADDRESS   (default: localhost:9000)
    MINIO_ACCESS_KEY (default: minioadmin)
    MINIO_SECRET_KEY (default: minioadmin)
    MINIO_BUCKET     (default: a-bucket)

Requires: vortex-data>=0.56.0, pyarrow, obstore (pip install in Python >=3.11)
"""
import json
import os
import struct
import sys
import tempfile

import obstore
import pyarrow as pa
import vortex as vx
import vortex.io  # noqa: F401 — ensure vx.io is importable
from obstore.store import S3Store


def encode_wkb_point(x: float, y: float) -> bytes:
    # WKB layout (little-endian POINT): 1B order + 4B type + 8B x + 8B y
    return struct.pack("<BIdd", 1, 1, x, y)


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <output_path> <num_rows> [dim]", file=sys.stderr)
        sys.exit(1)

    output_path = sys.argv[1]
    num_rows = int(sys.argv[2])
    dim = int(sys.argv[3]) if len(sys.argv) > 3 else 4

    endpoint = os.environ.get("MINIO_ADDRESS", "localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    bucket = os.environ.get("MINIO_BUCKET", "a-bucket")

    byte_width = dim * 4  # sizeof(float32)

    # Milvus represents FloatVector(dim) as FixedSizeList(UInt8, dim*4) internally.
    # Vortex Python bindings don't support FixedSizeBinary, but FixedSizeList works.
    ids = list(range(num_rows))
    values = [float(i) * 1.5 for i in ids]
    # Pack floats to bytes, then flatten to a single uint8 array for FixedSizeListArray
    flat_bytes = []
    for i in ids:
        flat_bytes.extend(struct.pack(f"{dim}f", *[float(i) * 0.1 + j for j in range(dim)]))

    embedding_arr = pa.FixedSizeListArray.from_arrays(
        pa.array(flat_bytes, type=pa.uint8()), byte_width,
    )

    varchar_vals = [f"vc_{i}" for i in ids]
    json_vals = [json.dumps({"k": i, "name": f"item_{i}"}) for i in ids]
    geo_vals = [encode_wkb_point(float(i), float(i) * 0.1) for i in ids]

    table = pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "value": pa.array(values, type=pa.float32()),
        "embedding": embedding_arr,
        "varchar_val": pa.array(varchar_vals, type=pa.string()),
        "json_val": pa.array(json_vals, type=pa.string()),
        "geo_val": pa.array(geo_vals, type=pa.binary()),
    })

    # vx.io.write() only writes to local filesystem.
    # Write locally then upload to MinIO via obstore.
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
