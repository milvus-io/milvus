#!/usr/bin/env python3
"""Generate Lance dataset on MinIO for e2e tests.

Usage:
    python3 generate_lance_data.py <s3_uri> <num_rows> [start_id]

Example:
    python3 generate_lance_data.py s3://a-bucket/files/external-e2e-test/lance_test 2000 0

Schema:
    - id (Int64)
    - value (Float32)
    - embedding (FixedSizeBinary[dim * sizeof(float32)])

Environment variables:
    MINIO_ADDRESS   (default: localhost:9000)
    MINIO_ACCESS_KEY (default: minioadmin)
    MINIO_SECRET_KEY (default: minioadmin)
"""
import os
import struct
import sys

import lance
import pyarrow as pa


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <s3_uri> <num_rows> [start_id]", file=sys.stderr)
        sys.exit(1)

    s3_uri = sys.argv[1]
    num_rows = int(sys.argv[2])
    start_id = int(sys.argv[3]) if len(sys.argv) > 3 else 0

    endpoint = os.environ.get("MINIO_ADDRESS", "localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    dim = 4
    byte_width = dim * 4  # sizeof(float32)

    # TODO: Switch to FixedSizeList<Float32, dim> (Lance's native vector format)
    # once milvus-storage's lance bridge supports automatic schema resolution.
    #
    # Currently BlockingFragmentReader (lance_bridgeimpl.rs) uses the caller's
    # schema as the FFI output schema (self.projection), but Lance reads data in
    # its native types. When Milvus passes FixedSizeBinary but Lance stores
    # FixedSizeList, ImportChunkedArray fails due to n_children mismatch.
    # Storing vectors as FixedSizeBinary avoids the type conflict.
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("value", pa.float32()),
        pa.field("embedding", pa.binary(byte_width)),
    ])

    ids = list(range(start_id, start_id + num_rows))
    values = [float(i) * 1.5 for i in ids]
    embeddings = [
        struct.pack(f"{dim}f", *[float(i) * 0.1 + j for j in range(dim)])
        for i in ids
    ]

    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array(values, type=pa.float32()),
            "embedding": pa.array(embeddings, type=pa.binary(byte_width)),
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
        f"fragment_ids={[f.fragment_id for f in ds.get_fragments()]}"
    )


if __name__ == "__main__":
    main()
