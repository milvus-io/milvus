"""Standalone helper to generate a Vortex parquet-equivalent file and upload to MinIO.

Invoked by test_milvus_client_external_table.py::TestExternalTableFormats::test_vortex_format
as a subprocess using the Python 3.12 sidecar venv at `.venv-vortex/` since
`vortex-data>=0.35.0` requires Python >= 3.11 while the main test venv is 3.10.

Inputs via environment variables:
    VT_NUM_ROWS      — row count (int)
    VT_START_ID      — starting id (int)
    VT_DIM           — vector dim (int, default 8)
    MINIO_ADDRESS    — host:port
    MINIO_BUCKET     — bucket name
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY
    VT_MINIO_KEY     — object key (e.g. "external-e2e-core/foo/data.vortex")

Writes the vortex file bytes to MinIO at the given key. Prints "OK <bytes>".
Exits non-zero on failure.
"""
import io
import os
import struct
import sys
import tempfile

import pyarrow as pa
import vortex as vx
import vortex.io  # noqa: F401 — registers writer
from minio import Minio


def main():
    num_rows = int(os.environ["VT_NUM_ROWS"])
    start_id = int(os.environ["VT_START_ID"])
    dim = int(os.environ.get("VT_DIM", "8"))

    minio_addr = os.environ["MINIO_ADDRESS"]
    bucket = os.environ["MINIO_BUCKET"]
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    key = os.environ["VT_MINIO_KEY"]

    ids = list(range(start_id, start_id + num_rows))
    values = [float(i) * 1.5 for i in ids]
    byte_width = dim * 4
    flat = bytearray()
    for i in ids:
        flat.extend(struct.pack(f"{dim}f", *[float(i) * 0.1 + j for j in range(dim)]))

    table = pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "value": pa.array(values, type=pa.float32()),
        "embedding": pa.FixedSizeListArray.from_arrays(
            pa.array(bytes(flat), type=pa.uint8()), byte_width,
        ),
    })

    with tempfile.NamedTemporaryFile(suffix=".vortex", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        vx.io.write(table, tmp_path)
        with open(tmp_path, "rb") as f:
            data = f.read()
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    client = Minio(minio_addr, access_key=access_key, secret_key=secret_key, secure=False)
    client.put_object(
        bucket, key,
        io.BytesIO(data), length=len(data),
        content_type="application/octet-stream",
    )
    print(f"OK {len(data)}")


if __name__ == "__main__":
    sys.exit(main())
