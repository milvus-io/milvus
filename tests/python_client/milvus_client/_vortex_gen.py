"""Standalone helper to generate a Vortex parquet-equivalent file and upload to MinIO.

Invoked by test_milvus_client_external_table.py as a subprocess using the
Python 3.12 sidecar venv at `.venv-vortex/` since `vortex-data>=0.35.0`
requires Python >= 3.11 while the main test venv is 3.10.

Two operating modes:

1. Legacy fixed schema (env-driven) — emitted when VT_INPUT_FROM_STDIN is unset.
   Generates a 3-column table (id int64, value float32, embedding FixedSizeList
   <uint8, dim*4>). Inputs:
       VT_NUM_ROWS, VT_START_ID, VT_DIM, VT_MINIO_KEY,
       MINIO_ADDRESS, MINIO_BUCKET, MINIO_ACCESS_KEY, MINIO_SECRET_KEY.

2. Arbitrary schema via Arrow IPC stdin — when VT_INPUT_FROM_STDIN=1.
   The caller writes a complete pyarrow IPC stream (one or more record
   batches) to this process's stdin; the sidecar deserializes the resulting
   table and hands it straight to `vx.io.write`. The schema, including
   FixedSizeList dimensions and dictionary encoding, is preserved verbatim.

Writes the vortex file bytes to MinIO at the given key. Prints "OK <bytes>".
Exits non-zero on failure.
"""

import io
import os
import struct
import sys
import tempfile

import pyarrow as pa
import pyarrow.ipc as ipc
import vortex as vx
import vortex.io  # noqa: F401 — registers writer
from minio import Minio


def _read_table_from_stdin() -> pa.Table:
    raw = sys.stdin.buffer.read()
    reader = ipc.open_stream(pa.BufferReader(raw))
    return reader.read_all()


def _build_basic_table() -> pa.Table:
    num_rows = int(os.environ["VT_NUM_ROWS"])
    start_id = int(os.environ["VT_START_ID"])
    dim = int(os.environ.get("VT_DIM", "8"))

    ids = list(range(start_id, start_id + num_rows))
    values = [float(i) * 1.5 for i in ids]
    byte_width = dim * 4
    flat = bytearray()
    for i in ids:
        flat.extend(struct.pack(f"{dim}f", *[float(i) * 0.1 + j for j in range(dim)]))

    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array(values, type=pa.float32()),
            "embedding": pa.FixedSizeListArray.from_arrays(
                pa.array(bytes(flat), type=pa.uint8()),
                byte_width,
            ),
        }
    )


def main():
    minio_addr = os.environ["MINIO_ADDRESS"]
    bucket = os.environ["MINIO_BUCKET"]
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    secure = os.environ.get("MINIO_SECURE", "false").lower() == "true"
    key = os.environ["VT_MINIO_KEY"]

    if os.environ.get("VT_INPUT_FROM_STDIN") == "1":
        table = _read_table_from_stdin()
    else:
        table = _build_basic_table()

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

    client = Minio(minio_addr, access_key=access_key, secret_key=secret_key, secure=secure)
    client.put_object(
        bucket,
        key,
        io.BytesIO(data),
        length=len(data),
        content_type="application/octet-stream",
    )
    print(f"OK {len(data)}")


if __name__ == "__main__":
    sys.exit(main())
