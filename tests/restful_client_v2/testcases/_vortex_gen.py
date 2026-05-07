"""Sidecar Vortex data generator for REST external collection tests.

The REST test process usually runs on Python 3.10, while vortex-data requires
Python >= 3.11. The main test sends an Arrow IPC stream to this script through
stdin, and this script writes a Vortex file to MinIO.
"""

import io
import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.ipc as ipc
import vortex.io  # noqa: F401 - registers Vortex writer
from minio import Minio


def _read_table_from_stdin() -> pa.Table:
    raw = sys.stdin.buffer.read()
    reader = ipc.open_stream(pa.BufferReader(raw))
    return reader.read_all()


def main():
    minio_addr = os.environ["MINIO_ADDRESS"]
    bucket = os.environ["MINIO_BUCKET"]
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    secure = os.environ.get("MINIO_SECURE", "false").lower() == "true"
    key = os.environ["VT_MINIO_KEY"]

    table = _read_table_from_stdin()
    with tempfile.NamedTemporaryFile(suffix=".vortex", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        vortex.io.write(table, tmp_path)
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
