"""Snapshot Import fixtures, inspectors, value checks and model-free mock service.

Run the standalone mock with: python -m common.snapshot_import_common --port 8080
"""

# ruff: noqa: E402
# The mock CLI stays above optional E2E imports so a stdlib-only image can run it.

import argparse
import hashlib
import io
import json
import math
import os
import posixpath
import random
import re
import struct
import sys
import threading
import time
from collections import Counter
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit
from uuid import UUID, uuid4

from common.mock_tei_server import generate_mock_embedding

DIM = 32
SERVICE = "snapshot-function-mock"
SESSION = re.compile(r"fn14_[0-9a-f]{32}")


def embedding_response(path, payload):
    """Implement exactly the two provider response shapes exercised by FN-14."""
    if path == "/embed":
        texts = payload["inputs"]
        assert isinstance(texts, list) and texts and all(isinstance(t, str) for t in texts)
        return [generate_mock_embedding(text, DIM) for text in texts]
    assert path == "/v1/embeddings", "unsupported provider path"
    assert payload["output_dtype"] == "int8" and payload["output_dimension"] == DIM
    assert payload["model"] == "fn14-mock"
    texts = payload["input"]
    assert isinstance(texts, list) and texts and all(isinstance(t, str) for t in texts)
    return {
        "object": "list",
        "model": payload["model"],
        "usage": {"total_tokens": len(texts)},
        "data": [
            {
                "object": "embedding",
                "index": i,
                "embedding": [byte - 128 for byte in hashlib.sha256(text.encode("utf-8")).digest()],
            }
            for i, text in enumerate(texts)
        ],
    }


class SnapshotFunctionMock(ThreadingHTTPServer):
    """Counters are isolated by integration_id, including across parallel tests."""

    def __init__(self, address):
        super().__init__(address, MockHandler)
        self.counts = {}
        self.lock = threading.Lock()

    def record(self, session, provider):
        with self.lock:
            self.counts.setdefault(session, Counter())[provider] += 1

    def counters(self, session):
        with self.lock:
            counts = self.counts.get(session, {})
            return {"tei": counts.get("tei", 0), "voyageai": counts.get("voyageai", 0)}


class MockHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        """Do not log request headers or integration tokens."""

    def respond(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self.respond(200, {"service": SERVICE, "dim": DIM})
        elif self.path.startswith("/_fn14/") and SESSION.fullmatch(self.path.removeprefix("/_fn14/")):
            session = self.path.removeprefix("/_fn14/")
            self.respond(200, self.server.counters(session))
        else:
            self.respond(404, {"error": "unknown control route"})

    def do_POST(self):
        token = self.headers.get("Authorization", "").removeprefix("Bearer ")
        session = token.split("|", 1)[0]
        if not SESSION.fullmatch(session):
            self.respond(401, {"error": "FN-14 integration_id required"})
            return
        if self.path not in ("/embed", "/v1/embeddings"):
            self.respond(404, {"error": "unknown provider route"})
            return
        self.server.record(session, "tei" if self.path == "/embed" else "voyageai")
        try:
            size = int(self.headers.get("Content-Length", "0"))
            assert 0 < size <= 1024 * 1024, "invalid request size"
            payload = json.loads(self.rfile.read(size))
            result = embedding_response(self.path, payload)
        except (AssertionError, KeyError, TypeError, ValueError) as exc:
            self.respond(400, {"error": str(exc)})
            return
        self.respond(200, result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    with SnapshotFunctionMock((args.host, args.port)) as server:
        server.serve_forever()


import fastavro
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
import urllib3
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.external_table_common import build_external_spec, get_minio_config, new_minio_client
from fastavro import reader, schemaless_reader
from minio.commonconfig import CopySource
from minio.credentials import StaticProvider
from minio.error import MinioAdminException, S3Error
from minio.minioadmin import MinioAdmin
from pymilvus import DataType, Function, FunctionType
from shapely import wkt
from utils.util_log import test_log as log

# Snapshot physical readers; independent of the Import Reshard test suite.


def object_bytes(storage, bucket, key):
    response = storage.get_object(bucket, key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def _data_key(base, path):
    if path.startswith(base + "/_data/"):
        return path
    # Ordinary writer manifests store a filename relative to base/_data, not
    # minio.rootPath. Do not guess another root or enumerate unreferenced files.
    assert path and "/" not in path and path not in (".", ".."), f"unexpected data filename: {path}"
    return base + "/_data/" + path


def _read_table(storage, bucket, key, format_name, tmp_path):
    data = object_bytes(storage, bucket, key)
    if format_name == "parquet":
        return pq.read_table(pa.BufferReader(data))
    if format_name == "vortex":
        # Unique local files live under pytest's own temporary directory.
        import tempfile

        import vortex.io

        with tempfile.NamedTemporaryFile(dir=tmp_path, suffix=".vortex") as stream:
            stream.write(data)
            stream.flush()
            return vortex.io.read_url("file:" + stream.name).to_arrow_table()
    raise AssertionError(f"unsupported column-group format: {format_name}")


def read_segment_rows(storage, bucket, manifest_path, field_ids, row_count, vector_dim, tmp_path):
    """Reassemble column groups by position; preserve manifest/file/row-group order."""
    import fastavro

    location = json.loads(manifest_path)
    base, version = location["base_path"], location["ver"]
    assert isinstance(base, str) and base and not base.startswith("/") and "://" not in base
    assert isinstance(version, int) and not isinstance(version, bool) and version > 0
    key = base + f"/_metadata/manifest-{version}.avro"
    manifest_data = object_bytes(storage, bucket, key)
    assert manifest_data.startswith(b"Obj\x01"), "expected an Avro container manifest"
    records = list(fastavro.reader(io.BytesIO(manifest_data)))
    assert len(records) == 1, "expected one manifest record"
    manifest = records[0]
    assert not manifest.get("delta_logs"), "fresh Import output must not contain delete logs"
    names = {str(field_id): name for name, field_id in field_ids.items()}
    assert len(names) == len(field_ids), "duplicate field IDs"
    columns = {}
    for group in manifest["column_groups"]:
        selected = [column for column in group["columns"] if column in names]
        if not selected:
            continue
        assert len(set(selected)) == len(selected) and not set(selected) & columns.keys(), "duplicate column group"
        assert group["format"] in ("parquet", "vortex"), f"unsupported column-group format: {group['format']}"
        values = {column: [] for column in selected}
        for file in group["files"]:
            table = _read_table(storage, bucket, _data_key(base, file["path"]), group["format"], tmp_path)
            start, end = file["start_index"], file["end_index"]
            assert isinstance(start, int) and isinstance(end, int) and 0 <= start < end <= table.num_rows, (
                "invalid manifest row range"
            )
            assert set(selected) <= set(table.column_names), "missing physical column"
            part = table.slice(start, end - start)
            for column in selected:
                values[column].extend(part[column].to_pylist())
        assert all(len(column) == row_count for column in values.values()), "column-group row count differs"
        columns.update(values)
    assert set(columns) == set(names), "manifest is missing a requested field"
    rows = [{names[column]: values[i] for column, values in columns.items()} for i in range(row_count)]
    for row in rows:
        vector = row["vector"]
        if isinstance(vector, bytes):
            assert len(vector) == vector_dim * 4, "invalid physical FloatVector width"
            row["vector"] = list(struct.unpack(f"<{vector_dim}f", vector))
        else:
            assert isinstance(vector, list) and len(vector) == vector_dim, "invalid physical FloatVector shape"
    return rows


def _export_request(url, action, payload, db_name, api_key):
    """Use the public REST export endpoints not yet wrapped by MilvusClient."""
    headers = {"DB-Name": db_name}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    response = requests.post(
        f"{url.rstrip('/')}/v2/vectordb/jobs/snapshot/export{action}",
        json={"dbName": db_name, **payload},
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    result = response.json()
    assert result["code"] == 0, result
    return result["data"]


def export_snapshot(url, collection, snapshot, prefix, db_name, api_key, external_spec=None):
    payload = {"collectionName": collection, "snapshotName": snapshot, "targetS3Path": prefix}
    if external_spec is not None:
        payload["externalSpec"] = external_spec
    result = _export_request(url, "", payload, db_name, api_key)
    job_id = str(result["jobId"])
    assert int(job_id) > 0, result
    log.info(f"Snapshot export job_id={job_id}, prefix={prefix}")
    return job_id


def wait_for_snapshot_export(url, job_id, db_name, api_key, timeout=300):
    deadline = time.monotonic() + timeout
    progress = None
    while time.monotonic() < deadline:
        progress = _export_request(url, "/describe", {"jobId": str(job_id)}, db_name, api_key)
        assert str(progress["jobId"]) == str(job_id), progress
        if progress["state"] in ("ExportSnapshotCompleted", "ExportSnapshotFailed"):
            return progress
        time.sleep(1)
    pytest.fail(f"Snapshot export {job_id} did not finish within {timeout}s: {progress}")


def _private_export_prefix(prefix):
    """Only accept prefixes generated by the snapshot fixtures in this suite."""
    assert re.fullmatch(
        r"snapshot_import_(?:sm02|fn01|fn02|fn03|fn04|fn05|fn06|fn07|fn08|fn09|fn10|fn11|fn12|fn13|fn14|fn16|fn17)_[0-9a-f]{32}/export_[ab]/",
        prefix,
    ), prefix
    return prefix


def remove_snapshot_export(store, bucket, prefix):
    """Delete only objects under one validated, test-owned export prefix."""
    prefix = _private_export_prefix(prefix)
    for obj in list(store.list_objects(bucket, prefix=prefix, recursive=True)):
        assert obj.object_name.startswith(prefix)
        store.remove_object(bucket, obj.object_name)
    assert not list(store.list_objects(bucket, prefix=prefix, recursive=True)), prefix


def relocate_snapshot_bundle(store, bucket, metadata_uri, source_prefix, target_prefix, expected_rows):
    """Copy the published small smoke-test bundle byte-for-byte, then remove the old copy."""
    source_prefix = _private_export_prefix(source_prefix)
    target_prefix = _private_export_prefix(target_prefix)
    assert source_prefix != target_prefix
    assert not list(store.list_objects(bucket, prefix=target_prefix, recursive=True))
    uri = urlsplit(metadata_uri)
    assert uri.scheme in ("s3", "minio") and uri.netloc and not uri.query and not uri.fragment
    assert uri.username is None and uri.password is None
    uri_bucket, metadata_key = unquote(uri.path.lstrip("/")).split("/", 1)
    assert uri_bucket == bucket and metadata_key.startswith(source_prefix)

    snapshot_root, anchor, _ = metadata_key.rpartition("/snapshots/")
    assert anchor and (snapshot_root + "/").startswith(source_prefix)
    # Completed is published before the exporter removes its private staging copy.
    # This temporary object is not part of the bundle and can disappear during listing.
    staging_key = snapshot_root + "/_staging/metadata.json"
    objects = [
        obj
        for obj in store.list_objects(bucket, prefix=source_prefix, recursive=True)
        if obj.object_name != staging_key
    ]
    keys = {obj.object_name for obj in objects}
    assert metadata_key in keys and all(key.startswith(source_prefix) for key in keys)
    metadata = json.loads(_read_object(store, bucket, metadata_key))
    assert metadata.get("layout") in ("SnapshotLayoutSelfContained", 2)
    descriptors = metadata["manifest_list"]
    assert descriptors and len(descriptors) == len(set(descriptors)) and set(descriptors) <= keys
    segments = [
        _segment_header(_read_object(store, bucket, key), int(metadata["format_version"])) for key in descriptors
    ]
    assert {s["segment_id"] for s in segments} == {int(i) for i in metadata["segment_ids"]}
    data_segments = [s for s in segments if s["num_of_rows"] > 0]
    assert data_segments and all(s["storage_version"] == 3 for s in data_segments)
    assert sum(s["num_of_rows"] for s in data_segments) == expected_rows
    manifests = {int(m["segment_id"]): json.loads(m["manifest"]) for m in metadata["storagev2_manifest_list"]}
    for segment in data_segments:
        manifest = manifests[segment["segment_id"]]
        assert manifest["base_path"].startswith(snapshot_root + "/files/") and manifest["ver"] > 0
        assert any(key.startswith(manifest["base_path"].rstrip("/") + "/") for key in keys), manifest

    # Copy every published object, not only metadata/manifests. Do not rewrite file contents.
    copied = {}
    for obj in objects:
        source_key = obj.object_name
        target_key = target_prefix + source_key.removeprefix(source_prefix)
        digest = hashlib.sha256(_read_object(store, bucket, source_key)).hexdigest()
        store.copy_object(bucket, target_key, CopySource(bucket, source_key))
        assert store.stat_object(bucket, target_key).size == obj.size, target_key
        assert hashlib.sha256(_read_object(store, bucket, target_key)).hexdigest() == digest, target_key
        copied[target_key] = digest
        log.info(f"Snapshot bundle copy: {source_key} -> {target_key}, bytes={obj.size}, sha256={digest}")
    assert {
        obj.object_name for obj in store.list_objects(bucket, prefix=target_prefix, recursive=True)
    } == copied.keys()

    # Keep the source until the complete destination has been verified.
    remove_snapshot_export(store, bucket, source_prefix)
    for key, digest in copied.items():
        assert hashlib.sha256(_read_object(store, bucket, key)).hexdigest() == digest, key
    moved_key = target_prefix + metadata_key.removeprefix(source_prefix)
    return uri._replace(path=f"/{quote(bucket, safe='')}/{quote(moved_key, safe='/')}").geturl()


def _read_object(store, bucket, key):
    response = store.get_object(bucket, key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def _snapshot_read_path(path, metadata, metadata_key, bucket, layout):
    """Resolve fixture reads without rewriting metadata or captured object bytes."""
    assert path and "://" not in path and ".." not in path.split("/")
    if layout == "referenced":
        return path

    original_key = metadata["snapshot_info"]["s3_location"]
    if "://" in original_key:
        uri = urlsplit(original_key)
        assert uri.scheme in ("s3", "minio") and uri.netloc and not uri.query and not uri.fragment
        assert uri.username is None and uri.password is None
        original_bucket, original_key = unquote(uri.path.lstrip("/")).split("/", 1)
        assert original_bucket == bucket, "Inspector requires a same-bucket export"

    anchor = r"(?:(.*)/)?snapshots/[0-9]+/metadata/[0-9]+\.json"
    old_match = re.fullmatch(anchor, original_key)
    new_match = re.fullmatch(anchor, metadata_key)
    assert old_match and new_match, "SelfContained fixture requires a terminal metadata anchor"
    old_root, new_root = old_match[1] or "", new_match[1] or ""
    old_prefix = old_root + "/" if old_root else ""
    new_prefix = new_root + "/" if new_root else ""
    assert path.startswith(old_prefix), "Snapshot object is outside the original bundle root"
    return new_prefix + path.removeprefix(old_prefix)


def read_snapshot_objects(store, bucket, metadata_uri, layout="referenced"):
    """Read the exact metadata and segment descriptors; do not follow live metadata."""
    uri = urlsplit(metadata_uri)
    assert uri.scheme in ("s3", "minio") and uri.netloc and not uri.query and not uri.fragment
    assert uri.username is None and uri.password is None
    uri_bucket, key = unquote(uri.path.lstrip("/")).split("/", 1)
    assert uri_bucket == bucket, (uri_bucket, bucket)
    objects = {key: _read_object(store, bucket, key)}
    metadata = json.loads(objects[key])
    layouts = {"referenced": ("SnapshotLayoutReferenced", 1), "self_contained": ("SnapshotLayoutSelfContained", 2)}
    assert metadata.get("layout") in layouts[layout], metadata.get("layout")
    paths = metadata.get("manifest_list", [])
    assert paths and len(paths) == len(set(paths)), "Snapshot must contain unique segment descriptors"
    for path in paths:
        path = _snapshot_read_path(path, metadata, key, bucket, layout)
        objects[path] = _read_object(store, bucket, path)
    return objects


def read_referenced_snapshot(store, bucket, metadata_uri):
    """Return metadata, decoded segment headers and original object bytes."""
    objects = read_snapshot_objects(store, bucket, metadata_uri)
    _, metadata_key = unquote(urlsplit(metadata_uri).path.lstrip("/")).split("/", 1)
    metadata = json.loads(objects[metadata_key])
    segments = [_segment_header(objects[path], int(metadata["format_version"])) for path in metadata["manifest_list"]]
    assert {s["segment_id"] for s in segments} == {int(i) for i in metadata["segment_ids"]}
    manifests = {int(m["segment_id"]): m["manifest"] for m in metadata.get("storagev2_manifest_list", [])}
    for segment in segments:
        if segment["num_of_rows"] > 0:
            manifest = json.loads(manifests[segment["segment_id"]])
            assert manifest["base_path"] and manifest["ver"] > 0, manifest
    return metadata, segments, objects


def read_snapshot_inventory(store, bucket, metadata_uri, layout="referenced", pk_type=DataType.INT64):
    """Inspect the small fixture's captured V3 data and plaintext legacy L0.

    This is not a general snapshot reader: V3 L0 needs a separate manifest-aware
    inspector. Refuse that fixture instead of treating pathless summaries as empty.
    Returned object bytes include metadata, descriptors and inspected delete files.
    """
    objects = read_snapshot_objects(store, bucket, metadata_uri, layout)
    _, metadata_key = unquote(urlsplit(metadata_uri).path.lstrip("/")).split("/", 1)
    metadata = json.loads(objects[metadata_key])
    segments = [
        _segment_header(
            objects[_snapshot_read_path(path, metadata, metadata_key, bucket, layout)],
            int(metadata["format_version"]),
            include_logs=True,
        )
        for path in metadata["manifest_list"]
    ]
    assert len({s["segment_id"] for s in segments}) == len(segments)
    assert {s["segment_id"] for s in segments} == {int(i) for i in metadata["segment_ids"]}
    data_segments = [s for s in segments if s["segment_level"] != 1 and s["num_of_rows"] > 0]
    assert data_segments and all(s["storage_version"] == 3 for s in data_segments)
    assert len({s["channel_name"] for s in data_segments}) == 1, "Fixture requires one source channel"
    manifests = snapshot_manifest_versions(metadata)
    assert all(s["segment_id"] in manifests for s in data_segments)
    deleted = set()
    for delta in (s for s in segments if s["segment_level"] == 1):
        assert delta["storage_version"] == 0, "Fixture inspection requires plaintext V1 L0, not a V3 L0 manifest"
        applicable = any(
            s["channel_name"] == delta["channel_name"] and delta["partition_id"] in (-1, s["partition_id"])
            for s in data_segments
        )
        for field in delta["deltalog_files"]:
            for entry in field["binlogs"]:
                path = _snapshot_read_path(entry["log_path"], metadata, metadata_key, bucket, layout)
                objects[path] = _read_object(store, bucket, path)
                records = _legacy_delete_records(objects[path], pk_type)
                timestamps = [ts for _, ts in records]
                timestamp_range = (min(timestamps), max(timestamps)) if timestamps else None
                log.info(
                    f"Snapshot L0 segment={delta['segment_id']}, channel={delta['channel_name']}, "
                    f"partition={delta['partition_id']}, applicable={applicable}, path={path}, "
                    f"pk_type={pk_type.name}, records={len(records)}, "
                    f"timestamp_range={timestamp_range}"
                )
                if applicable:
                    deleted.update(pk for pk, _ in records)
    return metadata, segments, objects, deleted


def read_snapshot_partition_inventory(store, bucket, metadata_uri, layout="referenced", case_id="SI-FN-05"):
    """Keep each captured legacy L0's source scope; never flatten cross-partition deletes."""
    objects = read_snapshot_objects(store, bucket, metadata_uri, layout)
    _, metadata_key = unquote(urlsplit(metadata_uri).path.lstrip("/")).split("/", 1)
    metadata = json.loads(objects[metadata_key])
    segments = [
        _segment_header(
            objects[_snapshot_read_path(path, metadata, metadata_key, bucket, layout)],
            int(metadata["format_version"]),
            include_logs=True,
        )
        for path in metadata["manifest_list"]
    ]
    assert len({s["segment_id"] for s in segments}) == len(segments)
    assert {s["segment_id"] for s in segments} == {int(i) for i in metadata["segment_ids"]}
    data = [s for s in segments if s["segment_level"] != 1 and s["num_of_rows"] > 0]
    assert data and all(s["storage_version"] == 3 and s["channel_name"] for s in data)
    manifests = snapshot_manifest_versions(metadata)
    assert all(s["segment_id"] in manifests for s in data)
    deletes = {}
    for delta in (s for s in segments if s["segment_level"] == 1):
        assert delta["storage_version"] == 0, f"{case_id} inspector requires plaintext V1 L0"
        assert delta["channel_name"], "Captured L0 must identify its source channel"
        scope = (delta["channel_name"], delta["partition_id"])
        records = deletes.setdefault(scope, set())
        for field in delta["deltalog_files"]:
            for entry in field["binlogs"]:
                path = _snapshot_read_path(entry["log_path"], metadata, metadata_key, bucket, layout)
                objects[path] = _read_object(store, bucket, path)
                records.update(_legacy_delete_records(objects[path], DataType.INT64))
    log.info(
        f"{case_id} captured data scopes/rows={[(s['channel_name'], s['partition_id'], s['num_of_rows']) for s in data]}"
    )
    log.info(f"{case_id} captured L0 scope/PK/timestamp={deletes}")
    return metadata, data, deletes, objects


def read_snapshot_partition_rows(
    store, bucket, metadata_uri, field_ids, tmp_path, layout="referenced", case_id="SI-FN-07"
):
    """Read exact captured manifests and return physical rows with their source partition IDs."""

    metadata, data, deletes, objects = read_snapshot_partition_inventory(
        store, bucket, metadata_uri, layout, case_id=case_id
    )
    assert not deletes, f"{case_id} requires a snapshot without L0"
    _, metadata_key = unquote(urlsplit(metadata_uri).path.lstrip("/")).split("/", 1)
    manifests = snapshot_manifest_versions(metadata)
    rows = []
    for segment in data:
        base, version = manifests[segment["segment_id"]]
        base = _snapshot_read_path(base, metadata, metadata_key, bucket, layout)
        physical = read_segment_rows(
            store,
            bucket,
            json.dumps({"base_path": base, "ver": version}),
            field_ids,
            segment["num_of_rows"],
            32,
            tmp_path,
        )
        rows.extend({**row, "partition_id": segment["partition_id"]} for row in physical)
    return metadata, rows, objects


def int64_partition_slot(value, partition_count):
    """Independent Murmur3 implementation for Milvus Int64 routing, not Python hash()."""
    from sklearn.utils import murmurhash3_32

    assert type(value) is int and -(2**63) <= value < 2**63 and partition_count > 0
    digest = murmurhash3_32(value.to_bytes(8, "little", signed=True), seed=0, positive=True)
    return (digest & 0x7FFFFFFF) % partition_count


def read_snapshot_commit_timestamps(store, bucket, metadata_uri, layout="referenced"):
    """Require nonzero captured commit timestamps in the no-L0 FN-04 fixture."""
    _, segments, objects, deleted = read_snapshot_inventory(store, bucket, metadata_uri, layout)
    assert not deleted and all(s["segment_level"] != 1 for s in segments), "FN-04 main fixture must have no L0"
    timestamps = {
        s["segment_id"]: (s["num_of_rows"], s.get("commit_timestamp", 0)) for s in segments if s["num_of_rows"] > 0
    }
    assert timestamps and all(ts > 0 for _, ts in timestamps.values()), timestamps
    log.info(f"Captured {layout} rows/commit_ts={timestamps}")
    return timestamps, objects


def snapshot_manifest_versions(metadata):
    """Map source segment IDs to their exact packed manifest base/version."""
    manifests = {}
    for item in metadata.get("storagev2_manifest_list", []):
        manifest = json.loads(item["manifest"])
        assert manifest["base_path"] and manifest["ver"] > 0, manifest
        segment_id = int(item["segment_id"])
        assert segment_id not in manifests
        manifests[segment_id] = (manifest["base_path"], manifest["ver"])
    return manifests


def assert_snapshot_objects_unchanged(store, bucket, objects):
    for key, original in objects.items():
        assert _read_object(store, bucket, key) == original, f"Captured snapshot object changed: {key}"


def _legacy_delete_records(data, pk_type=DataType.INT64):
    """Decode V1 JSON or two-column Parquet without coercing PKs or timestamps."""
    assert pk_type in (DataType.INT64, DataType.VARCHAR), pk_type
    pk_class = int if pk_type == DataType.INT64 else str
    assert len(data) >= 4 and int.from_bytes(data[:4], "little") == 0xFFFABC, "Invalid V1 deltalog magic"
    offset, event_count, records = 4, 0, []
    while offset < len(data):
        assert len(data) - offset >= 17, "Truncated V1 event header"
        length = int.from_bytes(data[offset + 9 : offset + 13], "little")
        assert 17 <= length <= len(data) - offset, "Invalid V1 event length"
        event_type = data[offset + 8]
        if event_count == 0:
            assert event_type == 0 and length >= 81, "Invalid V1 descriptor"
        else:
            assert event_type == 2 and length > 33, "Invalid V1 delete event"
            table = pq.read_table(io.BytesIO(data[offset + 33 : offset + length]))
            assert table.num_columns in (1, 2)
            if table.num_columns == 1:
                entries = [json.loads(value) for value in table.column(0).to_pylist()]
                assert all(entry["pkType"] == int(pk_type) for entry in entries), "Unexpected delete PK type"
                pairs = [(entry["pk"], entry["ts"]) for entry in entries]
            else:
                pairs = list(zip(table.column(0).to_pylist(), table.column(1).to_pylist()))
            assert all(type(pk) is pk_class and type(ts) is int and 0 < ts < 2**64 for pk, ts in pairs)
            records.extend(pairs)
        offset += length
        event_count += 1
    assert event_count >= 2, "V1 deltalog has no delete payload"
    return records


def _segment_header(data, format_version, include_logs=False):
    """Read the V2-V5 binary prefix, or the V6 self-describing Avro record.

    include_logs reads through deltalog_files using the version-specific prefix;
    later fields are not needed for these fixture checks.
    """
    if format_version == 6:
        assert data.startswith(b"Obj\x01"), "Snapshot V6 requires an Avro container"
        records = list(reader(io.BytesIO(data)))
        assert len(records) == 1, "Expected one segment per descriptor"
        return records[0]
    assert format_version in (2, 3, 4, 5), f"Unsupported snapshot format: {format_version}"
    position = {
        "type": "record",
        "name": "AvroMsgPosition",
        "fields": [
            {"name": name, "type": dtype}
            for name, dtype in (
                ("channel_name", "string"),
                ("msg_id", "bytes"),
                ("msg_group", "string"),
                ("timestamp", "long"),
            )
        ],
    }
    schema = {
        "type": "record",
        "name": "ManifestHeader",
        "fields": [
            {"name": name, "type": dtype}
            for name, dtype in (
                ("segment_id", "long"),
                ("partition_id", "long"),
                ("segment_level", "long"),
                ("channel_name", "string"),
                ("num_of_rows", "long"),
                ("start_position", position),
                ("dml_position", "AvroMsgPosition"),
                ("storage_version", "long"),
                ("is_sorted", "boolean"),
            )
        ],
    }
    if include_logs:
        if format_version >= 3:
            schema["fields"].append({"name": "commit_timestamp", "type": "long"})
        if format_version >= 5:
            schema["fields"].append({"name": "manifest_has_index", "type": "boolean"})
        binlog = {
            "type": "record",
            "name": "AvroBinlog",
            "fields": [
                {"name": name, "type": "string" if name == "log_path" else "long"}
                for name in (
                    "entries_num",
                    "timestamp_from",
                    "timestamp_to",
                    "log_path",
                    "log_size",
                    "log_id",
                    "memory_size",
                )
            ],
        }
        field_log = {"type": "record", "name": "AvroFieldBinlog", "fields": [{"name": "field_id", "type": "long"}]}
        if format_version >= 4:
            field_log["fields"].extend(
                [
                    {"name": "child_fields", "type": {"type": "array", "items": "long"}},
                    {"name": "format", "type": "string"},
                ]
            )
        field_log["fields"].append({"name": "binlogs", "type": {"type": "array", "items": binlog}})
        schema["fields"].extend(
            [
                {"name": "binlog_files", "type": {"type": "array", "items": field_log}},
                {"name": "deltalog_files", "type": {"type": "array", "items": "AvroFieldBinlog"}},
            ]
        )
    return schemaless_reader(io.BytesIO(data), schema)


# Private resources and public Import operations for the final snapshot cases.


def plain_rows(pks, marker_offset=1000):
    return [{"id": pk, "marker": marker_offset + pk, "vec": [(pk * 16 + j) / 4096 for j in range(16)]} for pk in pks]


class BatchContext:
    """One test owns every collection, snapshot and copied object it creates."""

    def __init__(self, client, store, bucket, node, db="default"):
        self.client, self.store, self.bucket = client, store, bucket
        self.prefix = f"snapshot_batch_{uuid4().hex}/"
        self.name = "snapshot_batch_" + uuid4().hex[:16]
        self.collections, self.snapshots, self.jobs = [], [], set()
        self.secret_tokens, self.events = [], []
        self.uncertain = False
        self.node = node
        self.url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        self.db = db

    def redact(self, value):
        if isinstance(value, dict):
            return {self.redact(key): self.redact(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [self.redact(item) for item in value]
        if isinstance(value, str):
            for token in self.secret_tokens:
                value = value.replace(token, "<redacted>")
        return value

    def record(self, kind, **data):
        self.events.append(self.redact({"kind": kind, **data}))

    def create(self, suffix, extra=(), properties=None, partitions=()):
        name = self.name + "_" + suffix
        schema = self.client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=16)
        schema.add_field("marker", DataType.INT64)
        for field_name, dtype, options in extra:
            schema.add_field(field_name, dtype, **options)
        indexes = self.client.prepare_index_params()
        indexes.add_index("vec", index_name="vec_idx", index_type="FLAT", metric_type="L2")
        self.collections.append(name)
        self.client.create_collection(
            name,
            schema=schema,
            shards_num=1,
            consistency_level="Strong",
            properties=properties or {},
        )
        self.client.create_index(name, indexes)
        for partition in partitions:
            self.client.create_partition(name, partition)
        return name

    def snapshot(self, source, layout="referenced", protect=3600):
        name = self.name + "_snap_" + str(len(self.snapshots))
        self.snapshots.append((source, name))
        self.client.create_snapshot(name, source, db_name=self.db, compaction_protection_seconds=protect)
        uri = self.client.describe_snapshot(name, source, db_name=self.db).s3_location
        if layout == "self_contained":
            location = urlsplit(uri)
            export = location._replace(path=f"/{self.bucket}/{self.prefix}export_{len(self.snapshots)}/").geturl()
            self.uncertain = True
            job = export_snapshot(self.url, source, name, export, self.db, cf.param_info.param_token)
            result = wait_for_snapshot_export(self.url, job, self.db, cf.param_info.param_token)
            self.uncertain = False
            assert result["state"] == "ExportSnapshotCompleted", self.redact(result)
            uri = result["snapshotMetadataURI"]
        self.record("snapshot", source=source, layout=layout, uri=uri)
        return uri

    def put(self, key, data):
        assert key.startswith(self.prefix), key
        if not isinstance(data, bytes):
            data = json.dumps(data).encode()
        self.store.put_object(self.bucket, key, io.BytesIO(data), len(data))

    def metadata(self, uri):
        bucket, key = unquote(urlsplit(uri).path.lstrip("/")).split("/", 1)
        assert bucket == self.bucket
        return json.loads(object_bytes(self.store, bucket, key))

    def metadata_copy(self, uri, label, mutate):
        original = self.metadata(uri)
        changed = deepcopy(original)
        mutate(changed)
        old_key = unquote(urlsplit(uri).path.lstrip("/")).split("/", 1)[1]
        anchor, separator, suffix = old_key.rpartition("/snapshots/")
        assert separator and anchor and suffix
        key = self.prefix + label + "/snapshots/" + suffix
        self.put(key, changed)
        return urlsplit(uri)._replace(path=f"/{self.bucket}/{key}").geturl()

    def request(self, target, uri, options=None, files=None, partition=None, label="import"):
        opts = {"backup": "true", "source_type": "snapshot", "auto_commit": "true", "timeout": "90s"}
        if options is not None:
            opts = options
        payload = {
            "collectionName": target,
            "dbName": self.db,
            "files": [[uri]] if files is None else files,
            "options": opts,
        }
        if partition is not None:
            payload["partitionName"] = partition
        headers = {"DB-Name": self.db}
        if cf.param_info.param_token:
            headers["Authorization"] = "Bearer " + cf.param_info.param_token
        self.uncertain = True
        response = requests.post(
            self.url + "/v2/vectordb/jobs/import/create", json=payload, headers=headers, timeout=30
        )
        response.raise_for_status()
        result = response.json()
        job = result.get("data", {}).get("jobId")
        if result.get("code") == 0:
            assert job, self.redact(result)
            self.jobs.add(str(job))
        self.uncertain = False
        log.info(f"SNAPSHOT_BATCH node={self.node}, label={label}, job_id={job}, code={result.get('code')}")
        self.record("request", label=label, target=target, response=result)
        return result

    def wait(self, job, timeout=180):
        headers = {"DB-Name": self.db}
        if cf.param_info.param_token:
            headers["Authorization"] = "Bearer " + cf.param_info.param_token
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            response = requests.post(
                self.url + "/v2/vectordb/jobs/import/describe",
                json={"dbName": self.db, "jobId": str(job)},
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            result = response.json()
            assert result["code"] == 0, self.redact(result)
            data = result["data"]
            if data["state"] in ("Completed", "Failed"):
                self.jobs.discard(str(job))
                self.record("terminal", job_id=str(job), progress=data)
                return data
            time.sleep(1)
        pytest.fail(f"Import {job} did not become terminal within {timeout}s")

    def import_ok(self, target, uri, rows, options=None, label="positive"):
        result = self.request(target, uri, options=options, label=label)
        assert result["code"] == 0, self.redact(result)
        progress = self.wait(result["data"]["jobId"])
        assert progress["state"] == "Completed" and progress["importedRows"] == rows, self.redact(progress)
        return progress

    def import_rejected(self, target, uri, *, options=None, files=None, partition=None, label, reason=()):
        result = self.request(target, uri, options=options, files=files, partition=partition, label=label)
        if result["code"] == 0:
            progress = self.wait(result["data"]["jobId"])
            assert progress["state"] == "Failed", self.redact(progress)
            message = progress.get("reason", "")
        else:
            message = result.get("message", "")
        assert message, self.redact(result)
        if reason:
            assert any(token.lower() in message.lower() for token in reason), self.redact(message)
        self.record("rejected", label=label, reason=message)

    def assert_rows(self, name, rows, fields=None):
        expected = {row["id"]: row for row in rows}
        assert len(expected) == len(rows)
        fields = list(rows[0]) if fields is None and rows else fields or ["id", "marker", "vec"]
        self.client.load_collection(name)
        deadline = time.monotonic() + 120
        while True:
            actual = self.client.query(
                name, filter="", output_fields=fields, limit=len(rows) + 1, consistency_level="Strong", timeout=30
            )
            if len(actual) >= len(rows) or time.monotonic() >= deadline:
                break
            time.sleep(1)
        assert len(actual) == len(rows), (name, len(actual), len(rows))
        assert len({row["id"] for row in actual}) == len(actual)
        assert {row["id"] for row in actual} == set(expected)
        for row in actual:
            assert set(row) == set(fields)
            for field in fields:
                if field == "vec":
                    np.testing.assert_array_equal(row[field], np.asarray(expected[row["id"]][field], dtype=np.float32))
                else:
                    assert row[field] == expected[row["id"]][field], (
                        row["id"],
                        field,
                        row[field],
                        expected[row["id"]][field],
                    )
        assert self.client.query(name, filter="", output_fields=["count(*)"])[0]["count(*)"] == len(rows)

    def reload_assert(self, name, rows, fields=None):
        self.assert_rows(name, rows, fields)
        self.client.release_collection(name)
        self.client.load_collection(name)
        self.assert_rows(name, rows, fields)

    def close(self):
        try:
            self._cleanup()
        finally:
            self._save_evidence()

    def _save_evidence(self):
        output = os.getenv("SNAPSHOT_BATCH_EVIDENCE")
        if output:
            path = Path(output)
            path.mkdir(parents=True, exist_ok=True)
            (path / (self.node.replace("/", "_").replace(":", "_") + ".json")).write_text(
                json.dumps(self.events, indent=2)
            )

    def _cleanup(self):
        if self.jobs or self.uncertain:
            log.warning(f"Unknown active work; preserving private resources for {self.node}")
            return
        for source, snapshot in reversed(self.snapshots):
            if self.client.has_collection(source):
                if snapshot in self.client.list_snapshots(source, db_name=self.db):
                    self.client.drop_snapshot(snapshot, source, db_name=self.db)
        for name in reversed(self.collections):
            if self.client.has_collection(name):
                self.client.drop_collection(name)
        for obj in list(self.store.list_objects(self.bucket, prefix=self.prefix, recursive=True)):
            assert obj.object_name.startswith(self.prefix)
            self.store.remove_object(self.bucket, obj.object_name)


class SnapshotBatchBase(TestMilvusClientV2Base):
    """Independent collections; no global RBAC or unrelated collection cleanup."""

    skip_global_role_cleanup = True

    def setup_method(self, method):
        super().setup_method(method)
        self.tear_down_collection_names, self.resource_group_list = [], []

    @pytest.fixture
    def batch(self, db_name, minio_host, minio_bucket, request):
        client = self._client(db_name=db_name)
        cfg = get_minio_config(minio_host, minio_bucket)
        context = BatchContext(client, new_minio_client(cfg), cfg["bucket"], request.node.name, db_name)
        try:
            yield context
        finally:
            try:
                context.close()
            finally:
                client.close()


def avro_manifest(store, bucket, base, version):
    key = base.rstrip("/") + f"/_metadata/manifest-{version}.avro"
    records = list(fastavro.reader(io.BytesIO(object_bytes(store, bucket, key))))
    assert len(records) == 1
    return records[0]


# FN-16 private MinIO source identities and read-only storage checks.


class _RedactedSpec(str):
    """Serialize normally on the wire, but redact nested pytest argument representations."""

    def __repr__(self):
        return "<redacted external_spec>"


@dataclass(repr=False)
class ExternalSnapshotBucket:
    store: object
    reader: object
    bucket: str
    prefix: str
    read_spec: str
    write_spec: str


@contextmanager
def external_snapshot_bucket(cfg, tmp_path, retain):
    """Own a new bucket/user/policy; preserve all dependencies if a job may be active."""
    store = new_minio_client(cfg)
    identity = uuid4().hex
    bucket = f"snapshot-import-fn16-{identity}"
    prefix = f"snapshot_import_fn16_{identity}/export_a/"
    user, secret, policy = f"fn16{identity[:16]}", uuid4().hex, f"fn16-ro-{identity}"
    admin = MinioAdmin(
        cfg["address"],
        StaticProvider(cfg["access_key"], cfg["secret_key"]),
        secure=cfg["secure"],
        http_client=urllib3.PoolManager(timeout=urllib3.Timeout(connect=10, read=30), retries=False),
    )
    policy_file = tmp_path / "source-readonly-policy.json"
    policy_file.write_text(
        json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                        "Resource": [f"arn:aws:s3:::{bucket}"],
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject"],
                        "Resource": [f"arn:aws:s3:::{bucket}/*"],
                    },
                ],
            }
        )
    )
    bucket_attempted = user_attempted = policy_attempted = False
    try:
        bucket_attempted = True
        store.make_bucket(bucket)
        policy_attempted = True
        admin.policy_add(policy, str(policy_file))
        user_attempted = True
        admin.user_add(user, secret)
        admin.policy_set(policy, user=user)
        read_cfg = {**cfg, "bucket": bucket, "access_key": user, "secret_key": secret}
        yield ExternalSnapshotBucket(
            store,
            new_minio_client(read_cfg),
            bucket,
            prefix,
            _RedactedSpec(build_external_spec(read_cfg)),
            _RedactedSpec(build_external_spec(cfg)),
        )
    finally:
        if retain():
            log.warning(f"Export/Import may be active; retaining FN-16 source bucket={bucket} and its reader")
        else:
            _cleanup_source_resources(
                store, bucket, prefix, admin, user, policy, bucket_attempted, user_attempted, policy_attempted
            )


def _cleanup_source_resources(
    store, bucket, prefix, admin, user, policy, bucket_attempted, user_attempted, policy_attempted
):
    """Reconcile unique owned names even after a lost create response; try every cleanup."""
    original = sys.exc_info()[1]
    errors = []

    def bucket_cleanup():
        remove_snapshot_export(store, bucket, prefix)
        # Retain unexpected objects; still revoke the no-longer-needed reader below.
        assert not list(store.list_objects(bucket, recursive=True)), bucket
        store.remove_bucket(bucket)

    actions = [
        (bucket_attempted, bucket_cleanup),
        (user_attempted, lambda: admin.user_remove(user)),
        (policy_attempted, lambda: admin.policy_remove(policy)),
    ]
    for attempted, cleanup in actions:
        if not attempted:
            continue
        try:
            cleanup()
        except S3Error as error:
            if error.code != "NoSuchBucket":
                errors.append(error)
        except MinioAdminException as error:
            try:
                code = json.loads(error._body).get("Code")
            except (ValueError, TypeError, AttributeError):
                code = None
            if code not in ("XMinioAdminNoSuchUser", "XMinioAdminNoSuchPolicy"):
                errors.append(error)
        except Exception as error:
            errors.append(error)
    if errors:
        if original is None:
            raise ExceptionGroup("FN-16 private resource cleanup failed", errors)
        for error in errors:
            original.add_note(f"FN-16 cleanup also failed: {type(error).__name__}")


def foreign_export_uri(instance_uri, bucket, prefix):
    """Keep the server-reported endpoint, never use the local port-forward address."""
    uri = urlsplit(instance_uri)
    assert uri.scheme in ("s3", "minio") and uri.netloc and not uri.query and not uri.fragment
    assert uri.username is None and uri.password is None
    return uri._replace(path=f"/{bucket}/{prefix}").geturl()


def bundle_fingerprints(store, bucket, prefix, timeout=60):
    """Wait only for the exporter's transient staging object, then hash the published bundle."""
    deadline = time.monotonic() + timeout
    while True:
        objects = list(store.list_objects(bucket, recursive=True))
        assert objects and all(obj.object_name.startswith(prefix) for obj in objects), bucket
        if not any("/_staging/" in obj.object_name for obj in objects):
            break
        assert time.monotonic() < deadline, "Snapshot exporter staging cleanup did not finish"
        time.sleep(1)
    return {
        obj.object_name: (obj.size, hashlib.sha256(object_bytes(store, bucket, obj.object_name)).hexdigest())
        for obj in objects
    }


def assert_source_readonly(source, target_bucket, fingerprints):
    """Check every source GET and verify that this identity cannot write or read the target bucket."""
    assert bundle_fingerprints(source.reader, source.bucket, source.prefix) == fingerprints
    key = source.prefix + "readonly-probe"
    try:
        with pytest.raises(S3Error) as error:
            source.reader.put_object(source.bucket, key, io.BytesIO(b"probe"), 5)
        assert error.value.code == "AccessDenied", error.value.code
    finally:
        # Clean up even if the policy unexpectedly permits the write.
        source.store.remove_object(source.bucket, key)
    with pytest.raises(S3Error) as error:
        source.reader.stat_object(target_bucket, next(iter(fingerprints)))
    assert error.value.code == "AccessDenied", error.value.code


def assert_external_credentials_rejected(url, target, metadata_uri, options, db_name, api_key):
    """Inspect raw REST rejection because bulk_import raises before returning its error body."""
    __tracebackhide__ = True
    invalid = json.loads(options["external_spec"])
    invalid["extfs"]["access_key_value"] = "invalid-" + uuid4().hex
    headers = {"DB-Name": db_name}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    response = requests.post(
        f"{url.rstrip('/')}/v2/vectordb/jobs/import/create",
        headers=headers,
        json={
            "collectionName": target,
            "dbName": db_name,
            "files": [[metadata_uri]],
            "options": {**options, "external_spec": json.dumps(invalid)},
        },
        timeout=30,
    )
    response.raise_for_status()
    rejected = response.json()
    assert rejected["code"] != 0, "Invalid source credentials unexpectedly admitted an Import"
    assert not (rejected.get("data") or {}).get("jobId"), "Invalid credentials created an Import job"
    assert "failed to capture snapshot import source" in rejected.get("message", "").lower(), rejected
    log.info("SI-FN-16 invalid source credential control: rejected before job admission")


def put_target_bucket_decoys(store, bucket, prefix, fingerprints):
    """Invalid same-key objects make any target-bucket fallback fail instead of silently passing."""
    for key in fingerprints:
        assert key.startswith(prefix)
        with pytest.raises(S3Error) as error:
            store.stat_object(bucket, key)
        assert error.value.code == "NoSuchKey", error.value.code
        payload = b"FN16_WRONG_BUCKET_OBJECT"
        store.put_object(bucket, key, io.BytesIO(payload), len(payload))


def assert_target_snapshot_storage(store, bucket, metadata_uri, collection_id, expected_rows):
    """Read exact target manifests and stat their data objects in the instance bucket."""
    metadata, segments, _ = read_referenced_snapshot(store, bucket, metadata_uri)
    _, key = unquote(urlsplit(metadata_uri).path.lstrip("/")).split("/", 1)
    anchor = re.fullmatch(r"(?:(.*)/)?snapshots/[0-9]+/metadata/[0-9]+\.json", key)
    assert anchor
    root = f"{anchor[1]}/" if anchor[1] else ""
    data = [segment for segment in segments if segment["num_of_rows"] > 0]
    assert data and sum(segment["num_of_rows"] for segment in data) == expected_rows
    locations = snapshot_manifest_versions(metadata)
    inspected = []
    for segment in data:
        assert segment["storage_version"] == 3, segment
        base, version = locations[segment["segment_id"]]
        assert base == f"{root}insert_log/{collection_id}/{segment['partition_id']}/{segment['segment_id']}"
        manifest_key = f"{base}/_metadata/manifest-{version}.avro"
        records = list(fastavro.reader(io.BytesIO(object_bytes(store, bucket, manifest_key))))
        assert len(records) == 1 and records[0]["column_groups"]
        for group in records[0]["column_groups"]:
            assert group["files"]
            for entry in group["files"]:
                path = entry["path"]
                if not path.startswith(base + "/_data/"):
                    assert path and "/" not in path and path not in (".", ".."), path
                    path = f"{base}/_data/{path}"
                assert store.stat_object(bucket, path).size > 0, path
                inspected.append(path)
    assert inspected
    log.info(f"SI-FN-16 target storage bucket={bucket}, collection_id={collection_id}, data_objects={inspected}")


def assert_external_snapshot_search(results, queries, expected):
    """Check exact FLAT/L2 top-5, allowing only equal-distance boundary ties."""
    assert len(results) == len(queries)
    for vector, hits in zip(queries, results):
        distances = {
            pk: float(np.sum((np.array(row["vec"], dtype=np.float64) - vector) ** 2)) for pk, row in expected.items()
        }
        threshold = sorted(distances.values())[4]
        strict = {pk for pk, distance in distances.items() if distance < threshold}
        allowed = {pk for pk, distance in distances.items() if distance <= threshold}
        ids = {hit["id"] for hit in hits}
        assert len(hits) == len(ids) == 5 and strict <= ids <= allowed
        for hit in hits:
            assert hit["entity"] == {"source_uid": expected[hit["id"]]["source_uid"]}
            assert hit["distance"] == pytest.approx(distances[hit["id"]], rel=1e-5, abs=1e-6)
        assert [hit["distance"] for hit in hits] == sorted(hit["distance"] for hit in hits)


# Stored-output codecs and mock control client for SI-FN-14.

FUNCTION_CASES = [
    ("bm25", DataType.VARCHAR),
    ("bm25", DataType.TEXT),
    ("minhash", DataType.VARCHAR),
    ("embedding_float", DataType.VARCHAR),
    ("embedding_float", DataType.TEXT),
    ("embedding_int8", DataType.VARCHAR),
    ("embedding_int8", DataType.TEXT),
]
FUNCTION_CASE_IDS = [f"{family}_{dtype.name.lower()}" for family, dtype in FUNCTION_CASES]


class FunctionMockClient:
    """Explicit Pod-reachable provider URL and pytest-reachable control URL."""

    def __init__(self, endpoint, control_endpoint):
        assert endpoint and control_endpoint, "FN-14 embedding requires explicit mock and control endpoints"
        for url in (endpoint, control_endpoint):
            parsed = urlsplit(url)
            assert parsed.scheme == "http" and parsed.hostname and parsed.path in ("", "/"), url
            assert not parsed.query and not parsed.fragment and parsed.username is None
        self.endpoint = endpoint.rstrip("/")
        self.control_endpoint = control_endpoint.rstrip("/")
        self.session = f"fn14_{uuid4().hex}"
        assert self._get("/health") == {"service": "snapshot-function-mock", "dim": 32}

    def _get(self, path):
        response = requests.get(self.control_endpoint + path, timeout=10)
        response.raise_for_status()
        return response.json()

    def counters(self):
        counts = self._get(f"/_fn14/{self.session}")
        assert set(counts) == {"tei", "voyageai"}
        assert all(type(value) is int and value >= 0 for value in counts.values()), counts
        return counts


def function_spec(family, mock):
    """Output type, field options, function, and compatible index/search parameters."""
    if family == "bm25":
        dtype, field, kind, params = DataType.SPARSE_FLOAT_VECTOR, {}, FunctionType.BM25, {}
        index_type, metric, index_params = "SPARSE_INVERTED_INDEX", "BM25", {}
    elif family == "minhash":
        dtype, field, kind = DataType.BINARY_VECTOR, {"dim": 512}, FunctionType.MINHASH
        params = {"num_hashes": 16, "shingle_size": 3}
        index_type, metric, index_params = "MINHASH_LSH", "MHJACCARD", {"mh_lsh_band": 8}
    else:
        assert family in ("embedding_float", "embedding_int8"), family
        assert mock is not None
        dtype = DataType.FLOAT_VECTOR if family == "embedding_float" else DataType.INT8_VECTOR
        field, kind = {"dim": 32}, FunctionType.TEXTEMBEDDING
        params = {"integration_id": mock.session}
        if family == "embedding_float":
            params.update(provider="TEI", endpoint=mock.endpoint)
            index_type, metric, index_params = "FLAT", "L2", {}
        else:
            # VoyageAI reads its URL from Milvus config, not from function params.
            params.update(provider="voyageai", model_name="fn14-mock", dim="32")
            index_type, metric, index_params = "HNSW", "L2", {"M": 8, "efConstruction": 64}
    function = Function(
        name="text_to_output",
        function_type=kind,
        input_field_names=["doc"],
        output_field_names=["output"],
        params=params,
    )
    return dtype, field, function, index_type, metric, index_params


def function_rows(ids=range(32)):
    """Per-PK markers/text/vectors expose output permutation without a model oracle."""
    return [
        {
            "id": i,
            "doc": f"snapshot import document row{i} topic{i % 4} token{i} 中文 共同 共同",
            "marker": 1000 + i,
            "vec": [(i * 32 + j) / 4096 for j in range(32)],
        }
        for i in ids
    ]


def normalize_output(family, value):
    """Normalize storage/API representations without regenerating any output."""
    assert value is not None
    if family == "bm25":
        assert isinstance(value, bytes) and value and len(value) % 8 == 0
        pairs = list(struct.iter_unpack("<If", value))
        result = dict(pairs)
        assert len(result) == len(pairs) and all(math.isfinite(weight) for weight in result.values())
        return result
    if family == "minhash":
        if isinstance(value, list) and len(value) == 1 and isinstance(value[0], bytes):
            value = value[0]
        assert isinstance(value, bytes) and len(value) == 64, "MinHash output must contain 512 bits"
        return value
    if family == "embedding_int8":
        if isinstance(value, list) and len(value) == 1 and isinstance(value[0], bytes):
            value = value[0]
        if isinstance(value, bytes):
            value = list(struct.unpack(f"<{len(value)}b", value))
        values = np.asarray(value)
        assert values.shape == (32,) and np.issubdtype(values.dtype, np.integer)
        assert np.all(values >= -128) and np.all(values <= 127)
        return tuple(int(v) for v in values)
    assert family == "embedding_float"
    values = np.asarray(value, dtype=np.float32)
    assert values.shape == (32,) and np.all(np.isfinite(values))
    return tuple(float(v) for v in values)


# Controlled legacy descriptor fixtures, explicitly distinct from historical backups.


def legacy_schema(version):
    """Mirror the checked-in producer's versioned schemas, never invent field order."""
    source = Path(__file__).resolve().parents[3] / "internal/snapshotio/snapshot.go"
    text = source.read_text()
    match = re.search(r"func AvroSchemaV2\(\) string \{\s*return `([\s\S]*?)`", text)
    assert match, "Versioned producer schema missing"
    schema = json.loads(match[1])
    fields = schema["fields"]
    if version >= 3:
        index = next(i for i, field in enumerate(fields) if field["name"] == "is_sorted") + 1
        fields.insert(index, {"name": "commit_timestamp", "type": "long", "default": 0})
    if version >= 4:

        def visit(node):
            if isinstance(node, dict):
                if node.get("name") == "AvroFieldBinlog":
                    node["fields"][1:1] = [
                        {"name": "child_fields", "type": {"type": "array", "items": "long"}, "default": []},
                        {"name": "format", "type": "string", "default": ""},
                    ]
                for value in node.values():
                    visit(value)
            elif isinstance(node, list):
                for value in node:
                    visit(value)

        visit(schema)
    if version >= 5:
        index = next(i for i, field in enumerate(fields) if field["name"] == "commit_timestamp") + 1
        fields.insert(index, {"name": "manifest_has_index", "type": "boolean", "default": False})
    return schema


def copy_legacy_descriptors(batch, uri, layout):
    """Write genuine V2 wire descriptors (no commit field) pointing to captured data."""
    metadata = batch.metadata(uri)
    original = deepcopy(metadata)
    key = uri.split("/" + batch.bucket + "/", 1)[1]
    legacy_root = batch.prefix + "legacy"
    for item in metadata.get("storagev2_manifest_list", []):
        manifest = json.loads(item["manifest"])
        old_base = _snapshot_read_path(manifest["base_path"], original, key, batch.bucket, layout)
        assert "/insert_log/" in old_base
        new_base = legacy_root + "/files/insert_log/" + old_base.split("/insert_log/", 1)[1]
        objects = list(batch.store.list_objects(batch.bucket, prefix=old_base.rstrip("/") + "/", recursive=True))
        assert objects, "Captured V3 data dependencies missing"
        for obj in objects:
            destination = new_base + obj.object_name[len(old_base) :]
            assert destination.startswith(batch.prefix)
            batch.store.copy_object(batch.bucket, destination, CopySource(batch.bucket, obj.object_name))
            assert batch.store.stat_object(batch.bucket, destination).size == obj.size
        manifest["base_path"] = new_base
        item["manifest"] = json.dumps(manifest)
    commits = []
    for index, path in enumerate(original["manifest_list"]):
        resolved = _snapshot_read_path(path, original, key, batch.bucket, layout)
        data = object_bytes(batch.store, batch.bucket, resolved)
        if int(original["format_version"]) == 6:
            records = list(fastavro.reader(io.BytesIO(data)))
            assert len(records) == 1
            record = records[0]
        else:
            record = fastavro.schemaless_reader(io.BytesIO(data), legacy_schema(int(original["format_version"])))
        for field in record.get("deltalog_files", []):
            for entry in field["binlogs"]:
                old_path = _snapshot_read_path(entry["log_path"], original, key, batch.bucket, layout)
                assert "/delta_log/" in old_path
                new_path = legacy_root + "/files/delta_log/" + old_path.split("/delta_log/", 1)[1]
                batch.store.copy_object(batch.bucket, new_path, CopySource(batch.bucket, old_path))
                assert object_bytes(batch.store, batch.bucket, new_path) == object_bytes(
                    batch.store, batch.bucket, old_path
                )
                entry["log_path"] = new_path
        commits.append(record.get("commit_timestamp", 0))
        output = io.BytesIO()
        fastavro.schemaless_writer(output, legacy_schema(2), record)
        collection_id = key.rsplit("/snapshots/", 1)[1].split("/", 1)[0]
        descriptor = legacy_root + f"/snapshots/{collection_id}/segments/{record['segment_id']}.avro"
        batch.put(descriptor, output.getvalue())
        metadata["manifest_list"][index] = descriptor
        decoded = fastavro.schemaless_reader(io.BytesIO(output.getvalue()), legacy_schema(2))
        assert "commit_timestamp" not in decoded
    # The captured data keeps its actual V3 format. Only the snapshot descriptor is old.
    metadata["format_version"] = 2
    new_uri = batch.metadata_copy(uri, "legacy", lambda value: (value.clear(), value.update(metadata)))
    batch.record(
        "legacy_fixture",
        producer="current captured data + checked-in AvroSchemaV2",
        real_historical_backup=False,
        original_commit_timestamps=commits,
        legacy_commit_timestamps=[0] * len(commits),
        layout=layout,
    )
    return new_uri, commits


def parquet_bytes(columns, packed_groups=None):
    """Use Arrow physical field IDs and fixed-width vectors, as the Go writer does."""
    fields, arrays = [], []
    for field_id, name, values, dtype in columns:
        fields.append(pa.field(name, dtype, nullable=False, metadata={b"PARQUET:field_id": str(field_id).encode()}))
        arrays.append(pa.array(values, type=dtype))
    output = io.BytesIO()
    table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    if packed_groups is not None:
        # milvus-storage/common/metadata.cpp: one actual Arrow row group, full file-to-field mapping.
        group_fields = ";".join(",".join(str(column[0]) for column in group) for group in packed_groups)
        table = table.replace_schema_metadata(
            {
                b"row_group_metadata": f"{table.nbytes}|{table.num_rows}|0".encode(),
                b"storage_version": b"1.0.0",
                b"group_field_id_list": group_fields.encode(),
            }
        )
    pq.write_table(table, output, compression="zstd", row_group_size=table.num_rows)
    return output.getvalue()


def v1_insert_binlog(collection, partition, segment, field_id, dtype, timestamp, payload, original_size):
    """Encode the documented V1 descriptor + insert event around a real Parquet payload."""
    extra = json.dumps({"original_size": str(original_size), "nullable": False}).encode()
    # internal/storage/event_data.go: 8 event codes, fixed post-header sizes 52/16/... .
    descriptor = struct.pack("<qqqqQQi", collection, partition, segment, field_id, timestamp, timestamp, dtype)
    descriptor += bytes([52, 16, 16, 16, 16, 16, 16, 16]) + struct.pack("<i", len(extra)) + extra
    size = 17 + len(descriptor)
    header = struct.pack("<QBii", timestamp, 0, size, 4 + size)
    event_size = 17 + 16 + len(payload)
    event = struct.pack("<QBiiQQ", timestamp, 1, event_size, 4 + size + event_size, timestamp, timestamp)
    return b"\xbc\xfa\xff\x00" + header + descriptor + event + payload


def build_legacy_storage_fixture(batch, version):
    """Construct actual V1/V2 files; require a positive backup Import before negative snapshot testing."""
    assert version in (1, 2)
    storage_version = 0 if version == 1 else 2  # internal/storage/rw.go: StorageV1=0, StorageV2=2
    source = batch.create("producer", properties={"collection.autocompaction.enabled": "false"})
    rows = plain_rows(range(32))
    connection, context = batch.client._get_connection(), batch.client._generate_call_context()
    timestamp = connection.insert_rows(source, rows, context=context, timeout=30).timestamp
    batch.client.flush(source)
    uri = batch.snapshot(source)
    metadata = batch.metadata(uri)
    key = urlsplit(uri).path.split("/" + batch.bucket + "/", 1)[1]
    assert len(metadata["manifest_list"]) == 1
    descriptor_key = _snapshot_read_path(metadata["manifest_list"][0], metadata, key, batch.bucket, "referenced")
    encoded = object_bytes(batch.store, batch.bucket, descriptor_key)
    reader = fastavro.reader(io.BytesIO(encoded))
    schema, record = reader.writer_schema, next(reader)
    collection = batch.client.describe_collection(source)["collection_id"]
    partition, segment = record["partition_id"], record["segment_id"]
    root = batch.prefix + f"storage_v{version}"
    prefix = root + f"/files/insert_log/{collection}/{partition}/{segment}"
    info = batch.client.describe_collection(source)
    fields = {field["name"]: field["field_id"] for field in info["fields"]}
    columns = [
        (0, "RowID", list(range(1, 33)), pa.int64()),
        (1, "Timestamp", [timestamp] * 32, pa.int64()),
        (fields["id"], "id", [row["id"] for row in rows], pa.int64()),
        (fields["vec"], "vec", [np.asarray(row["vec"], dtype="<f4").tobytes() for row in rows], pa.binary(64)),
        (fields["marker"], "marker", [row["marker"] for row in rows], pa.int64()),
    ]
    groups = (
        [[column] for column in columns]
        if version == 1
        else [[column for column in columns if column[1] != "vec"], [columns[3]]]
    )
    logs, checksums = [], {}
    for group in groups:
        group_id = group[0][0]
        data = parquet_bytes(group, packed_groups=groups if version == 2 else None)
        if version == 1:
            dtype = 101 if group[0][1] == "vec" else 5
            data = v1_insert_binlog(
                collection, partition, segment, group_id, dtype, timestamp, data, 32 * (64 if dtype == 101 else 8)
            )
        path = prefix + f"/{group_id}/1"
        batch.put(path, data)
        checksums[path] = hashlib.sha256(data).hexdigest()
        logs.append(
            {
                "field_id": group_id,
                "child_fields": [],
                "format": "",
                "binlogs": [
                    {
                        "entries_num": 32,
                        "timestamp_from": timestamp,
                        "timestamp_to": timestamp,
                        "log_path": path,
                        "log_size": len(data),
                        "memory_size": 32 * 64,
                        "log_id": 1,
                    }
                ],
            }
        )
    control = batch.create("backup_control")
    response = batch.request(
        control,
        prefix,
        partition="_default",
        options={"backup": "true", "storage_version": str(storage_version), "auto_commit": "true"},
        label=f"SI-ER-03/V{version}_readability_control",
    )
    assert response["code"] == 0, response
    progress = batch.wait(response["data"]["jobId"])
    assert progress["state"] == "Completed" and progress["importedRows"] == len(rows), progress
    batch.reload_assert(control, rows)
    record.update(
        storage_version=storage_version, binlog_files=logs, statslog_files=[], deltalog_files=[], commit_timestamp=0
    )
    out = io.BytesIO()
    fastavro.writer(out, schema, [record])
    new_descriptor = root + f"/snapshots/{collection}/segments/{segment}.avro"
    batch.put(new_descriptor, out.getvalue())
    metadata["manifest_list"] = [new_descriptor]
    metadata["storagev2_manifest_list"] = []
    result = batch.metadata_copy(uri, f"storage_v{version}", lambda value: (value.clear(), value.update(metadata)))
    batch.record(
        "constructed_storage_fixture",
        storage_version=storage_version,
        storage_format=f"V{version}",
        generator="Python V1 envelope/Arrow packed Parquet",
        physical_file_checksums=checksums,
        positive_backup_control=True,
        historical_release_artifact=False,
    )
    return {"uri": result, "physical_file_checksums": checksums, "layout": "referenced"}


def reexport_owned_legacy_snapshot(batch, source, uri, restore_dir):
    """Convert only this test's snapshot artifact to V2, export via API, then restore every byte.

    Live row files, collection metadata and etcd are never changed. This exercises
    the named-snapshot exporter reading an actual old wire snapshot, not a fresh
    snapshot reconstructed from today's catalog (which would restore the commit).
    """
    from common.snapshot_import_common import export_snapshot, wait_for_snapshot_export

    assert source in batch.collections
    assert batch.snapshots[-1][0] == source
    snapshot_name = batch.snapshots[-1][1]
    assert batch.client.describe_snapshot(snapshot_name, source, db_name=batch.db).s3_location == uri
    metadata = batch.metadata(uri)
    collection = batch.client.describe_collection(source)["collection_id"]
    key = urlsplit(uri).path.split("/" + batch.bucket + "/", 1)[1]
    root, anchor, suffix = key.rpartition("/snapshots/")
    assert root and anchor and suffix.startswith(f"{collection}/metadata/")
    owner_prefix = f"{root}/snapshots/{collection}/"
    originals = {key: object_bytes(batch.store, batch.bucket, key)}
    replacements = {}
    for path in metadata["manifest_list"]:
        resolved = _snapshot_read_path(path, metadata, key, batch.bucket, "referenced")
        assert resolved.startswith(owner_prefix), "Cannot mutate an unrelated snapshot artifact"
        data = object_bytes(batch.store, batch.bucket, resolved)
        originals[resolved] = data
        record = next(fastavro.reader(io.BytesIO(data)))
        out = io.BytesIO()
        fastavro.schemaless_writer(out, legacy_schema(2), record)
        replacements[resolved] = out.getvalue()
    metadata["format_version"] = 2
    replacements[key] = json.dumps(metadata).encode()
    export_uri = urlsplit(uri)._replace(path=f"/{batch.bucket}/{batch.prefix}reexport/").geturl()
    restore_dir.mkdir(parents=True, exist_ok=False)
    recovery = {}
    for index, (path, data) in enumerate(originals.items()):
        backup = restore_dir / f"{index}.bin"
        backup.write_bytes(data)
        recovery[path] = {"backup": str(backup), "sha256": hashlib.sha256(data).hexdigest()}
    (restore_dir / "manifest.json").write_text(json.dumps(recovery, indent=2))
    try:
        for path, data in replacements.items():
            batch.store.put_object(batch.bucket, path, io.BytesIO(data), len(data))
            assert object_bytes(batch.store, batch.bucket, path) == data
        batch.uncertain = True
        job = export_snapshot(batch.url, source, snapshot_name, export_uri, batch.db, cf.param_info.param_token)
        progress = wait_for_snapshot_export(batch.url, job, batch.db, cf.param_info.param_token)
        batch.uncertain = False
        assert progress["state"] == "ExportSnapshotCompleted", progress
        exported = progress["snapshotMetadataURI"]
    finally:
        if batch.uncertain:
            # A timeout/transport error does not stop the exporter. Freeze its inputs,
            # preserve owned resources and the original bytes until terminal state is proven.
            batch.record(
                "owned_snapshot_restore_pending", source=source, recovery_manifest=str(restore_dir / "manifest.json")
            )
        else:
            for path, data in reversed(list(originals.items())):
                batch.store.put_object(batch.bucket, path, io.BytesIO(data), len(data))
                assert object_bytes(batch.store, batch.bucket, path) == data
            batch.record(
                "owned_snapshot_restored",
                source=source,
                sha256={path: info["sha256"] for path, info in recovery.items()},
            )
    batch.record("legacy_reexport", job_id=job, uri=exported, input_format=2)
    return exported


# FN-17 V3 ownership evidence from captured manifests; no live metadata guessing.


class IndexEvidencePending(Exception):
    """An eligible snapshot has not yet captured a real index artifact."""


def ownership_runtime_config():
    """Read only three exported settings; never print or persist the full config response."""
    endpoint = os.environ.get("MILVUS_MANAGEMENT_URI")
    if not endpoint:
        uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        host = urlsplit(uri).hostname
        assert host, "FN-17 requires a valid Proxy URI or MILVUS_MANAGEMENT_URI override"
        host = f"[{host}]" if ":" in host else host
        endpoint = f"http://{host}:9091"
    response = requests.get(endpoint.rstrip("/") + "/api/v1/_cluster/configs", timeout=30)
    response.raise_for_status()
    config = response.json()

    def value(key):
        canonical = re.sub(r"[._/]", "", key.lower())
        raw = config[canonical] if canonical in config else config[key.lower()]
        return str(raw).split("[", 1)[0]

    assert value("common.storage.useLoonFFI").lower() == "true", "FN-17 V3 target configuration is required"
    threshold = int(value("dataNode.text.inlineThreshold"))
    minimum = int(value("indexCoord.segment.minSegmentNumRowsToEnableIndex"))
    assert threshold > 0 and 0 <= minimum <= 4096, "FN-17 4096-row fixture cannot prove real indexing at this threshold"
    result = {"text_inline_threshold": threshold, "index_min_segment_rows": minimum, "target_storage_version": 3}
    log.info(f"SI-FN-17 runtime settings={result}")
    return result


def ownership_rows(threshold):
    """4096 unique rows; eight scattered LOB values per TEXT column."""
    vectors = np.random.default_rng().random((4096, 32)).astype(np.float32)
    unit = "中文🙂/owned snapshot/"
    long_text = unit * (max(96 * 1024, threshold + 1024) // len(unit.encode("utf-8")) + 1)
    # One LOB per column in each 512-row block, on different PKs.
    lob_pks = {"text_a": set(range(3, 4096, 512)), "text_b": set(range(2, 4096, 512))}

    def text(name, pk, kind):
        if kind == 0:
            return ""
        prefix = f"{name}:PK={pk}:"
        if pk in lob_pks[name]:
            return prefix + long_text
        return prefix + ("中文🙂" if kind == 2 else "short English")

    return [
        {
            "id": pk,
            "marker": 1000 + pk,
            "vec": vectors[pk].tolist(),
            "text_a": text("text_a", pk, pk % 4),
            "text_b": text("text_b", pk, (pk + 1) % 4),
        }
        for pk in range(4096)
    ]


def snapshot_file_fingerprints(store, bucket, keys):
    """Hash only explicit captured files; stream large LOBs with bounded client memory."""
    result = {}
    for key in sorted(set(keys)):
        size = store.stat_object(bucket, key).size
        response = store.get_object(bucket, key)
        digest, received = hashlib.sha256(), 0
        try:
            for chunk in iter(lambda: response.read(1024 * 1024), b""):
                digest.update(chunk)
                received += len(chunk)
        finally:
            response.close()
            response.release_conn()
        assert received == size, key
        result[key] = (size, digest.hexdigest())
    assert result
    return result


def owned_bundle_fingerprints(store, bucket, prefix, timeout=60):
    """The primary bucket is shared: enumerate ONLY this validated private export prefix."""
    _private_export_prefix(prefix)
    deadline = time.monotonic() + timeout
    while True:
        objects = list(store.list_objects(bucket, prefix=prefix, recursive=True))
        assert objects and all(item.object_name.startswith(prefix) for item in objects), prefix
        if not any("/_staging/" in item.object_name for item in objects):
            return snapshot_file_fingerprints(store, bucket, [item.object_name for item in objects])
        assert time.monotonic() < deadline, "FN-17 private export staging cleanup exceeded 60s"
        time.sleep(min(1, max(0, deadline - time.monotonic())))


def assert_hnsw_definition(definitions, index_name, m, ef_construction, mmap):
    assert str(definitions["collection"]["properties"]["mmap.enabled"]).lower() == str(mmap).lower()
    assert set(definitions["indexes"]) == {index_name}
    index = definitions["indexes"][index_name]
    assert index["field_name"] == "vec" and index["index_type"] == "HNSW" and index["metric_type"] == "L2"
    assert int(index["M"]) == m and int(index["efConstruction"]) == ef_construction


def _index_objects(base, entry, segment, root, collection_id, layout, bundle_root, direct_paths=None):
    """Match ManifestIndexRelativePath and IndexPathBuilder, including legitimate '..' walks."""
    if direct_paths is None:
        path = entry["path"]
        assert path and "://" not in path and not path.startswith("/")
        directory = posixpath.normpath(posixpath.join(base, "_index", path))
        names = entry["index_file_keys"]
        files = [directory + "/" + name for name in names]
    else:
        files = direct_paths
        assert files
        directory = posixpath.dirname(files[0])
        assert all(posixpath.dirname(path) == directory for path in files)
        names = [posixpath.basename(path) for path in files]
    build, version = int(entry["build_id"]), int(entry["index_version"])
    assert build > 0 and version >= 0
    if layout == "referenced":
        if int(entry["index_store_path_version"]) >= 1:
            suffix = f"index_v1/{collection_id}/{segment['partition_id']}/{segment['segment_id']}/{build}/{version}"
        else:
            suffix = f"index_files/{build}/{version}/{segment['partition_id']}/{segment['segment_id']}"
        assert directory == posixpath.join(root, suffix), directory
    else:
        assert directory.startswith(bundle_root + "/files/"), "Exported index escapes its private bundle"
    assert names and len(names) == len(set(names))
    assert all(name and "/" not in name and name not in (".", "..") for name in names)
    return files


def inspect_snapshot_ownership(
    store,
    bucket,
    uri,
    collection_id,
    vector_field_id,
    index_name,
    m,
    ef_construction,
    minimum_rows,
    layout="referenced",
):
    """Require V3 data/LOB ownership and at least one real, nonempty HNSW artifact."""
    metadata, segments, deletes, captured = read_snapshot_partition_inventory(
        store, bucket, uri, layout=layout, case_id="SI-FN-17"
    )
    assert not deletes and sum(segment["num_of_rows"] for segment in segments) == 4096
    definitions = [entry for entry in metadata.get("indexes", []) if entry.get("index_name") == index_name]
    assert len(definitions) == 1 and int(definitions[0]["fieldID"]) == vector_field_id, (
        "Snapshot lost the index definition"
    )
    uri_bucket, metadata_key = unquote(urlsplit(uri).path.lstrip("/")).split("/", 1)
    assert uri_bucket == bucket
    anchor = re.fullmatch(r"(?:(.*)/)?snapshots/[0-9]+/metadata/[0-9]+\.json", metadata_key)
    assert anchor
    root = anchor[1] or ""
    versions = snapshot_manifest_versions(metadata)
    found = {
        "segment_ids": [],
        "metadata_files": set(captured),
        "data_files": set(),
        "lob_files": set(),
        "index_files": set(),
    }
    indexed = []
    for segment in segments:
        found["segment_ids"].append(segment["segment_id"])
        base, version = versions[segment["segment_id"]]
        base = _snapshot_read_path(base, metadata, metadata_key, bucket, layout)
        if layout == "referenced":
            assert base == posixpath.join(
                root, f"insert_log/{collection_id}/{segment['partition_id']}/{segment['segment_id']}"
            )
        else:
            assert base.startswith(root + "/files/"), base
        manifest_key = f"{base}/_metadata/manifest-{version}.avro"
        found["metadata_files"].add(manifest_key)
        records = list(fastavro.reader(io.BytesIO(object_bytes(store, bucket, manifest_key))))
        assert len(records) == 1
        manifest = records[0]
        assert not manifest.get("delta_logs") and manifest["column_groups"]
        for group in manifest["column_groups"]:
            assert group["files"]
            for item in group["files"]:
                path = item["path"]
                if not path.startswith(base + "/_data/"):
                    assert path and "/" not in path and path not in (".", ".."), path
                    path = base + "/_data/" + path
                found["data_files"].add(path)
        lob_root = base.rsplit("/", 1)[0] + "/lobs/"
        for item in manifest.get("lob_files", []):
            path = item["path"]
            assert path and "://" not in path and ".." not in path.split("/")
            path = path if path.startswith(lob_root) else lob_root + path
            assert path.startswith(lob_root), path
            found["lob_files"].add(path)
        # 9e0485da stores completed index files in the captured OCF segment record.
        # Newer runtimes can additionally publish them in the packed manifest.
        entries = [(entry, None) for entry in manifest.get("indexes", [])]
        for legacy in segment.get("index_files", []):
            assert int(legacy["segment_id"]) == segment["segment_id"]
            params = {item["key"]: item["value"] for item in legacy["index_params"]}
            entry = {**legacy, "properties": params, "index_type": params["index_type"]}
            files = [
                _snapshot_read_path(path, metadata, metadata_key, bucket, layout) for path in legacy["index_file_paths"]
            ]
            entries.append((entry, files))
        for entry, direct_paths in entries:
            if int(entry["field_id"]) != vector_field_id:
                continue
            assert entry["index_name"] == index_name, entry
            assert int(entry["num_rows"]) == segment["num_of_rows"]
            paths = entry["index_file_keys"] if direct_paths is None else direct_paths
            if not paths and int(entry["serialized_size"]) == 0:
                if segment["num_of_rows"] < minimum_rows:
                    continue  # Legitimate fake-finished small-segment record, not real artifact evidence.
                raise IndexEvidencePending("FN-17 eligible segment has not captured nonempty index artifacts")
            assert int(entry["serialized_size"]) > 0
            keys = _index_objects(base, entry, segment, root, collection_id, layout, root, direct_paths)
            found["index_files"].update(keys)
            if segment["num_of_rows"] >= minimum_rows:
                assert entry["index_type"].upper() == "HNSW", entry
                params = entry["properties"]
                assert params["metric_type"].upper() == "L2"
                assert int(params["M"]) == m and int(params["efConstruction"]) == ef_construction
                indexed.append(segment["segment_id"])
    if not indexed:
        raise IndexEvidencePending("FN-17 real HNSW build NotCovered: no eligible segment with registered artifact")
    assert found["data_files"] and found["lob_files"] and found["index_files"]
    for kind in ("metadata_files", "data_files", "lob_files", "index_files"):
        for key in found[kind]:
            assert store.stat_object(bucket, key).size > 0, key
        found[kind] = sorted(found[kind])
    found["indexed_segments"] = sorted(indexed)
    log.info(f"SI-FN-17 {layout} storage evidence={found}")
    return found


def assert_owned_hnsw_search(results, queries, rows):
    """HNSW is approximate: verify returned identities, payload, distance and ordering."""
    expected = {row["id"]: row for row in rows}
    assert len(results) == len(queries)
    for vector, hits in zip(queries, results):
        assert len(hits) == len({hit["id"] for hit in hits}) == 5
        for hit in hits:
            assert hit["id"] in expected
            row = expected[hit["id"]]
            assert hit["entity"] == {"marker": row["marker"]}
            distance = float(np.sum((np.asarray(row["vec"], dtype=np.float64) - vector) ** 2))
            np.testing.assert_allclose(hit["distance"], distance, rtol=1e-5, atol=1e-6)
        distances = [hit["distance"] for hit in hits]
        assert distances == sorted(distances)


# Small input truth and strict value checks for SI-FN-19 (no server access).

SCALAR_TYPES = (
    DataType.BOOL,
    DataType.INT8,
    DataType.INT16,
    DataType.INT32,
    DataType.INT64,
    DataType.FLOAT,
    DataType.DOUBLE,
    DataType.VARCHAR,
    DataType.TEXT,
    DataType.JSON,
    DataType.GEOMETRY,
    DataType.TIMESTAMPTZ,
)
ARRAY_TYPES = SCALAR_TYPES[:8]
INTEGER_BITS = {DataType.INT8: 8, DataType.INT16: 16, DataType.INT32: 32, DataType.INT64: 64}


@dataclass(frozen=True)
class ScalarField:
    name: str
    dtype: DataType
    nullable: bool
    element_type: DataType | None = None

    @property
    def options(self):
        options = {"nullable": self.nullable}
        if self.dtype == DataType.ARRAY:
            options.update(element_type=self.element_type, max_capacity=8)
        if self.dtype == DataType.VARCHAR or self.element_type == DataType.VARCHAR:
            options["max_length"] = 64
        return options


def scalar_fields(group):
    """Required/nullable pairs share one schema; no layout or null-ratio product."""
    assert group in ("scalar", "array"), group
    return [
        ScalarField(
            f"{dtype.name.lower()}_{'null' if nullable else 'req'}",
            DataType.ARRAY if group == "array" else dtype,
            nullable,
            dtype if group == "array" else None,
        )
        for dtype in (ARRAY_TYPES if group == "array" else SCALAR_TYPES)
        for nullable in (False, True)
    ]


def _scalar_value(dtype, case, pk):
    if dtype == DataType.BOOL:
        return case % 2 == 0
    if dtype in INTEGER_BITS:
        bits = INTEGER_BITS[dtype]
        return [0, -1, -(2 ** (bits - 1)), 2 ** (bits - 1) - 1, 1, 3, -7, 2 ** (bits - 2) + 1][case]
    if dtype in (DataType.FLOAT, DataType.DOUBLE):
        precision = np.float32 if dtype == DataType.FLOAT else np.float64
        bounds = np.finfo(precision)
        return float(precision([0, -1.25, 0.1, -0.1, bounds.tiny, bounds.max, -bounds.max, 3.5][case]))
    if dtype in (DataType.VARCHAR, DataType.TEXT):
        return ["", "ascii", "中文a", "🙂", "a" * 64, "中" * 20 + "🙂", 'quote"slash\\', f"row_{pk}"][case]
    if dtype == DataType.JSON:
        return [
            {},
            {"row": pk},
            {"nested": {"value": None, "flag": False}},
            {"items": [3, 1, 3, None]},
            {"text": "中文🙂"},
            {"empty": [], "object": {}},
            {"fraction": 1.25, "zero": 0},
            {"row": pk, "mixed": [True, "a", {"x": -1}]},
        ][case]
    if dtype == DataType.GEOMETRY:
        return [
            f"POINT ({pk} 1)",
            f"LINESTRING ({pk} 0, {pk + 1} 1, {pk + 2} 0)",
            f"POLYGON (({pk} 0, {pk + 1} 0, {pk + 1} 1, {pk} 1, {pk} 0))",
        ][case % 3]
    assert dtype == DataType.TIMESTAMPTZ, dtype
    return [
        "2024-01-01T00:00:00Z",
        "2024-01-01T08:00:00+08:00",
        "2023-12-31T19:00:00-05:00",
        "2024-02-29T12:34:56.123456Z",
        "1970-01-01T00:00:00Z",
        "2025-01-01T00:00:00.000001Z",
        "2025-01-01T00:00:00.999999Z",
        "2000-01-01T00:00:00Z",
    ][case]


def scalar_rows(fields):
    """Eight value patterns, four rows each; staggered NULLs leave every pattern visible."""
    rows = []
    for pk in range(32):
        row = {"id": pk, "vec": [(pk * 32 + j) / 4096 for j in range(32)]}
        for index, field in enumerate(fields):
            if field.nullable and (pk + index // 2) % 4 == 0:
                row[field.name] = None
                continue
            case = pk // 4
            if field.dtype == DataType.ARRAY:
                # Include empty, single, repeated, unordered and full-capacity lists.
                indices = [[], [0], [5, 4, 5], list(range(8)), [2, 3], [1, 1], [7, 6, 0], [4, 0]][case]
                row[field.name] = [_scalar_value(field.element_type, i, pk) for i in indices]
            else:
                row[field.name] = _scalar_value(field.dtype, case, pk)
        rows.append(row)
    random.shuffle(rows)
    return rows


def _utc(value):
    assert isinstance(value, str), value
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    assert timestamp.tzinfo is not None, value
    return timestamp.astimezone(UTC)


def _assert_value(actual, expected, dtype):
    if expected is None:
        assert actual is None, (actual, expected)
    elif dtype == DataType.BOOL:
        assert type(actual) is bool and actual is expected, (actual, expected)
    elif dtype in INTEGER_BITS:
        assert type(actual) is int and actual == expected, (actual, expected)
    elif dtype in (DataType.FLOAT, DataType.DOUBLE):
        assert isinstance(actual, (float, np.floating)) and math.isfinite(actual), actual
        # Only round input truth; rounding the returned value could conceal corruption.
        truth = float(np.float32(expected)) if dtype == DataType.FLOAT else float(expected)
        assert float(actual) == truth, (actual, truth)
    elif dtype == DataType.JSON:
        assert type(actual) is type(expected), (actual, expected)
        if isinstance(expected, dict):
            assert actual.keys() == expected.keys(), (actual, expected)
            for key, value in expected.items():
                _assert_value(actual[key], value, dtype)
        elif isinstance(expected, list):
            assert len(actual) == len(expected), (actual, expected)
            for value, truth in zip(actual, expected, strict=True):
                _assert_value(value, truth, dtype)
        else:
            assert actual == expected, (actual, expected)
    elif dtype == DataType.GEOMETRY:
        assert isinstance(actual, str) and wkt.loads(actual).equals(wkt.loads(expected)), (actual, expected)
    elif dtype == DataType.TIMESTAMPTZ:
        assert _utc(actual) == _utc(expected), (actual, expected)
    else:
        assert dtype in (DataType.VARCHAR, DataType.TEXT), dtype
        assert isinstance(actual, str) and actual == expected, (actual, expected)


def assert_scalar_rows(actual, expected, fields, projection):
    """Exact PK set, projection, NULLs and values; never infer truth from source output."""
    assert len(actual) == len(expected), (len(actual), len(expected))
    assert len({row["id"] for row in actual}) == len(actual), "Duplicate query PKs"
    assert {row["id"] for row in actual} == set(expected), "Wrong query PKs"
    selected = {field.name: field for field in fields if field.name in projection}
    for row in actual:
        assert set(row) == set(projection), (set(row), projection)
        truth = expected[row["id"]]
        assert type(row["id"]) is int
        np.testing.assert_array_equal(np.asarray(row["vec"]), truth["vec"])
        for name, field in selected.items():
            value, original = row[name], truth[name]
            if original is not None and field.dtype == DataType.ARRAY:
                assert isinstance(value, list) and len(value) == len(original), (name, value, original)
                for item, wanted in zip(value, original, strict=True):
                    _assert_value(item, wanted, field.element_type)
            else:
                _assert_value(value, original, field.dtype)


def scalar_filters(fields, expected):
    """Filter expressions plus independent input-derived PK sets."""
    for field in fields:
        name, dtype = field.name, field.dtype
        if dtype == DataType.TEXT:
            # Text fields cannot be used in ordinary expressions, including IS NULL.
            continue
        for is_null in (True, False):
            yield (
                f"{name} is {'null' if is_null else 'not null'}",
                {pk for pk, row in expected.items() if (row[name] is None) == is_null},
            )
        if dtype in INTEGER_BITS or dtype in (DataType.FLOAT, DataType.DOUBLE):
            yield f"{name} > 0", {pk for pk, row in expected.items() if row[name] is not None and row[name] > 0}
        elif dtype in (DataType.BOOL, DataType.VARCHAR):
            value = False if dtype == DataType.BOOL else ""
            yield f"{name} == {json.dumps(value)}", {pk for pk, row in expected.items() if row[name] == value}
        elif dtype == DataType.ARRAY:
            value = _scalar_value(field.element_type, 0, 0)
            yield (
                f"array_contains({name}, {json.dumps(value)})",
                {pk for pk, row in expected.items() if row[name] is not None and value in row[name]},
            )


# FN-15 text fixtures and read-only evidence from exact snapshot manifests.


def snapshot_text_rows():
    """Generate 32 rows with different inline/LOB text positions in each column.

    The threshold environment value is a fixture sizing hint, to be checked in
    environment preflight. Actual reference flags and files must still prove LOB.
    """
    threshold = int(os.getenv("MILVUS_TEXT_INLINE_THRESHOLD", "65536"))
    assert threshold > 0

    def text(field, pk, kind):
        prefix = f"{field}:PK={pk}:"
        if kind == 0:
            return ""
        if kind == 1:
            return prefix + "short English"
        if kind == 2:
            return prefix + "中文🙂不同内容"
        unit = "中文🙂/snapshot payload/"
        size = max(96 * 1024, threshold + 1024)
        return prefix + unit * (size // len(unit.encode("utf-8")) + 1)

    rows = [
        {
            "id": pk,
            "marker": 1000 + pk,
            "vec": [(pk * 32 + i) / 4096 for i in range(32)],
            "text_a": text("text_a", pk, pk % 4),
            "text_b": text("text_b", pk, (pk + 1) % 4),
        }
        for pk in range(32)
    ]
    return rows, threshold


def inspect_snapshot_text_lobs(store, bucket, uri, fields, expected_rows, threshold, tmp_path, case_id="SI-FN-15"):
    """Require real inline values and existing, nonempty registered LOB objects in both TEXT columns.

    This checks source preparation only. Public query assertions verify text
    values; it does not claim that an unprojected LOB is never opened by Import.
    """
    metadata, segments, deletes, _ = read_snapshot_partition_inventory(store, bucket, uri, case_id=case_id)
    assert metadata["layout"] in ("SnapshotLayoutReferenced", 1)
    assert not deletes, f"{case_id} requires a source without L0 deletes"
    manifests = snapshot_manifest_versions(metadata)
    expected = {row["id"]: row for row in expected_rows}
    seen = set()
    evidence = {name: {"inline": 0, "lob": 0, "files": set()} for name in ("text_a", "text_b")}
    physical_ids = {"id": fields["id"]["field_id"], "vector": fields["vec"]["field_id"]}
    physical_ids.update({name: fields[name]["field_id"] for name in evidence})
    for segment in segments:
        base, version = manifests[segment["segment_id"]]
        manifest_key = f"{base}/_metadata/manifest-{version}.avro"
        records = list(fastavro.reader(io.BytesIO(object_bytes(store, bucket, manifest_key))))
        assert len(records) == 1
        manifest = records[0]
        lob_root = base.rsplit("/", 1)[0] + "/lobs/"
        registered = {}
        for entry in manifest.get("lob_files", []):
            path = entry["path"]
            assert path and "://" not in path and ".." not in path.split("/")
            key = path if path.startswith(lob_root) else lob_root + path
            identity = (entry["field_id"], key)
            assert identity not in registered, "Duplicate manifest LOB registration"
            registered[identity] = entry
        physical = read_segment_rows(
            store,
            bucket,
            json.dumps({"base_path": base, "ver": version}),
            physical_ids,
            segment["num_of_rows"],
            32,
            tmp_path,
        )
        for row in physical:
            pk = row["id"]
            assert pk in expected and pk not in seen, ("Unexpected or repeated source PK", pk)
            seen.add(pk)
            for name, counts in evidence.items():
                ref = row[name]
                assert isinstance(ref, bytes) and ref, ("Expected encoded TEXT reference", pk, name)
                original = expected[pk][name].encode("utf-8")
                if ref[0] == 0:
                    assert ref[1:] == original, ("Inline TEXT bytes differ", pk, name)
                    assert len(original) < threshold, ("Long fixture stayed inline; check actual threshold", pk, name)
                    counts["inline"] += 1
                else:
                    assert ref[0] == 1 and len(ref) == 24 and ref[1:4] == b"\x00\x00\x00"
                    assert len(original) >= threshold, ("Short fixture became LOB; check actual threshold", pk, name)
                    file_id = str(UUID(bytes=ref[4:20]))
                    key = f"{lob_root}{fields[name]['field_id']}/_data/{file_id}.vx"
                    entry = registered[(fields[name]["field_id"], key)]
                    offset = int.from_bytes(ref[20:24], "little", signed=True)
                    assert 0 <= offset < entry["total_rows"] and entry["valid_rows"] > 0
                    if key not in counts["files"]:
                        assert store.stat_object(bucket, key).size > 0, key
                        counts["files"].add(key)
                    counts["lob"] += 1
    assert seen == set(expected)
    for counts in evidence.values():
        assert counts["inline"] > 0 and counts["lob"] > 0 and counts["files"], f"{case_id} LOB fixture not established"
        counts["files"] = sorted(counts["files"])
    return evidence
