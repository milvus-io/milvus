"""Inspect persisted Milvus TEXT references and their backing LOB objects."""

import io
import os
import tempfile
import uuid

import pytest
from utils.util_log import test_log as log


def segment_identifier(segment):
    if isinstance(segment, dict):
        for key in ["id", "segment_id", "segmentID"]:
            if key in segment:
                return str(segment[key])
    for attr in ["id", "segment_id", "segmentID"]:
        if hasattr(segment, attr):
            return str(getattr(segment, attr))
    return repr(segment)


def item_value(item, keys, default=None):
    for key in keys:
        if isinstance(item, dict) and key in item:
            return item[key]
        if hasattr(item, key):
            return getattr(item, key)
    return default


def int_env(name, default):
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        pytest.fail(f"{name} must be an integer, got {raw!r}")


def object_key(*parts):
    return "/".join(str(part).strip("/") for part in parts if part not in [None, ""])


def minio_endpoint(minio_host):
    endpoint = (minio_host or "localhost").strip()
    if "://" in endpoint:
        endpoint = endpoint.split("://", 1)[1]
    endpoint = endpoint.split("/", 1)[0]
    if ":" not in endpoint:
        endpoint = f"{endpoint}:9000"
    return endpoint


def new_minio_client(minio_host):
    minio_mod = pytest.importorskip("minio", reason="TEXT LOB layout inspection requires minio client")
    return minio_mod.Minio(
        minio_endpoint(minio_host),
        access_key=os.getenv("MILVUS_MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MILVUS_MINIO_SECRET_KEY", "minioadmin"),
        secure=os.getenv("MILVUS_MINIO_SECURE", "false").lower() in ["1", "true", "yes"],
    )


def ensure_minio_bucket(minio_client, bucket):
    try:
        if not minio_client.bucket_exists(bucket):
            pytest.skip(f"TEXT LOB layout inspection requires existing MinIO bucket {bucket!r}")
    except Exception as exc:
        pytest.skip(f"TEXT LOB layout inspection requires MinIO bucket access: {exc}")


def list_minio_keys(minio_client, bucket, prefix):
    prefix = object_key(prefix)
    if prefix and not prefix.endswith("/"):
        prefix = f"{prefix}/"
    return sorted(obj.object_name for obj in minio_client.list_objects(bucket, prefix=prefix, recursive=True))


def field_id_from_desc(desc, field_name):
    for field in desc.get("fields", []):
        if item_value(field, ["name"]) == field_name:
            field_id = item_value(field, ["field_id", "fieldID", "id"])
            assert field_id is not None, f"field {field_name!r} has no field_id in describe_collection: {field}"
            return str(field_id)
    pytest.fail(f"field {field_name!r} not found in describe_collection: {desc}")


def collection_id_from_desc(desc):
    collection_id = item_value(desc, ["collection_id", "collectionID", "id"])
    assert collection_id is not None, f"collection_id missing from describe_collection: {desc}"
    return str(collection_id)


def select_segment_by_rows(segments, expected_rows):
    assert segments, "expected at least one persistent segment after flush"
    exact_matches = []
    for segment in segments:
        row_count = item_value(segment, ["num_rows", "numRows", "num_of_rows", "numOfRows"])
        if row_count is not None and int(row_count) == expected_rows:
            exact_matches.append(segment)
    if len(exact_matches) == 1:
        return exact_matches[0]
    if len(segments) == 1:
        return segments[0]
    pytest.fail(f"could not identify TEXT LOB segment: segments={segments}, expected_rows={expected_rows}")


def find_segment_base_path(minio_client, bucket, root_path, collection_id, segment_id):
    prefix = object_key(root_path, "insert_log", collection_id)
    keys = list_minio_keys(minio_client, bucket, prefix)
    for key in keys:
        parts = key.split("/")
        for index, part in enumerate(parts):
            if part == str(segment_id):
                return "/".join(parts[: index + 1])
    return None


def read_parquet_object(minio_client, bucket, key):
    pq = pytest.importorskip("pyarrow.parquet", reason="TEXT LOB layout inspection requires pyarrow")
    response = minio_client.get_object(bucket, key)
    try:
        return pq.read_table(io.BytesIO(response.read()))
    finally:
        response.close()
        response.release_conn()


def read_vortex_object(minio_client, bucket, key):
    vortex_io = pytest.importorskip("vortex.io", reason="TEXT LOB Vortex layout inspection requires vortex-data")
    response = minio_client.get_object(bucket, key)
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".vortex", delete=False) as tmp:
            tmp.write(response.read())
            tmp_path = tmp.name
        try:
            return vortex_io.read_url(f"file:{tmp_path}").to_arrow_table()
        except ValueError as e:
            if "Registry missing encoding" in str(e):
                pytest.skip(
                    "installed vortex-data cannot decode Milvus StorageV3 Vortex custom encoding; "
                    "skip instead of synthesizing TEXT LOB references"
                )
            raise
    finally:
        response.close()
        response.release_conn()
        if tmp_path is not None:
            os.remove(tmp_path)


def bytes_value(value):
    if value is None:
        return None
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    return None


def int_values(values):
    converted = []
    for value in values:
        if hasattr(value, "as_py"):
            value = value.as_py()
        if not isinstance(value, int) or isinstance(value, bool):
            return None
        converted.append(int(value))
    return converted


def column_names_for(field_id, field_name):
    return [str(field_id), field_name, f"field_{field_id}", f"{field_name}_{field_id}"]


def candidate_columns(tables, preferred_names, predicate):
    # Storage can put different fields in separate files. Prefer the named column across all files.
    preferred = []
    fallback = []
    for key, table in tables:
        for name in table.schema.names:
            target = preferred if name in preferred_names else fallback
            target.append((key, name, table))
    candidates = []
    for key, name, table in preferred or fallback:
        values = predicate(table[name].to_pylist())
        if values is not None:
            candidates.append((key, name, values))
    return candidates


def text_reference_values(values):
    converted = [bytes_value(value) for value in values]
    if any(value is not None and len(value) > 0 and value[0] in [0, 1] for value in converted):
        return converted
    return None


def text_reference_rows_from_tables(
    tables, content_field_id, id_field_id, expected_ids, layout, *, text_field_name, pk_field_name
):
    table_debug = [{"key": key, "columns": list(table.schema.names), "rows": table.num_rows} for key, table in tables]
    id_candidates = candidate_columns(tables, column_names_for(id_field_id, pk_field_name), int_values)
    ref_candidates = candidate_columns(
        tables, column_names_for(content_field_id, text_field_name), text_reference_values
    )

    expected_id_set = set(expected_ids)
    assert id_candidates, f"no primary-key column found in {layout} files: {table_debug}"
    assert ref_candidates, f"no TEXT reference column found in {layout} files: {table_debug}"

    id_key, id_name, ids = max(id_candidates, key=lambda candidate: len(expected_id_set & set(candidate[2])))
    assert expected_id_set <= set(ids), (
        f"primary-key column {id_name} from {id_key} does not contain expected ids; "
        f"expected={expected_id_set}, actual={ids}, {layout}={table_debug}"
    )

    matching_refs = [candidate for candidate in ref_candidates if len(candidate[2]) == len(ids)]
    assert matching_refs, (
        f"no TEXT reference column has the same row count as primary-key column {id_name} from {id_key}; "
        f"ref_candidates={[(key, name, len(values)) for key, name, values in ref_candidates]}, {layout}={table_debug}"
    )
    ref_key, ref_name, refs = matching_refs[0]
    log.info(f"TEXT LOB {layout} reference layout: pk={id_key}:{id_name}, text={ref_key}:{ref_name}")
    return {pk: ref for pk, ref in zip(ids, refs) if pk in expected_id_set}


def read_text_reference_rows(
    minio_client,
    bucket,
    segment_base,
    content_field_id,
    id_field_id,
    expected_ids,
    *,
    text_field_name,
    pk_field_name,
):
    data_prefix = object_key(segment_base, "_data")
    data_keys = list_minio_keys(minio_client, bucket, data_prefix)
    parquet_keys = [key for key in data_keys if key.endswith(".parquet")]
    if not parquet_keys:
        parquet_keys = [key for key in list_minio_keys(minio_client, bucket, segment_base) if key.endswith(".parquet")]

    if parquet_keys:
        layout = "parquet"
        tables = [(key, read_parquet_object(minio_client, bucket, key)) for key in parquet_keys]
    else:
        layout = "vortex"
        vortex_keys = [key for key in data_keys if key.endswith(".vortex")]
        assert vortex_keys, f"no parquet or vortex reference files found under segment base {segment_base}"
        tables = [(key, read_vortex_object(minio_client, bucket, key)) for key in vortex_keys]
    return text_reference_rows_from_tables(
        tables,
        content_field_id,
        id_field_id,
        expected_ids,
        layout,
        text_field_name=text_field_name,
        pk_field_name=pk_field_name,
    )


def classify_text_ref(ref_bytes, expected_text):
    assert ref_bytes is not None and len(ref_bytes) > 0, "TEXT reference must be non-empty bytes"
    expected_bytes = expected_text.encode("utf-8")
    if ref_bytes[0] == 0:
        assert ref_bytes[1:] == expected_bytes, (
            f"inline TEXT payload mismatch: actual_bytes={len(ref_bytes) - 1}, expected_bytes={len(expected_bytes)}"
        )
        return "inline"
    if ref_bytes[0] == 1:
        assert len(ref_bytes) == 24, f"LOB reference must be 24 bytes, got {len(ref_bytes)}"
        return "lob"
    pytest.fail(f"unknown TEXT reference flag {ref_bytes[0]!r}")


def lob_object_key(partition_base, content_field_id, ref_bytes):
    file_id = str(uuid.UUID(bytes=ref_bytes[4:20]))
    return object_key(partition_base, "lobs", content_field_id, "_data", f"{file_id}.vx")
