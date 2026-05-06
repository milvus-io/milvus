import copy
import io
import json
import os
import re
import shutil
import struct
import subprocess
import tempfile
import time
from pathlib import Path
from uuid import uuid4

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
from minio import Minio

from base.testbase import TestBase
from utils.util_log import test_log as logger
from utils.utils import gen_collection_name

DIM = 8
FORMAT_CASES = ("parquet", "lance-table", "iceberg-table", "vortex")
FORMAT_IDS = ("parquet", "lance", "iceberg", "vortex")
FORMAT_ENUM = set(FORMAT_CASES)
FIELD_DATA_TYPE_ENUM = {"Int64", "Float", "FloatVector"}
CONSISTENCY_LEVEL_ENUM = {"Strong", "Session", "Bounded", "Eventually", "Customized"}
LOAD_STATE_ENUM = {"LoadStateNotExist", "LoadStateNotLoad", "LoadStateLoading", "LoadStateLoaded"}
JOB_STATE_ENUM = {"RefreshPending", "RefreshInProgress", "RefreshCompleted", "RefreshFailed"}
TERMINAL_JOB_STATES = {"RefreshCompleted", "RefreshFailed"}
EXTFS_CLOUD_PROVIDER_ENUM = {"aws", "gcp", "aliyun", "tencent", "huawei", "azure", "minio"}
EXTFS_ALLOWED_KEYS = {
    "access_key_id",
    "access_key_value",
    "role_arn",
    "session_name",
    "external_id",
    "use_iam",
    "anonymous",
    "gcp_target_service_account",
    "region",
    "cloud_provider",
    "bucket_name",
    "iam_endpoint",
    "storage_type",
    "ssl_ca_cert",
    "use_ssl",
    "use_virtual_host",
    "load_frequency",
}
EXTFS_BOOL_KEYS = {"use_iam", "anonymous", "use_ssl", "use_virtual_host"}
REFRESH_TIMEOUT = 180
LOAD_TIMEOUT = 180


def _minio_config(minio_host, bucket_name):
    address = minio_host or "localhost"
    secure = address.startswith("https://")
    if address.startswith("http://"):
        address = address.removeprefix("http://")
    elif address.startswith("https://"):
        address = address.removeprefix("https://")
    if ":" not in address:
        address = f"{address}:9000"
    return {
        "address": address,
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "bucket": bucket_name,
        "secure": secure,
    }


def _minio_endpoint_url(cfg):
    scheme = "https" if cfg["secure"] else "http"
    return f"{scheme}://{cfg['address']}"


def _external_source(cfg, key_prefix):
    return f"s3://{cfg['address']}/{cfg['bucket']}/{key_prefix}/"


def _external_spec(cfg, fmt="parquet", snapshot_id=None):
    spec = {
        "format": fmt,
        "extfs": {
            "cloud_provider": "minio",
            "region": "us-east-1",
            "access_key_id": cfg["access_key"],
            "access_key_value": cfg["secret_key"],
            "use_ssl": "true" if cfg["secure"] else "false",
        },
    }
    if snapshot_id is not None:
        spec["snapshot_id"] = int(snapshot_id)
    return json.dumps(spec, separators=(",", ":"))


def _parse_external_spec(spec):
    assert isinstance(spec, str), spec
    parsed = json.loads(spec or "{}")
    assert isinstance(parsed, dict), parsed
    return parsed


def _assert_external_source_shape(source, allow_empty=False):
    assert isinstance(source, str), source
    if allow_empty and source == "":
        return
    assert source.startswith("s3://"), source
    assert "@" not in source.removeprefix("s3://").split("/", 1)[0], source


def _assert_snapshot_id_shape(snapshot_id):
    if isinstance(snapshot_id, int):
        assert snapshot_id > 0
        return
    assert isinstance(snapshot_id, str), snapshot_id
    assert snapshot_id.isdecimal(), snapshot_id
    assert int(snapshot_id) > 0


def _assert_external_spec_shape(spec, expected_format=None, allow_empty=False):
    assert isinstance(spec, str), spec
    if allow_empty and spec == "":
        return
    parsed = _parse_external_spec(spec)
    assert set(parsed).issubset({"format", "columns", "extfs", "snapshot_id"}), parsed

    fmt = parsed.get("format", "parquet")
    assert fmt in FORMAT_ENUM, parsed
    if expected_format is not None:
        assert fmt == expected_format

    if "columns" in parsed:
        assert isinstance(parsed["columns"], list), parsed
        assert all(isinstance(column, str) and column for column in parsed["columns"]), parsed

    if "snapshot_id" in parsed:
        _assert_snapshot_id_shape(parsed["snapshot_id"])

    extfs = parsed.get("extfs") or {}
    assert isinstance(extfs, dict), parsed
    assert set(extfs).issubset(EXTFS_ALLOWED_KEYS), parsed
    if extfs:
        cloud_provider = extfs.get("cloud_provider")
        assert cloud_provider in EXTFS_CLOUD_PROVIDER_ENUM, parsed
        for key in EXTFS_BOOL_KEYS & set(extfs):
            assert extfs[key] in {"true", "false"}, parsed
        assert bool(extfs.get("access_key_id")) == bool(extfs.get("access_key_value")), parsed


def _assert_external_spec(actual_spec, expected_spec):
    _assert_external_spec_shape(expected_spec)
    _assert_external_spec_shape(actual_spec)
    actual = json.loads(actual_spec or "{}")
    expected = json.loads(expected_spec or "{}")
    assert actual.get("format") == expected.get("format")
    if "snapshot_id" in expected:
        assert str(actual.get("snapshot_id")) == str(expected.get("snapshot_id"))

    actual_extfs = actual.get("extfs") or {}
    expected_extfs = expected.get("extfs") or {}
    for key in ("cloud_provider", "region", "use_ssl"):
        assert actual_extfs.get(key) == expected_extfs.get(key)
    for key in ("access_key_id", "access_key_value"):
        value = actual_extfs.get(key)
        assert value
        assert value == "***" or value == expected_extfs.get(key)


def _float_vectors(ids, dim):
    return np.array([[float(i) * 0.1 + d for d in range(dim)] for i in ids], dtype=np.float32)


def _expected_value(row_id):
    return float(row_id) * 1.5


def _expected_embedding(row_id, dim=DIM):
    return [float(row_id) * 0.1 + d for d in range(dim)]


def _basic_arrow_table(num_rows, start_id, dim=DIM):
    ids = list(range(start_id, start_id + num_rows))
    vectors = _float_vectors(ids, dim)
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array([_expected_value(i) for i in ids], type=pa.float32()),
            "embedding": pa.FixedSizeListArray.from_arrays(
                pa.array(vectors.flatten(), type=pa.float32()),
                list_size=dim,
            ),
        }
    )


def _basic_parquet_bytes(num_rows, start_id, dim=DIM):
    buf = io.BytesIO()
    pq.write_table(_basic_arrow_table(num_rows, start_id, dim=dim), buf, compression="snappy")
    return buf.getvalue()


def _arrow_ipc_bytes(table):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _put_bytes(minio_client, bucket, key, data):
    minio_client.put_object(
        bucket,
        key,
        io.BytesIO(data),
        length=len(data),
        content_type="application/octet-stream",
    )


def _write_parquet_dataset(minio_client, cfg, key_prefix, batches):
    for idx, (start_id, num_rows) in enumerate(batches):
        _put_bytes(
            minio_client,
            cfg["bucket"],
            f"{key_prefix}/file_{idx:03d}.parquet",
            _basic_parquet_bytes(num_rows, start_id),
        )


def _write_parquet_tables(minio_client, cfg, key_prefix, tables):
    for idx, table in enumerate(tables):
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        _put_bytes(
            minio_client,
            cfg["bucket"],
            f"{key_prefix}/file_{idx:03d}.parquet",
            buf.getvalue(),
        )


def _write_lance_tables(minio_client, cfg, key_prefix, tables):
    try:
        import lance
    except ImportError as exc:
        pytest.skip(f"lance external collection dependency unavailable: {exc}")

    tmpdir = tempfile.mkdtemp(prefix="rest_ext_lance_")
    local_path = os.path.join(tmpdir, "dataset.lance")
    try:
        for idx, table in enumerate(tables):
            mode = "create" if idx == 0 else "append"
            lance.write_dataset(table, local_path, mode=mode)
        for root, _dirs, files in os.walk(local_path):
            for filename in files:
                absolute = os.path.join(root, filename)
                relative = os.path.relpath(absolute, local_path)
                minio_client.fput_object(cfg["bucket"], f"{key_prefix}/{relative}", absolute)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _write_lance_dataset(minio_client, cfg, key_prefix, batches):
    _write_lance_tables(
        minio_client,
        cfg,
        key_prefix,
        [_basic_arrow_table(num_rows, start_id) for start_id, num_rows in batches],
    )


def _vector_bytes(ids, dim):
    return [struct.pack(f"<{dim}f", *_expected_embedding(i, dim=dim)) for i in ids]


def _table_to_iceberg_arrow(table):
    ids = table.column("id").to_pylist()
    values = table.column("value").to_pylist()
    vectors = table.column("embedding").to_pylist()
    vector_blobs = [None if vector is None else struct.pack(f"<{DIM}f", *vector) for vector in vectors]
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=True),
            pa.field("value", pa.float32(), nullable=True),
            pa.field("embedding", pa.binary(DIM * 4), nullable=True),
        ]
    )
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array(values, type=pa.float32()),
            "embedding": pa.array(vector_blobs, type=pa.binary(DIM * 4)),
        },
        schema=arrow_schema,
    )


def _write_iceberg_tables(cfg, key_prefix, tables):
    try:
        from pyiceberg.catalog.sql import SqlCatalog
        from pyiceberg.schema import Schema as IcebergSchema
        from pyiceberg.types import FixedType, FloatType, LongType, NestedField
    except ImportError as exc:
        pytest.skip(f"iceberg external collection dependency unavailable: {exc}")

    tmpdir = tempfile.mkdtemp(prefix="rest_ext_iceberg_")
    try:
        catalog = SqlCatalog(
            "milvus_rest_test",
            **{
                "uri": f"sqlite:///{tmpdir}/catalog.db",
                "warehouse": f"s3://{cfg['bucket']}/{key_prefix}",
                "s3.endpoint": _minio_endpoint_url(cfg),
                "s3.access-key-id": cfg["access_key"],
                "s3.secret-access-key": cfg["secret_key"],
                "s3.region": "us-east-1",
                "s3.path-style-access": "true",
            },
        )
        catalog.create_namespace("ext")
        iceberg_schema = IcebergSchema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "value", FloatType(), required=False),
            NestedField(3, "embedding", FixedType(DIM * 4), required=False),
        )
        table = catalog.create_table("ext.t", schema=iceberg_schema)
        for arrow_table in tables:
            table.append(_table_to_iceberg_arrow(arrow_table))

        snapshot = table.current_snapshot()
        snapshot_id = None if snapshot is None else snapshot.snapshot_id
        metadata_location = table.metadata_location
        assert metadata_location.startswith("s3://"), metadata_location
        bucket_and_key = metadata_location.removeprefix("s3://")
        return f"s3://{cfg['address']}/{bucket_and_key}", snapshot_id
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _write_iceberg_dataset(cfg, key_prefix, batches):
    return _write_iceberg_tables(
        cfg,
        key_prefix,
        [_basic_arrow_table(num_rows, start_id) for start_id, num_rows in batches],
    )


def _vortex_python():
    explicit = os.environ.get("REST_VORTEX_PYTHON")
    candidates = []
    if explicit:
        candidates.append(Path(explicit))

    rest_root = Path(__file__).resolve().parents[1]
    candidates.append(rest_root / ".venv-vortex" / "bin" / "python")
    candidates.append(rest_root.parent / "python_client" / ".venv-vortex" / "bin" / "python")

    for candidate in candidates:
        if candidate.exists():
            return candidate

    pytest.skip(
        "vortex external collection dependency unavailable: set REST_VORTEX_PYTHON "
        "or create tests/restful_client_v2/.venv-vortex with vortex-data"
    )


def _write_vortex_tables(cfg, key_prefix, tables):
    helper = Path(__file__).resolve().parent / "_vortex_gen.py"
    vortex_python = _vortex_python()
    for idx, table in enumerate(tables):
        env = {
            **os.environ,
            "MINIO_ADDRESS": cfg["address"],
            "MINIO_BUCKET": cfg["bucket"],
            "MINIO_ACCESS_KEY": cfg["access_key"],
            "MINIO_SECRET_KEY": cfg["secret_key"],
            "MINIO_SECURE": "true" if cfg["secure"] else "false",
            "VT_MINIO_KEY": f"{key_prefix}/file_{idx:03d}.vortex",
        }
        result = subprocess.run(
            [str(vortex_python), str(helper)],
            input=_arrow_ipc_bytes(table),
            env=env,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                "failed to generate vortex external data: "
                f"stdout={result.stdout.decode(errors='replace')!r}, "
                f"stderr={result.stderr.decode(errors='replace')!r}"
            )


def _write_vortex_dataset(cfg, key_prefix, batches):
    _write_vortex_tables(cfg, key_prefix, [_basic_arrow_table(num_rows, start_id) for start_id, num_rows in batches])


def _cleanup_prefix(minio_client, bucket, key_prefix):
    for obj in minio_client.list_objects(bucket, prefix=f"{key_prefix}/", recursive=True):
        minio_client.remove_object(bucket, obj.object_name)


@pytest.fixture(scope="function")
def external_store(request, minio_host, bucket_name):
    cfg = _minio_config(minio_host, bucket_name)
    minio_client = Minio(
        cfg["address"],
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        secure=cfg["secure"],
    )
    assert minio_client.bucket_exists(cfg["bucket"]), f"MinIO bucket {cfg['bucket']} is not accessible"

    safe_name = re.sub(r"[^A-Za-z0-9_-]", "_", request.node.name)
    key = f"rest-external/{safe_name}-{uuid4().hex[:8]}"
    yield {
        "client": minio_client,
        "cfg": cfg,
        "key": key,
        "source": _external_source(cfg, key),
    }
    try:
        _cleanup_prefix(minio_client, cfg["bucket"], key)
    except Exception as exc:
        logger.warning(f"cleanup external prefix {key} failed: {exc}")


def _write_format_dataset(fmt, external_store, batches):
    tables = [_basic_arrow_table(num_rows, start_id) for start_id, num_rows in batches]
    return _write_format_tables(fmt, external_store, tables)


def _write_format_tables(fmt, external_store, tables):
    assert fmt in FORMAT_ENUM
    minio_client = external_store["client"]
    cfg = external_store["cfg"]
    key = external_store["key"]
    source = external_store["source"]

    if fmt == "parquet":
        _write_parquet_tables(minio_client, cfg, key, tables)
        return source, _external_spec(cfg, fmt=fmt)
    if fmt == "lance-table":
        _write_lance_tables(minio_client, cfg, key, tables)
        return source, _external_spec(cfg, fmt=fmt)
    if fmt == "iceberg-table":
        iceberg_source, snapshot_id = _write_iceberg_tables(cfg, key, tables)
        return iceberg_source, _external_spec(cfg, fmt=fmt, snapshot_id=snapshot_id)
    if fmt == "vortex":
        _write_vortex_tables(cfg, key, tables)
        return source, _external_spec(cfg, fmt=fmt)
    raise AssertionError(f"unsupported external format: {fmt}")


def _nullable_arrow_table(num_rows=6, null_value_ids=None, null_vector_ids=None, dim=DIM):
    null_value_ids = set(null_value_ids or set())
    null_vector_ids = set(null_vector_ids or set())
    ids = list(range(num_rows))
    vector_type = pa.list_(pa.float32(), dim)
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array(
                [None if i in null_value_ids else _expected_value(i) for i in ids],
                type=pa.float32(),
            ),
            "embedding": pa.array(
                [None if i in null_vector_ids else _expected_embedding(i, dim=dim) for i in ids],
                type=vector_type,
            ),
        }
    )


def _external_collection_payload(collection_name, source, spec, vector_nullable=None):
    vector_field = {
        "fieldName": "embedding",
        "dataType": "FloatVector",
        "externalField": "embedding",
        "elementTypeParams": {"dim": str(DIM)},
    }
    if vector_nullable is not None:
        vector_field["nullable"] = vector_nullable
    return {
        "collectionName": collection_name,
        "schema": {
            "enableDynamicField": False,
            "externalSource": source,
            "externalSpec": spec,
            "fields": [
                {"fieldName": "id", "dataType": "Int64", "externalField": "id", "elementTypeParams": {}},
                {"fieldName": "value", "dataType": "Float", "externalField": "value", "elementTypeParams": {}},
                vector_field,
            ],
        },
    }


def _assert_json_object_request(payload, required_keys, optional_keys=None):
    optional_keys = optional_keys or set()
    assert isinstance(payload, dict), payload
    assert set(required_keys).issubset(payload), payload
    assert set(payload).issubset(set(required_keys) | set(optional_keys)), payload


def _assert_external_collection_create_request(payload, expected_name=None, expected_source=None, expected_spec=None):
    _assert_json_object_request(payload, {"collectionName", "schema"}, optional_keys={"dbName"})
    assert "externalSource" not in payload
    assert "externalSpec" not in payload
    assert isinstance(payload["collectionName"], str) and payload["collectionName"], payload
    if "dbName" in payload:
        assert isinstance(payload["dbName"], str) and payload["dbName"], payload
    if expected_name is not None:
        assert payload["collectionName"] == expected_name

    schema = payload["schema"]
    assert set(schema) == {"enableDynamicField", "externalSource", "externalSpec", "fields"}, schema
    assert schema["enableDynamicField"] is False
    if expected_source is not None:
        assert schema["externalSource"] == expected_source
    if expected_spec is not None:
        assert schema["externalSpec"] == expected_spec
    _assert_external_source_shape(schema["externalSource"], allow_empty=True)
    _assert_external_spec_shape(schema["externalSpec"], allow_empty=True)

    fields = schema["fields"]
    assert isinstance(fields, list) and len(fields) == 3, schema
    expected_fields = {
        "id": ("Int64", "id", {}),
        "value": ("Float", "value", {}),
        "embedding": ("FloatVector", "embedding", {"dim": str(DIM)}),
    }
    for field in fields:
        assert {"fieldName", "dataType", "externalField", "elementTypeParams"}.issubset(field), field
        assert set(field).issubset(
            {"fieldName", "dataType", "externalField", "elementTypeParams", "nullable"}
        ), field
        assert field["fieldName"] in expected_fields, field
        data_type, external_field, params = expected_fields[field["fieldName"]]
        assert field["dataType"] in FIELD_DATA_TYPE_ENUM, field
        assert field["dataType"] == data_type
        assert field["externalField"] == external_field
        assert field["elementTypeParams"] == params
        if "nullable" in field:
            assert isinstance(field["nullable"], bool), field


def _assert_refresh_request(payload, expected_collection=None, require_complete_external_tuple=True):
    _assert_json_object_request(
        payload,
        {"collectionName"},
        optional_keys={"dbName", "externalSource", "externalSpec"},
    )
    assert isinstance(payload["collectionName"], str) and payload["collectionName"], payload
    if expected_collection is not None:
        assert payload["collectionName"] == expected_collection
    if "dbName" in payload:
        assert isinstance(payload["dbName"], str) and payload["dbName"], payload

    src_set = "externalSource" in payload
    spec_set = "externalSpec" in payload
    if require_complete_external_tuple:
        assert src_set == spec_set, payload
    if src_set:
        _assert_external_source_shape(payload["externalSource"])
    if spec_set:
        _assert_external_spec_shape(payload["externalSpec"])


def _assert_job_describe_request(payload, expected_job_id=None):
    _assert_json_object_request(payload, {"jobId"}, optional_keys={"dbName"})
    assert isinstance(payload["jobId"], int), payload
    assert payload["jobId"] > 0, payload
    if expected_job_id is not None:
        assert payload["jobId"] == expected_job_id


def _assert_job_list_request(payload, expected_collection=None):
    _assert_json_object_request(payload, set(), optional_keys={"dbName", "collectionName"})
    if expected_collection is not None:
        assert payload.get("collectionName") == expected_collection
    if "collectionName" in payload:
        assert isinstance(payload["collectionName"], str) and payload["collectionName"], payload
    if "dbName" in payload:
        assert isinstance(payload["dbName"], str) and payload["dbName"], payload


def _assert_index_create_request(payload, collection_name):
    _assert_json_object_request(payload, {"collectionName", "indexParams"}, optional_keys={"dbName"})
    assert payload["collectionName"] == collection_name
    params = payload["indexParams"]
    assert isinstance(params, list) and len(params) == 1, payload
    index = params[0]
    assert index == {
        "fieldName": "embedding",
        "indexName": "embedding_vector",
        "metricType": "L2",
        "indexType": "AUTOINDEX",
        "params": {},
    }


def _assert_load_or_release_request(payload, collection_name, db_name="default"):
    expected = {"collectionName": collection_name}
    if db_name != "default":
        expected["dbName"] = db_name
        assert payload == expected
        return
    assert payload == expected or payload == {"dbName": "default", "collectionName": collection_name}


def _assert_query_count_request(payload, collection_name):
    _assert_json_object_request(
        payload,
        {"collectionName", "filter", "limit", "outputFields"},
        optional_keys={"dbName"},
    )
    assert payload["collectionName"] == collection_name
    assert isinstance(payload["filter"], str), payload
    assert payload["limit"] == 0
    assert payload["outputFields"] == ["count(*)"]


def _assert_query_rows_request(payload, collection_name, output_fields):
    _assert_json_object_request(
        payload,
        {"collectionName", "filter", "limit", "outputFields"},
        optional_keys={"dbName"},
    )
    assert payload["collectionName"] == collection_name
    assert isinstance(payload["filter"], str) and payload["filter"], payload
    assert isinstance(payload["limit"], int) and payload["limit"] > 0, payload
    assert payload["outputFields"] == output_fields
    if "dbName" in payload:
        assert isinstance(payload["dbName"], str) and payload["dbName"], payload


def _assert_search_request(payload, collection_name, output_fields):
    _assert_json_object_request(
        payload,
        {"collectionName", "data", "annsField", "limit", "outputFields"},
        optional_keys={"dbName", "filter", "searchParams"},
    )
    assert payload["collectionName"] == collection_name
    assert payload["annsField"] == "embedding"
    assert isinstance(payload["data"], list) and payload["data"], payload
    assert all(isinstance(vector, list) and len(vector) == DIM for vector in payload["data"]), payload
    assert isinstance(payload["limit"], int) and payload["limit"] > 0, payload
    assert payload["outputFields"] == output_fields
    if "filter" in payload:
        assert isinstance(payload["filter"], str) and payload["filter"], payload
    if "searchParams" in payload:
        assert isinstance(payload["searchParams"], dict), payload
    if "dbName" in payload:
        assert isinstance(payload["dbName"], str) and payload["dbName"], payload


def _assert_success_default_response(rsp):
    assert set(rsp) == {"code", "data"}, rsp
    assert rsp["code"] == 0, rsp
    assert rsp["data"] == {}, rsp


def _assert_error_response(rsp, message_substr=None):
    assert isinstance(rsp, dict), rsp
    assert "code" in rsp and isinstance(rsp["code"], int), rsp
    assert rsp["code"] != 0, rsp
    assert "message" in rsp and isinstance(rsp["message"], str) and rsp["message"], rsp
    if message_substr is not None:
        assert message_substr.lower() in rsp["message"].lower(), rsp


def _assert_refresh_response(rsp):
    assert set(rsp) == {"code", "data"}, rsp
    assert rsp["code"] == 0, rsp
    assert set(rsp["data"]) == {"jobId"}, rsp
    job_id = rsp["data"]["jobId"]
    assert isinstance(job_id, int) and job_id > 0, rsp
    return job_id


def _assert_job_info_map(job, expected_collection=None, expected_source=None, expected_job_id=None):
    required = {"jobId", "collectionName", "state", "progress", "externalSource", "startTime", "endTime"}
    assert required.issubset(job), job
    assert set(job).issubset(required | {"reason"}), job
    assert isinstance(job["jobId"], int) and job["jobId"] > 0, job
    if expected_job_id is not None:
        assert job["jobId"] == expected_job_id
    assert isinstance(job["collectionName"], str), job
    if expected_collection is not None:
        assert job["collectionName"] == expected_collection
    assert job["state"] in JOB_STATE_ENUM, job
    assert isinstance(job["progress"], int) and 0 <= job["progress"] <= 100, job
    assert isinstance(job["externalSource"], str), job
    if expected_source is not None:
        assert job["externalSource"] == expected_source
    assert isinstance(job["startTime"], int) and job["startTime"] >= 0, job
    assert isinstance(job["endTime"], int) and job["endTime"] >= 0, job
    if job["state"] == "RefreshCompleted":
        assert job["progress"] == 100, job
        assert "reason" not in job, job
    if "reason" in job:
        assert isinstance(job["reason"], str) and job["reason"], job


def _assert_job_info_response(rsp, expected_collection=None, expected_source=None, expected_job_id=None):
    assert set(rsp) == {"code", "data"}, rsp
    assert rsp["code"] == 0, rsp
    assert isinstance(rsp["data"], dict), rsp
    _assert_job_info_map(
        rsp["data"],
        expected_collection=expected_collection,
        expected_source=expected_source,
        expected_job_id=expected_job_id,
    )


def _assert_job_list_response(rsp, expected_collection=None):
    assert set(rsp) == {"code", "data"}, rsp
    assert rsp["code"] == 0, rsp
    assert set(rsp["data"]) == {"records"}, rsp
    records = rsp["data"]["records"]
    assert isinstance(records, list), rsp
    for job in records:
        _assert_job_info_map(job, expected_collection=expected_collection)
    return records


def _assert_describe_external_collection_response(
    desc,
    collection_name,
    expected_source=None,
    expected_spec=None,
    expected_vector_nullable=False,
):
    assert {"code", "data"}.issubset(desc), desc
    assert set(desc).issubset({"code", "data", "message"}), desc
    assert desc["code"] == 0, desc
    if "message" in desc:
        assert isinstance(desc["message"], str), desc
    data = desc["data"]
    required = {
        "collectionName",
        "collectionID",
        "description",
        "autoId",
        "fields",
        "functions",
        "aliases",
        "indexes",
        "load",
        "shardsNum",
        "partitionsNum",
        "consistencyLevel",
        "enableDynamicField",
        "externalSource",
        "externalSpec",
        "properties",
    }
    assert required.issubset(data), desc
    assert data["collectionName"] == collection_name
    assert isinstance(data["collectionID"], int), desc
    assert isinstance(data["autoId"], bool), desc
    assert data["enableDynamicField"] is False
    assert data["load"] in LOAD_STATE_ENUM, desc
    assert data["consistencyLevel"] in CONSISTENCY_LEVEL_ENUM, desc
    assert isinstance(data["aliases"], list), desc
    assert isinstance(data["indexes"], list), desc
    assert isinstance(data["functions"], list), desc
    assert isinstance(data["properties"], list), desc
    assert isinstance(data["shardsNum"], int) and data["shardsNum"] == 1, desc
    assert isinstance(data["partitionsNum"], int) and data["partitionsNum"] >= 1, desc
    if expected_source is not None:
        assert data["externalSource"] == expected_source
    if expected_spec is not None:
        _assert_external_spec(data["externalSpec"], expected_spec)
    else:
        _assert_external_spec_shape(data["externalSpec"], allow_empty=True)

    fields = {field["name"]: field for field in data["fields"]}
    assert {"id", "value", "embedding"}.issubset(fields), desc
    expected_fields = {
        "id": ("Int64", "id", True),
        "value": ("Float", "value", True),
        "embedding": ("FloatVector", "embedding", expected_vector_nullable),
    }
    for field_name, (field_type, external_field, nullable) in expected_fields.items():
        field = fields[field_name]
        assert {
            "name",
            "id",
            "type",
            "primaryKey",
            "partitionKey",
            "clusteringKey",
            "autoId",
            "nullable",
            "externalField",
        }.issubset(field), field
        assert field["type"] == field_type, field
        assert field["externalField"] == external_field, field
        assert field["primaryKey"] is False, field
        assert field["partitionKey"] is False, field
        assert field["clusteringKey"] is False, field
        assert field["autoId"] is False, field
        assert field["nullable"] is nullable, field
    embedding_params = {param["key"]: param["value"] for param in fields["embedding"].get("params", [])}
    assert embedding_params["dim"] == str(DIM), fields["embedding"]


def _assert_query_count_response(rsp):
    assert {"code", "data", "cost"}.issubset(rsp), rsp
    assert set(rsp).issubset(
        {
            "code",
            "data",
            "cost",
            "scanned_remote_bytes",
            "scanned_total_bytes",
            "cache_hit_ratio",
        }
    ), rsp
    assert rsp["code"] == 0, rsp
    assert isinstance(rsp["cost"], int), rsp
    assert isinstance(rsp["data"], list) and len(rsp["data"]) == 1, rsp
    row = rsp["data"][0]
    assert set(row) == {"count(*)"}, rsp
    assert isinstance(row["count(*)"], int), rsp
    return row["count(*)"]


def _assert_query_rows_response(rsp):
    assert {"code", "data", "cost"}.issubset(rsp), rsp
    assert set(rsp).issubset(
        {
            "code",
            "data",
            "cost",
            "scanned_remote_bytes",
            "scanned_total_bytes",
            "cache_hit_ratio",
        }
    ), rsp
    assert rsp["code"] == 0, rsp
    assert isinstance(rsp["data"], list), rsp
    assert isinstance(rsp["cost"], int), rsp
    return rsp["data"]


def _assert_search_response(rsp):
    assert {"code", "data"}.issubset(rsp), rsp
    assert set(rsp).issubset({"code", "data", "cost", "sessionTs", "topks"}), rsp
    assert rsp["code"] == 0, rsp
    assert isinstance(rsp["data"], list), rsp
    if "cost" in rsp:
        assert isinstance(rsp["cost"], int), rsp
    if "topks" in rsp:
        assert isinstance(rsp["topks"], list), rsp
        assert all(isinstance(topk, int) and topk >= 0 for topk in rsp["topks"]), rsp
    return rsp["data"]


def _row_id(row):
    assert "id" in row, row
    row_id = row["id"]
    if isinstance(row_id, str):
        assert row_id.isdecimal(), row
        return int(row_id)
    assert isinstance(row_id, int), row
    return row_id


def _assert_close(actual, expected, tolerance=1e-5):
    assert actual is not None
    assert abs(float(actual) - float(expected)) <= tolerance, (actual, expected)


def _assert_vector_close(actual, expected, tolerance=1e-5):
    assert isinstance(actual, list), actual
    assert len(actual) == len(expected), actual
    for actual_value, expected_value in zip(actual, expected):
        _assert_close(actual_value, expected_value, tolerance=tolerance)


def _rows_by_id(rows):
    return {_row_id(row): row for row in rows}


def _assert_basic_row_body(row, row_id, include_embedding=True):
    assert _row_id(row) == row_id
    _assert_close(row["value"], _expected_value(row_id), tolerance=1e-4)
    if include_embedding:
        _assert_vector_close(row["embedding"], _expected_embedding(row_id))


@pytest.mark.ExternalCollection
class TestRestExternalCollection(TestBase):
    def _external_job_post(self, action, payload):
        url = f"{self.endpoint}/v2/vectordb/jobs/external_collection/{action}"
        return self.collection_client.post(
            url,
            headers=self.collection_client.update_headers(),
            data=payload,
        ).json()

    def _external_job_post_with_token(self, action, payload, token):
        url = f"{self.endpoint}/v2/vectordb/jobs/external_collection/{action}"
        headers = self.collection_client.update_headers()
        headers["Authorization"] = f"Bearer {token}"
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        return response.json()

    def _external_job_raw_post(self, action, body):
        url = f"{self.endpoint}/v2/vectordb/jobs/external_collection/{action}"
        response = requests.post(
            url,
            headers=self.collection_client.update_headers(),
            data=body,
            timeout=30,
        )
        return response.json()

    def _with_db_name(self, payload, db_name="default"):
        payload = dict(payload or {})
        if self.collection_client.db_name is not None:
            db_name = self.collection_client.db_name
        if db_name != "default":
            payload["dbName"] = db_name
        return payload

    def _refresh_external_collection(self, payload, db_name="default"):
        request_payload = self._with_db_name(payload, db_name=db_name)
        return self._external_job_post("refresh", request_payload)

    def _describe_external_collection_job(self, job_id, db_name="default"):
        payload = {}
        if job_id is not None:
            payload["jobId"] = job_id
        return self._external_job_post("describe", self._with_db_name(payload, db_name=db_name))

    def _list_external_collection_jobs(self, payload=None, db_name="default"):
        return self._external_job_post("list", self._with_db_name(payload or {}, db_name=db_name))

    def _wait_refresh_completed(self, job_id, db_name="default", timeout=REFRESH_TIMEOUT, interval=2):
        t0 = time.time()
        last_rsp = None
        while time.time() - t0 < timeout:
            last_rsp = self._describe_external_collection_job(job_id, db_name=db_name)
            if last_rsp.get("code") != 0:
                return last_rsp, False
            state = last_rsp.get("data", {}).get("state")
            if state == "RefreshCompleted":
                return last_rsp, True
            if state == "RefreshFailed":
                return last_rsp, False
            time.sleep(interval)
        return last_rsp, False

    def _refresh_and_wait(self, collection_name, source=None, spec=None, db_name="default"):
        payload = {"collectionName": collection_name}
        if source is not None:
            payload["externalSource"] = source
        if spec is not None:
            payload["externalSpec"] = spec
        request_payload = self._with_db_name(payload, db_name=db_name)
        _assert_refresh_request(request_payload, expected_collection=collection_name)
        rsp = self._external_job_post("refresh", request_payload)
        job_id = _assert_refresh_response(rsp)

        progress, finished = self._wait_refresh_completed(
            job_id,
            db_name=db_name,
            timeout=REFRESH_TIMEOUT,
        )
        assert finished, progress
        _assert_job_info_response(
            progress,
            expected_collection=collection_name,
            expected_source=source,
            expected_job_id=job_id,
        )
        assert progress["data"]["state"] in TERMINAL_JOB_STATES, progress
        return job_id, progress

    def _create_external_collection(self, collection_name, source, spec, db_name="default", vector_nullable=None):
        payload = _external_collection_payload(collection_name, source, spec, vector_nullable=vector_nullable)
        if db_name != "default":
            payload["dbName"] = db_name
        _assert_external_collection_create_request(
            payload,
            expected_name=collection_name,
            expected_source=source,
            expected_spec=spec,
        )
        rsp = self.collection_client.collection_create(copy.deepcopy(payload))
        _assert_success_default_response(rsp)
        return rsp

    def _create_vector_index(self, collection_name, db_name="default"):
        payload = {
            "collectionName": collection_name,
            "indexParams": [
                {
                    "fieldName": "embedding",
                    "indexName": "embedding_vector",
                    "metricType": "L2",
                    "indexType": "AUTOINDEX",
                    "params": {},
                }
            ],
        }
        if db_name != "default":
            payload["dbName"] = db_name
        _assert_index_create_request(payload, collection_name)
        rsp = self.index_client.index_create(payload, db_name=db_name)
        _assert_success_default_response(rsp)

    def _load_and_wait(self, collection_name, db_name="default", expected_vector_nullable=False):
        payload = {"collectionName": collection_name}
        if db_name != "default":
            payload["dbName"] = db_name
        _assert_load_or_release_request(payload, collection_name, db_name=db_name)
        rsp = self.collection_client.collection_load(collection_name=collection_name, db_name=db_name)
        _assert_success_default_response(rsp)
        deadline = time.time() + LOAD_TIMEOUT
        last_rsp = None
        while time.time() < deadline:
            last_rsp = self.collection_client.collection_describe(collection_name, db_name=db_name)
            if last_rsp.get("code") == 0:
                _assert_describe_external_collection_response(
                    last_rsp,
                    collection_name,
                    expected_vector_nullable=expected_vector_nullable,
                )
            if last_rsp.get("data", {}).get("load") == "LoadStateLoaded":
                return
            time.sleep(2)
        assert False, f"collection {collection_name} did not load: {last_rsp}"

    def _query_count(self, collection_name, filter_expr=" ", db_name="default"):
        payload = {
            "collectionName": collection_name,
            "filter": filter_expr,
            "limit": 0,
            "outputFields": ["count(*)"],
        }
        if db_name != "default":
            payload["dbName"] = db_name
        _assert_query_count_request(payload, collection_name)
        rsp = self.vector_client.vector_query(payload, db_name=db_name, timeout=30)
        return _assert_query_count_response(rsp)

    def _query_rows(self, collection_name, filter_expr, output_fields, limit=100, db_name="default"):
        payload = {
            "collectionName": collection_name,
            "filter": filter_expr,
            "limit": limit,
            "outputFields": output_fields,
        }
        if db_name != "default":
            payload["dbName"] = db_name
        _assert_query_rows_request(payload, collection_name, output_fields)
        rsp = self.vector_client.vector_query(payload, db_name=db_name, timeout=30)
        return _assert_query_rows_response(rsp)

    def _search_rows(self, collection_name, query_vector, output_fields, limit=3, db_name="default"):
        payload = {
            "collectionName": collection_name,
            "data": [query_vector],
            "annsField": "embedding",
            "filter": "id >= 0",
            "limit": limit,
            "outputFields": output_fields,
            "searchParams": {"metricType": "L2"},
        }
        if db_name != "default":
            payload["dbName"] = db_name
        _assert_search_request(payload, collection_name, output_fields)
        rsp = self.vector_client.vector_search(payload, db_name=db_name, timeout=30)
        return _assert_search_response(rsp)

    def _assert_job_list_contains(self, collection_name, job_id, db_name="default"):
        by_collection_payload = {"collectionName": collection_name}
        _assert_job_list_request(by_collection_payload, expected_collection=collection_name)
        by_collection = self._list_external_collection_jobs(by_collection_payload, db_name=db_name)
        by_collection_records = _assert_job_list_response(by_collection, expected_collection=collection_name)
        assert job_id in [job["jobId"] for job in by_collection_records]

        all_jobs_payload = {}
        _assert_job_list_request(all_jobs_payload)
        all_jobs = self._list_external_collection_jobs(all_jobs_payload, db_name=db_name)
        all_records = _assert_job_list_response(all_jobs)
        assert job_id in [job["jobId"] for job in all_records]

    @pytest.mark.L0
    def test_rest_external_collection_create_describe_metadata_parquet(self, external_store):
        name = gen_collection_name()
        batches = [(0, 10)]
        source, spec = _write_format_dataset("parquet", external_store, batches)

        self._create_external_collection(name, source, spec)

        desc = self.collection_client.collection_describe(name)
        _assert_describe_external_collection_response(desc, name, expected_source=source, expected_spec=spec)

        fields = {field["name"]: field for field in desc["data"]["fields"]}
        assert fields["id"]["externalField"] == "id"
        assert fields["value"]["externalField"] == "value"
        assert fields["embedding"]["externalField"] == "embedding"

    @pytest.mark.L1
    @pytest.mark.parametrize("fmt", FORMAT_CASES, ids=FORMAT_IDS)
    def test_rest_external_collection_refresh_query_by_format(self, fmt, external_store):
        name = gen_collection_name()
        batches = [(0, 12), (1000, 8)]
        total_rows = sum(num_rows for _start_id, num_rows in batches)
        source, spec = _write_format_dataset(fmt, external_store, batches)

        self._create_external_collection(name, source, spec)
        job_id, progress = self._refresh_and_wait(name)
        assert progress["data"]["collectionName"] == name
        assert progress["data"]["externalSource"] == source
        self._assert_job_list_contains(name, job_id)

        self._create_vector_index(name)
        self._load_and_wait(name)
        assert self._query_count(name) == total_rows
        assert self._query_count(name, "id >= 1000") == 8

        rows = self._query_rows(
            name,
            "id in [0, 1000, 1007]",
            output_fields=["id", "value", "embedding"],
            limit=3,
        )
        assert len(rows) == 3, rows
        rows_by_id = _rows_by_id(rows)
        assert set(rows_by_id) == {0, 1000, 1007}, rows
        for row_id in (0, 1000, 1007):
            _assert_basic_row_body(rows_by_id[row_id], row_id)

        hits = self._search_rows(name, _expected_embedding(0), output_fields=["id", "value"], limit=3)
        assert len(hits) == 3, hits
        assert _row_id(hits[0]) == 0, hits
        for hit in hits:
            row_id = _row_id(hit)
            _assert_close(hit["value"], _expected_value(row_id), tolerance=1e-4)
            if "distance" in hit:
                assert isinstance(hit["distance"], (int, float)), hit

    @pytest.mark.L1
    def test_rest_external_collection_refresh_override_persists_and_reuses(self, external_store):
        name = gen_collection_name()
        cfg = external_store["cfg"]
        minio_client = external_store["client"]
        root_key = external_store["key"]
        key_a = f"{root_key}/source_a"
        key_b = f"{root_key}/source_b"
        source_a = _external_source(cfg, key_a)
        source_b = _external_source(cfg, key_b)
        spec = _external_spec(cfg)
        _write_parquet_dataset(minio_client, cfg, key_a, [(0, 20)])
        _write_parquet_dataset(minio_client, cfg, key_b, [(1000, 20)])

        self._create_external_collection(name, source_a, spec)
        self._refresh_and_wait(name)
        self._create_vector_index(name)
        self._load_and_wait(name)
        assert self._query_count(name, "id < 1000") == 20
        assert self._query_count(name, "id >= 1000") == 0

        _assert_load_or_release_request({"collectionName": name}, name)
        rsp = self.collection_client.collection_release(collection_name=name)
        _assert_success_default_response(rsp)
        self._refresh_and_wait(name, source=source_b, spec=spec)
        self._load_and_wait(name)
        assert self._query_count(name, "id < 1000") == 0
        assert self._query_count(name, "id >= 1000") == 20

        desc = self.collection_client.collection_describe(name)
        _assert_describe_external_collection_response(desc, name, expected_source=source_b, expected_spec=spec)

        _assert_load_or_release_request({"collectionName": name}, name)
        rsp = self.collection_client.collection_release(collection_name=name)
        _assert_success_default_response(rsp)
        self._refresh_and_wait(name)
        self._load_and_wait(name)
        assert self._query_count(name, "id >= 1000") == 20

    @pytest.mark.L1
    def test_rest_external_collection_create_rejections(self, external_store):
        source = external_store["source"]
        cfg = external_store["cfg"]
        spec = _external_spec(external_store["cfg"])
        extfs = _parse_external_spec(spec)["extfs"]

        source_without_spec = _external_collection_payload(gen_collection_name(), source, "")
        spec_without_source = _external_collection_payload(gen_collection_name(), "", spec)
        duplicate_external_field = _external_collection_payload(gen_collection_name(), source, spec)
        duplicate_external_field["schema"]["fields"][1]["externalField"] = "id"
        unsupported_field_type = _external_collection_payload(gen_collection_name(), source, spec)
        unsupported_field_type["schema"]["fields"][1] = {
            "fieldName": "sparse_embedding",
            "dataType": "SparseFloatVector",
            "externalField": "sparse_embedding",
            "elementTypeParams": {},
        }
        invalid_data_type_enum = _external_collection_payload(gen_collection_name(), source, spec)
        invalid_data_type_enum["schema"]["fields"][2]["dataType"] = "floatvector"
        dynamic_field_enabled = _external_collection_payload(gen_collection_name(), source, spec)
        dynamic_field_enabled["schema"]["enableDynamicField"] = True
        multi_shard_external = _external_collection_payload(gen_collection_name(), source, spec)
        multi_shard_external["params"] = {"shardsNum": 2}
        invalid_consistency = _external_collection_payload(gen_collection_name(), source, spec)
        invalid_consistency["params"] = {"consistencyLevel": "Linearizable"}
        invalid_format_spec = json.dumps({"format": "csv", "extfs": extfs}, separators=(",", ":"))
        invalid_format = _external_collection_payload(gen_collection_name(), source, invalid_format_spec)
        invalid_bool_extfs = dict(extfs)
        invalid_bool_extfs["use_ssl"] = "maybe"
        invalid_bool_spec = json.dumps({"format": "parquet", "extfs": invalid_bool_extfs}, separators=(",", ":"))
        invalid_use_ssl = _external_collection_payload(gen_collection_name(), source, invalid_bool_spec)
        missing_cloud_provider_extfs = dict(extfs)
        missing_cloud_provider_extfs.pop("cloud_provider")
        missing_cloud_provider_spec = json.dumps(
            {"format": "parquet", "extfs": missing_cloud_provider_extfs},
            separators=(",", ":"),
        )
        missing_cloud_provider = _external_collection_payload(
            gen_collection_name(), source, missing_cloud_provider_spec
        )
        source_with_credentials = f"s3://ak:sk@{cfg['address']}/{cfg['bucket']}/bad-source/"
        userinfo_source = _external_collection_payload(gen_collection_name(), source_with_credentials, spec)
        no_scheme_source = _external_collection_payload(
            gen_collection_name(),
            f"{cfg['bucket']}/bad-source/",
            spec,
        )

        cases = [
            (
                "top_level_external_config",
                {
                    "collectionName": gen_collection_name(),
                    "externalSource": source,
                    "externalSpec": spec,
                    "schema": _external_collection_payload("unused", "", "")["schema"],
                },
                "schema.externalSource/schema.externalSpec",
            ),
            (
                "quick_create_external_collection",
                {
                    "collectionName": gen_collection_name(),
                    "dimension": DIM,
                    "schema": {"externalSource": source, "externalSpec": spec},
                },
                "quick create",
            ),
            ("source_without_spec", source_without_spec, "both set or both empty"),
            ("spec_without_source", spec_without_source, "both set or both empty"),
            (
                "missing_external_field",
                {
                    "collectionName": gen_collection_name(),
                    "schema": {
                        "externalSource": source,
                        "externalSpec": spec,
                        "fields": [
                            {"fieldName": "id", "dataType": "Int64", "elementTypeParams": {}},
                            {
                                "fieldName": "embedding",
                                "dataType": "FloatVector",
                                "externalField": "embedding",
                                "elementTypeParams": {"dim": str(DIM)},
                            },
                        ],
                    },
                },
                "external",
            ),
            ("duplicate_external_field", duplicate_external_field, "mapped by multiple fields"),
            ("unsupported_field_type", unsupported_field_type, "does not support field type"),
            ("invalid_data_type_enum", invalid_data_type_enum, "invalid(case sensitive)"),
            ("dynamic_field_enabled", dynamic_field_enabled, "does not support dynamic field"),
            ("multi_shard_external", multi_shard_external, "multiple shards"),
            ("invalid_consistency_enum", invalid_consistency, "consistencyLevel can only be"),
            ("invalid_format_enum", invalid_format, "unsupported format"),
            ("invalid_extfs_bool_enum", invalid_use_ssl, 'must be "true" or "false"'),
            ("missing_cloud_provider", missing_cloud_provider, "cloud_provider is required"),
            ("source_with_credentials", userinfo_source, "must not embed credentials"),
            ("source_without_scheme", no_scheme_source, "explicit scheme"),
        ]

        for case_name, payload, message in cases:
            rsp = self.collection_client.collection_create(copy.deepcopy(payload))
            _assert_error_response(rsp, message)

    @pytest.mark.L1
    def test_rest_external_collection_refresh_rejections(self, external_store):
        source = external_store["source"]
        spec = _external_spec(external_store["cfg"])
        external_name = gen_collection_name()
        self._create_external_collection(external_name, source, spec)

        extfs = _parse_external_spec(spec)["extfs"]
        invalid_format_spec = json.dumps({"format": "csv", "extfs": extfs}, separators=(",", ":"))
        invalid_bool_extfs = dict(extfs)
        invalid_bool_extfs["anonymous"] = "maybe"
        invalid_bool_spec = json.dumps({"format": "parquet", "extfs": invalid_bool_extfs}, separators=(",", ":"))
        missing_cloud_provider_extfs = dict(extfs)
        missing_cloud_provider_extfs.pop("cloud_provider")
        missing_cloud_provider_spec = json.dumps(
            {"format": "parquet", "extfs": missing_cloud_provider_extfs},
            separators=(",", ":"),
        )

        invalid_refresh_cases = [
            ({}, "missing required parameters"),
            ({"collectionName": external_name, "externalSource": source}, "both"),
            ({"collectionName": external_name, "externalSpec": spec}, "both"),
            (
                {
                    "collectionName": external_name,
                    "externalSource": "file:///tmp/data/",
                    "externalSpec": spec,
                },
                "scheme",
            ),
            (
                {
                    "collectionName": external_name,
                    "externalSource": source,
                    "externalSpec": invalid_format_spec,
                },
                "unsupported format",
            ),
            (
                {
                    "collectionName": external_name,
                    "externalSource": source,
                    "externalSpec": invalid_bool_spec,
                },
                'must be "true" or "false"',
            ),
            (
                {
                    "collectionName": external_name,
                    "externalSource": source,
                    "externalSpec": missing_cloud_provider_spec,
                },
                "cloud_provider is required",
            ),
        ]
        for payload, message in invalid_refresh_cases:
            rsp = self._refresh_external_collection(payload)
            _assert_error_response(rsp, message)

        deferred_name = gen_collection_name()
        self._create_external_collection(deferred_name, "", "")
        rsp = self._refresh_external_collection({"collectionName": deferred_name})
        _assert_error_response(rsp, "no persisted external_source/external_spec")

        empty_list_payload = {"collectionName": deferred_name}
        _assert_job_list_request(empty_list_payload, expected_collection=deferred_name)
        rsp = self._list_external_collection_jobs(empty_list_payload)
        assert _assert_job_list_response(rsp, expected_collection=deferred_name) == []

        regular_name = gen_collection_name()
        rsp = self.collection_client.collection_create({"collectionName": regular_name, "dimension": DIM})
        _assert_success_default_response(rsp)
        rsp = self._refresh_external_collection({"collectionName": regular_name})
        _assert_error_response(rsp, "not an external collection")

        rsp = self._list_external_collection_jobs({"collectionName": regular_name})
        _assert_error_response(rsp, "not an external collection")

        rsp = self._describe_external_collection_job(None)
        _assert_error_response(rsp, "missing required parameters")

        rsp = self._external_job_post("describe", {"jobId": "abc"})
        _assert_error_response(rsp, "json")

        rsp = self._describe_external_collection_job(9223372036854775807)
        _assert_error_response(rsp, "not found")

    @pytest.mark.L1
    def test_rest_external_collection_raw_request_body_validation(self):
        cases = [
            ("describe", "", "request body"),
            ("describe", "{", "json format request"),
            ("refresh", "[]", "json format request"),
        ]
        for action, body, message in cases:
            rsp = self._external_job_raw_post(action, body)
            _assert_error_response(rsp, message)

    @pytest.mark.L1
    @pytest.mark.parametrize("fmt", FORMAT_CASES, ids=FORMAT_IDS)
    def test_rest_external_collection_zero_row_refresh_boundary_by_format(self, fmt, external_store):
        name = gen_collection_name()
        source, spec = _write_format_dataset(fmt, external_store, [(0, 0)])

        self._create_external_collection(name, source, spec)
        payload = {"collectionName": name}
        _assert_refresh_request(payload, expected_collection=name)
        rsp = self._refresh_external_collection(payload)
        job_id = _assert_refresh_response(rsp)

        progress, finished = self._wait_refresh_completed(job_id)
        _assert_job_info_response(progress, expected_collection=name, expected_job_id=job_id)
        assert progress["data"]["state"] in TERMINAL_JOB_STATES, progress
        if finished:
            self._create_vector_index(name)
            self._load_and_wait(name)
            assert self._query_count(name) == 0
        else:
            assert progress["data"]["state"] == "RefreshFailed", progress
            assert progress["data"]["reason"], progress
        self._assert_job_list_contains(name, job_id)

    @pytest.mark.L1
    @pytest.mark.parametrize("fmt", FORMAT_CASES, ids=FORMAT_IDS)
    def test_rest_external_collection_nullable_scalar_by_format(self, fmt, external_store):
        name = gen_collection_name()
        table = _nullable_arrow_table(num_rows=6, null_value_ids={1})
        source, spec = _write_format_tables(fmt, external_store, [table])

        self._create_external_collection(name, source, spec)
        self._refresh_and_wait(name)
        self._create_vector_index(name)
        self._load_and_wait(name)

        rows = self._query_rows(
            name,
            "id in [1, 2]",
            output_fields=["id", "value", "embedding"],
            limit=2,
        )
        assert len(rows) == 2, rows
        rows_by_id = _rows_by_id(rows)
        assert rows_by_id[1]["value"] is None, rows_by_id[1]
        _assert_vector_close(rows_by_id[1]["embedding"], _expected_embedding(1))
        _assert_basic_row_body(rows_by_id[2], 2)

    @pytest.mark.L1
    @pytest.mark.xfail(
        reason="milvus#49519 external collection nullable vector query output is currently incorrect",
        strict=True,
    )
    @pytest.mark.parametrize("fmt", FORMAT_CASES, ids=FORMAT_IDS)
    def test_rest_external_collection_nullable_vector_by_format(self, fmt, external_store):
        name = gen_collection_name()
        table = _nullable_arrow_table(num_rows=6, null_vector_ids={2})
        source, spec = _write_format_tables(fmt, external_store, [table])

        self._create_external_collection(name, source, spec, vector_nullable=True)
        desc = self.collection_client.collection_describe(name)
        _assert_describe_external_collection_response(
            desc,
            name,
            expected_source=source,
            expected_spec=spec,
            expected_vector_nullable=True,
        )
        self._refresh_and_wait(name)
        self._create_vector_index(name)
        self._load_and_wait(name, expected_vector_nullable=True)

        rows = self._query_rows(
            name,
            "id in [1, 2, 3]",
            output_fields=["id", "value", "embedding"],
            limit=3,
        )
        assert len(rows) == 3, rows
        rows_by_id = _rows_by_id(rows)
        _assert_basic_row_body(rows_by_id[1], 1)
        assert rows_by_id[2]["embedding"] is None, rows_by_id[2]
        _assert_close(rows_by_id[2]["value"], _expected_value(2), tolerance=1e-4)
        _assert_basic_row_body(rows_by_id[3], 3)

    @pytest.mark.L1
    def test_rest_external_collection_custom_db_e2e(self, external_store):
        db_name = f"db_{uuid4().hex[:8]}"
        rsp = self.database_client.database_create({"dbName": db_name})
        _assert_success_default_response(rsp)

        name = gen_collection_name()
        source, spec = _write_format_dataset("parquet", external_store, [(0, 10)])

        self._create_external_collection(name, source, spec, db_name=db_name)
        desc = self.collection_client.collection_describe(name, db_name=db_name)
        _assert_describe_external_collection_response(desc, name, expected_source=source, expected_spec=spec)
        job_id, _progress = self._refresh_and_wait(name, db_name=db_name)
        self._assert_job_list_contains(name, job_id, db_name=db_name)
        self._create_vector_index(name, db_name=db_name)
        self._load_and_wait(name, db_name=db_name)
        assert self._query_count(name, db_name=db_name) == 10

        rows = self._query_rows(
            name,
            "id == 0",
            output_fields=["id", "value", "embedding"],
            limit=1,
            db_name=db_name,
        )
        assert len(rows) == 1, rows
        _assert_basic_row_body(rows[0], 0)

    @pytest.mark.L1
    def test_rest_external_collection_job_api_invalid_token(self):
        payloads = [
            ("refresh", {"collectionName": "missing_external_collection"}),
            ("describe", {"jobId": 9223372036854775807}),
            ("list", {}),
        ]
        responses = [
            self._external_job_post_with_token(action, payload, self.invalid_api_key)
            for action, payload in payloads
        ]
        if all(rsp.get("code") != 1800 for rsp in responses):
            pytest.skip("authorization is not enforced for this deployment")
        for rsp in responses:
            assert rsp["code"] == 1800, rsp
            assert "message" in rsp and rsp["message"], rsp

    @pytest.mark.L1
    def test_rest_external_collection_duplicate_refresh_rejected_or_reuses_job(self, external_store):
        name = gen_collection_name()
        source, spec = _write_format_dataset("parquet", external_store, [(0, 20_000)])

        self._create_external_collection(name, source, spec)
        payload = {"collectionName": name}
        _assert_refresh_request(payload, expected_collection=name)
        first_rsp = self._refresh_external_collection(payload)
        first_job_id = _assert_refresh_response(first_rsp)

        second_rsp = self._refresh_external_collection(payload)
        if second_rsp.get("code") == 0:
            second_job_id = _assert_refresh_response(second_rsp)
            assert second_job_id == first_job_id, (
                f"duplicate refresh returned a different job id: first={first_job_id}, second={second_job_id}"
            )
        else:
            _assert_error_response(second_rsp)
            message = second_rsp["message"].lower()
            assert any(token in message for token in ("already", "duplicate", "in progress", "precondition", "exist"))

        progress, finished = self._wait_refresh_completed(first_job_id)
        assert finished, progress
        _assert_job_info_response(progress, expected_collection=name, expected_job_id=first_job_id)
        self._assert_job_list_contains(name, first_job_id)

    @pytest.mark.L1
    def test_rest_external_collection_job_list_includes_multiple_refreshes(self, external_store):
        name = gen_collection_name()
        source, spec = _write_format_dataset("parquet", external_store, [(0, 100)])

        self._create_external_collection(name, source, spec)
        first_job_id, _ = self._refresh_and_wait(name)
        second_job_id, _ = self._refresh_and_wait(name)

        payload = {"collectionName": name}
        _assert_job_list_request(payload, expected_collection=name)
        rsp = self._list_external_collection_jobs(payload)
        records = _assert_job_list_response(rsp, expected_collection=name)
        listed_ids = [record["jobId"] for record in records]
        assert first_job_id in listed_ids, records
        assert second_job_id in listed_ids, records

    @pytest.mark.L1
    @pytest.mark.xfail(
        reason="REST external job endpoints currently ignore unknown top-level JSON keys",
        strict=True,
    )
    def test_rest_external_collection_unknown_top_level_keys_rejected(self, external_store):
        name = gen_collection_name()
        source, spec = _write_format_dataset("parquet", external_store, [(0, 10)])
        create_payload = _external_collection_payload(name, source, spec)
        create_payload["unexpected"] = "reject-me"
        rsp = self.collection_client.collection_create(copy.deepcopy(create_payload))
        _assert_error_response(rsp, "unexpected")

        self._create_external_collection(name, source, spec)
        for action, payload in (
            ("refresh", {"collectionName": name, "unexpected": "reject-me"}),
            ("describe", {"jobId": 9223372036854775807, "unexpected": "reject-me"}),
            ("list", {"unexpected": "reject-me"}),
        ):
            rsp = self._external_job_post(action, payload)
            _assert_error_response(rsp, "unexpected")

    @pytest.mark.L1
    def test_rest_external_collection_add_field_external_mapping_rejected(self):
        name = gen_collection_name()
        rsp = self.collection_client.collection_create({"collectionName": name, "dimension": DIM})
        _assert_success_default_response(rsp)

        field_schema = {
            "fieldName": "external_value",
            "dataType": "Int64",
            "externalField": "remote_value",
            "elementTypeParams": {},
        }
        assert field_schema["dataType"] in FIELD_DATA_TYPE_ENUM
        assert set(field_schema) == {"fieldName", "dataType", "externalField", "elementTypeParams"}
        rsp = self.collection_client.add_field(name, field_schema)
        _assert_error_response(rsp, "external field mapping")
