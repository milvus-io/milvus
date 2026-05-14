"""MilvusClient external table tests."""

import json
import os
import random
import re
import time

import numpy as np
import pyarrow as pa
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.external_table_common import (
    BASIC_FORMAT_IDS,
    BASIC_FORMATS,
    FORMAT_NUM_FILES,
    FORMAT_ROWS_PER_FILE,
    FORMAT_TOTAL_ROWS,
    FULL_MATRIX_NB,
    REFRESH_TIMEOUT,
    _build_iceberg_full_matrix_table,
    _float_vectors,
    _full_matrix_arrow_columns,
    _full_matrix_assert_basic,
    _iceberg_full_matrix_assert,
    build_external_source,
    build_external_spec,
    build_full_matrix_schema,
    build_iceberg_full_matrix_schema,
    cleanup_minio_prefix,
    create_full_matrix_indexes,
    create_iceberg_full_matrix_indexes,
    gen_all_null_columns_parquet_bytes,
    gen_all_scalar_parquet_bytes,
    gen_array_parquet_bytes,
    gen_basic_parquet_bytes,
    gen_dtype_mismatch_parquet_bytes,
    gen_full_matrix_parquet_bytes,
    gen_geometry_wkt_parquet_bytes,
    gen_large_parquet_with_row_groups,
    gen_multi_vector_parquet_bytes,
    gen_parquet_bytes,
    gen_parquet_bytes_with_codec,
    gen_timestamptz_parquet_bytes,
    gen_vector_variant_parquet_bytes,
    get_minio_config,
    new_minio_client,
    upload_basic_data,
    upload_parquet,
    write_basic_format_dataset,
    write_vortex_table,
)
from pymilvus import AnnSearchRequest, DataType, RRFRanker
from tenacity import Retrying, retry_if_result, stop_after_delay, wait_fixed
from utils.util_log import test_log as log

REFRESH_POLL_INTERVAL_SECONDS = 2
VORTEX_FULL_MATRIX_EXCLUDED_FIELDS = ("vc_trie", "j", "f16v", "bf16v", "binv_flat", "binv_ivf")

ALLOWED_EXTERNAL_SOURCE_SCHEMES = (pytest.param("minio", id="minio"),)
_PLACEHOLDER_SRC = "s3://localhost:9000/milvus-bucket/placeholder/"
_PLACEHOLDER_SPEC = build_external_spec()
_PLACEHOLDER_NEW_SOURCE = "minio://localhost:9000/milvus-bucket/new/"
_PLACEHOLDER_NEW_SPEC = build_external_spec()


def assert_external_spec_persisted(actual_spec, expected_spec, context):
    actual = json.loads(actual_spec or "{}")
    expected = json.loads(expected_spec or "{}")
    assert actual.get("format") == expected.get("format"), f"{context}: format mismatch: {actual}"

    actual_extfs = actual.get("extfs") or {}
    expected_extfs = expected.get("extfs") or {}
    for key in ("cloud_provider", "region", "use_ssl"):
        assert actual_extfs.get(key) == expected_extfs.get(key), f"{context}: extfs.{key} mismatch: {actual_extfs}"

    for key in ("access_key_id", "access_key_value"):
        value = actual_extfs.get(key)
        assert value, f"{context}: extfs.{key} missing: {actual_extfs}"
        assert value == "***" or value == expected_extfs.get(key), f"{context}: extfs.{key} mismatch: {actual_extfs}"


def _build_basic_schema(self, client, ext_path, dim=ct.default_dim, ext_spec=None):
    schema = self.create_schema(client, external_source=ext_path, external_spec=ext_spec or build_external_spec())[0]
    self.add_field(schema, "id", DataType.INT64, external_field="id")
    self.add_field(schema, "value", DataType.FLOAT, external_field="value")
    self.add_field(schema, "embedding", DataType.FLOAT_VECTOR, dim=dim, external_field="embedding")
    return schema


def _assert_basic_format_rows(self, client, coll, fmt, batches, dim=ct.default_dim):
    total = sum(num_rows for _start_id, num_rows in batches)
    assert self.query_count(client, coll) == total
    for start_id, num_rows in batches:
        if num_rows == 0:
            continue
        rows = self.query(client, coll, filter=f"id == {start_id}", output_fields=["id", "value", "embedding"])[0]
        assert len(rows) == 1, f"[{fmt}] row id={start_id} missing"
        assert abs(rows[0]["value"] - start_id * 1.5) < 1e-3
        vec = rows[0]["embedding"]
        assert len(vec) == dim
        for j, v in enumerate(vec):
            expected = float(start_id) * 0.1 + j
            assert abs(v - expected) < 1e-2, f"[{fmt}] vec[{j}] for id={start_id} = {v}"


# ============================================================
# Fixtures
# ============================================================


@pytest.fixture(scope="module")
def minio_env(request):
    """Shared MinIO client + config for the module."""
    cfg = get_minio_config(
        minio_host=request.config.getoption("--minio_host"),
        minio_bucket=request.config.getoption("--minio_bucket"),
    )
    mc = new_minio_client(cfg)
    assert mc.bucket_exists(cfg["bucket"]), f"MinIO bucket {cfg['bucket']} not accessible at {cfg['address']}"
    return mc, cfg


@pytest.fixture(scope="function")
def external_prefix(minio_env, request):
    """Per-test MinIO prefix bundled with the resolved external_source URL."""
    mc, cfg = minio_env
    # Sanitize node.name aggressively — parametrize IDs can contain chars MinIO or
    # Milvus's S3 URI parser rejects. Keep only [A-Za-z0-9_-].
    safe = re.sub(r"[^A-Za-z0-9_-]", "_", request.node.name)
    key = f"external-e2e-core/{safe}-{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
    yield {"key": key, "url": build_external_source(cfg, key)}
    cleanup_minio_prefix(mc, cfg["bucket"], f"{key}/")


class ExternalTableTestBase(TestMilvusClientV2Base):
    """Shared base for external table MilvusClient wrapper tests."""

    def query_count(self, client, collection_name):
        res = self.query(client, collection_name, filter="", output_fields=["count(*)"])[0]
        return res[0]["count(*)"]

    def wait_until(self, probe, is_done, timeout, description, interval=REFRESH_POLL_INTERVAL_SECONDS):
        last = None

        def _probe_once():
            nonlocal last
            last = probe()
            return is_done(last)

        completed = Retrying(
            stop=stop_after_delay(timeout),
            wait=wait_fixed(interval),
            retry=retry_if_result(lambda done: not done),
            retry_error_callback=lambda _retry_state: False,
        )(_probe_once)
        assert completed, f"{description} did not finish in {timeout}s, last={last}"
        return last

    def wait_refresh_progress(
        self,
        client,
        job_id,
        timeout=REFRESH_TIMEOUT,
        terminal_states=("RefreshCompleted", "RefreshFailed"),
        return_on_reason=False,
    ):
        return self.wait_until(
            lambda: self.get_refresh_external_collection_progress(client, job_id=job_id)[0],
            lambda progress: progress.state in terminal_states or (return_on_reason and bool(progress.reason)),
            timeout=timeout,
            description=f"refresh job {job_id}",
        )

    def refresh_and_wait(
        self,
        client,
        collection_name,
        timeout=REFRESH_TIMEOUT,
        external_source=None,
        external_spec=None,
    ):
        kwargs = {"collection_name": collection_name}
        if external_source is not None:
            kwargs["external_source"] = external_source
        if external_spec is not None:
            kwargs["external_spec"] = external_spec

        job_id = self.refresh_external_collection(client, **kwargs)[0]
        progress = self.wait_refresh_progress(client, job_id, timeout=timeout)
        assert progress.state != "RefreshFailed", f"refresh failed: job_id={job_id}, reason={progress.reason}"
        assert progress.state == "RefreshCompleted", f"refresh job {job_id} ended in {progress.state}"
        return job_id

    def refresh_and_poll_terminal(
        self,
        client,
        collection_name,
        timeout=60,
        external_source=None,
        external_spec=None,
        return_on_reason=False,
    ):
        kwargs = {"collection_name": collection_name}
        if external_source is not None:
            kwargs["external_source"] = external_source
        if external_spec is not None:
            kwargs["external_spec"] = external_spec

        job_id = self.refresh_external_collection(client, **kwargs)[0]
        progress = self.wait_refresh_progress(
            client,
            job_id,
            timeout=timeout,
            return_on_reason=return_on_reason,
        )
        return job_id, progress

    def refresh_and_expect_failed(
        self,
        client,
        collection_name,
        timeout=60,
        external_source=None,
        external_spec=None,
        reason_terms=(),
    ):
        job_id, progress = self.refresh_and_poll_terminal(
            client,
            collection_name,
            timeout=timeout,
            external_source=external_source,
            external_spec=external_spec,
        )
        assert progress.state == "RefreshFailed", f"expected RefreshFailed for job {job_id}, got {progress.state}"
        reason = (progress.reason or "").lower()
        assert reason, f"refresh job {job_id} failed without a reason"
        if reason_terms:
            assert any(term.lower() in reason for term in reason_terms), (
                f"refresh job {job_id} reason {progress.reason!r} did not contain any of {reason_terms}"
            )
        return job_id, progress

    def add_vector_index(
        self, client, collection_name, vec_field="embedding", index_type="AUTOINDEX", metric_type="L2", **extra
    ):
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=vec_field,
            index_type=index_type,
            metric_type=metric_type,
            **extra,
        )
        self.create_index(client, collection_name, index_params)

    def index_and_load(self, client, collection_name, vec_field="embedding", metric_type="L2"):
        self.add_vector_index(client, collection_name, vec_field=vec_field, metric_type=metric_type)
        self.load_collection(client, collection_name)

    # Basic parquet collection lifecycle helpers. These intentionally cover
    # only the common happy path; specialized cases keep their setup inline.
    def create_basic_external_collection(self, client, collection_name, external_source, ext_spec=None):
        schema = _build_basic_schema(self, client, external_source, ext_spec=ext_spec)
        self.create_collection(client, collection_name=collection_name, schema=schema)
        return schema

    def prepare_refreshed_basic_collection(
        self,
        client,
        minio_env,
        external_prefix,
        num_rows=ct.default_nb,
        start_id=0,
        filename="data.parquet",
        collection_name=None,
        ext_spec=None,
    ):
        external_source = external_prefix["url"]
        minio_client, cfg = minio_env
        coll = collection_name or cf.gen_collection_name_by_testcase_name()
        upload_basic_data(
            minio_client,
            cfg,
            external_prefix["key"],
            num_rows=num_rows,
            start_id=start_id,
            filename=filename,
        )
        self.create_basic_external_collection(client, coll, external_source, ext_spec=ext_spec)
        self.refresh_and_wait(client, coll)
        return coll

    def prepare_indexed_basic_collection(self, client, minio_env, external_prefix, **kwargs):
        index_type = kwargs.pop("index_type", "AUTOINDEX")
        metric_type = kwargs.pop("metric_type", "L2")
        vector_index_params = kwargs.pop("index_params", None)
        coll = self.prepare_refreshed_basic_collection(
            client,
            minio_env,
            external_prefix,
            **kwargs,
        )
        self.add_vector_index(
            client,
            coll,
            "embedding",
            index_type=index_type,
            metric_type=metric_type,
            **({"params": vector_index_params} if vector_index_params else {}),
        )
        return coll

    def prepare_loaded_basic_collection(self, client, minio_env, external_prefix, **kwargs):
        metric_type = kwargs.pop("metric_type", "L2")
        coll = self.prepare_refreshed_basic_collection(
            client,
            minio_env,
            external_prefix,
            **kwargs,
        )
        self.index_and_load(client, coll, metric_type=metric_type)
        return coll


# ============================================================
# 1. Schema + Lifecycle
# ============================================================


class TestMilvusClientExternalTableSchema(ExternalTableTestBase):
    """Collection creation, describe, list, drop and schema validation."""

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_external_table_create_describe_list_drop(self, external_prefix):
        """
        target: test MilvusClient external table create describe list drop
        method: Create an external collection, verify describe/list, then drop
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url = external_prefix["url"]

        self.create_basic_external_collection(client, coll, ext_url)
        assert self.has_collection(client, coll)[0], "collection should exist after create"

        info = self.describe_collection(client, coll)[0]
        assert info.get("external_source") == ext_url
        assert json.loads(info.get("external_spec", "{}")).get("format") == "parquet"

        field_names = [f["name"] for f in info["fields"]]
        assert "__virtual_pk__" in field_names, "virtual PK should be injected"
        for required in ("id", "value", "embedding"):
            assert required in field_names, f"{required} missing from {field_names}"

        vpk = next(f for f in info["fields"] if f["name"] == "__virtual_pk__")
        assert vpk["is_primary"] is True
        assert vpk["auto_id"] is True

        value_f = next(f for f in info["fields"] if f["name"] == "value")
        assert value_f.get("external_field") == "value"

        assert (
            coll
            in self.list_collections(
                client,
            )[0]
        )

        self.drop_collection(client, coll)
        assert not self.has_collection(client, coll)[0], "collection should be gone after drop"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "case_name,schema_kwargs,fields,err_hint,err_stage",
        [
            (
                "user_primary_key",
                {},
                [
                    ("pk", DataType.INT64, {"is_primary": True, "external_field": "pk"}),
                    ("vec", DataType.FLOAT_VECTOR, {"dim": ct.default_dim, "external_field": "vec"}),
                ],
                "primary key",
                "create_collection",
            ),
            (
                "auto_id_on_user_field",
                {},
                [
                    ("id", DataType.INT64, {"auto_id": True, "external_field": "id"}),
                    ("vec", DataType.FLOAT_VECTOR, {"dim": ct.default_dim, "external_field": "vec"}),
                ],
                "auto_id",
                "add_field",
            ),
            (
                "dynamic_field_enabled",
                {"enable_dynamic_field": True},
                [
                    ("id", DataType.INT64, {"external_field": "id"}),
                    ("vec", DataType.FLOAT_VECTOR, {"dim": ct.default_dim, "external_field": "vec"}),
                ],
                "dynamic field",
                "create_collection",
            ),
            (
                "partition_key",
                {},
                [
                    (
                        "cat",
                        DataType.VARCHAR,
                        {"max_length": 64, "is_partition_key": True, "external_field": "cat"},
                    ),
                    ("vec", DataType.FLOAT_VECTOR, {"dim": ct.default_dim, "external_field": "vec"}),
                ],
                "partition key",
                "create_collection",
            ),
            (
                "missing_external_field",
                {},
                [
                    ("plain_field", DataType.INT64, {}),
                    ("vec", DataType.FLOAT_VECTOR, {"dim": ct.default_dim, "external_field": "vec"}),
                ],
                "external_field",
                "create_collection",
            ),
            (
                "sparse_float_vector",
                {},
                [
                    ("id", DataType.INT64, {"external_field": "id"}),
                    ("sv", DataType.SPARSE_FLOAT_VECTOR, {"external_field": "sv"}),
                ],
                "SparseFloatVector",
                "create_collection",
            ),
            (
                "duplicate_external_field",
                {},
                [
                    ("id", DataType.INT64, {"external_field": "same_col"}),
                    ("value", DataType.FLOAT, {"external_field": "same_col"}),
                    ("embedding", DataType.FLOAT_VECTOR, {"dim": ct.default_dim, "external_field": "embedding"}),
                ],
                "external_field",
                "create_collection",
            ),
        ],
        ids=[
            "user_primary_key",
            "auto_id_on_user_field",
            "dynamic_field_enabled",
            "partition_key",
            "missing_external_field",
            "sparse_float_vector",
            "duplicate_external_field",
        ],
    )
    def test_milvus_client_external_table_schema_reject_invalid_definition(
        self, case_name, schema_kwargs, fields, err_hint, err_stage
    ):
        """
        target: test MilvusClient external table schema reject invalid definition
        method: Forbidden or invalid external schema shapes must be rejected while building or creating schema
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(
            client,
            external_source=_PLACEHOLDER_SRC,
            external_spec=_PLACEHOLDER_SPEC,
            **schema_kwargs,
        )[0]
        if err_stage == "add_field":
            field_name, dtype, field_kwargs = fields[0]
            self.add_field(
                schema,
                field_name,
                dtype,
                check_task=CheckTasks.err_res,
                check_items={ct.err_code: 1, ct.err_msg: err_hint},
                **field_kwargs,
            )
            log.info(f"[{case_name}] correctly rejected by add_field with {err_hint!r}")
            return
        for field_name, dtype, field_kwargs in fields:
            self.add_field(schema, field_name, dtype, **field_kwargs)
        self.create_collection(
            client,
            collection_name=coll,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: err_hint},
        )
        log.info(f"[{case_name}] correctly rejected with {err_hint!r}")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("scheme", ALLOWED_EXTERNAL_SOURCE_SCHEMES)
    def test_milvus_client_external_table_external_source_allowed_schemes(self, scheme, minio_env):
        """
        target: test MilvusClient external table external source allowed schemes
        method: The CI allowlist keeps MinIO as the only external source provider at create time
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        _minio_client, cfg = minio_env
        source = f"{scheme}://{cfg['address']}/{cfg['bucket']}/prefix/"
        schema = self.create_schema(
            client,
            external_source=source,
            external_spec=build_external_spec(cfg=cfg, cloud_provider=scheme),
        )[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(schema, "vec", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="vec")
        self.create_collection(client, collection_name=coll, schema=schema)
        assert self.has_collection(client, coll)[0]

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_secure_minio_smoke(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table secure minio smoke
        method: HTTPS MinIO config should flow through source upload and extfs.use_ssl
        expected: behavior matches the case assertion.
        """
        _minio_client, cfg = minio_env
        if not cfg["secure"]:
            pytest.skip("set --minio_host=https://<host>:<port> to run secure MinIO smoke")

        client = self._client()
        ext_spec = build_external_spec(cfg)
        coll = self.prepare_loaded_basic_collection(
            client,
            minio_env,
            external_prefix,
            num_rows=20,
            ext_spec=ext_spec,
        )
        assert self.query_count(client, coll) == 20
        info = self.describe_collection(client, coll)[0]
        assert_external_spec_persisted(info.get("external_spec"), ext_spec, "secure minio external_spec")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "case_name,source",
        [
            ("relative_path_no_scheme", "my-data/parquet/"),
            ("http", "http://169.254.169.254/metadata"),
            ("ftp", "ftp://example.com/data"),
            ("unknown", "xyz://bucket/prefix"),
            ("file", "file:///tmp/data"),
            ("userinfo", "s3://ak:sk@bucket/prefix"),
        ],
        ids=["relative_path_no_scheme", "http", "ftp", "unknown", "file", "userinfo"],
    )
    def test_milvus_client_external_table_external_source_rejected(self, case_name, source):
        """
        target: test MilvusClient external table external source rejected
        method: Invalid source forms and schemes outside the allowlist must be rejected
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(
            client,
            external_source=source,
            external_spec=_PLACEHOLDER_SPEC,
        )[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(schema, "vec", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="vec")
        self.create_collection(
            client,
            collection_name=coll,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "external_source"},
        )
        log.info(f"[{case_name}] {source} correctly rejected for external_source")

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_user_fields_force_nullable(self):
        """
        target: test MilvusClient external table user fields force nullable
        method: User fields are server-side normalized to nullable=True; the virtual PK is exempt and remains the primary key
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(
            client,
            external_source=_PLACEHOLDER_SRC,
            external_spec=_PLACEHOLDER_SPEC,
        )[0]
        # Intentionally do NOT pass nullable on any user field.
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(schema, "v", DataType.VARCHAR, max_length=64, external_field="v")
        self.add_field(schema, "vec", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="vec")
        self.create_collection(client, collection_name=coll, schema=schema)
        info = self.describe_collection(client, coll)[0]
        by_name = {f["name"]: f for f in info["fields"]}
        for field in ("id", "v"):
            assert by_name[field].get("nullable") is True, (
                f"user field '{field}' should be force-nullable, got {by_name[field]}"
            )
        assert by_name["vec"].get("type") == DataType.FLOAT_VECTOR, f"vec field should exist, got {by_name['vec']}"
        vpk = by_name.get("__virtual_pk__")
        assert vpk is not None and vpk.get("is_primary") is True, "__virtual_pk__ should still be the primary key"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "properties",
        [
            {"collection.external_source": _PLACEHOLDER_NEW_SOURCE},
            {"collection.external_spec": _PLACEHOLDER_NEW_SPEC},
            {
                "collection.external_source": _PLACEHOLDER_NEW_SOURCE,
                "collection.external_spec": _PLACEHOLDER_NEW_SPEC,
            },
        ],
        ids=["source_only", "spec_only", "source_and_spec"],
    )
    def test_milvus_client_external_table_alter_collection_external_source_via_property_rejected(
        self, properties, minio_env
    ):
        """
        target: test MilvusClient external table alter collection external source via property rejected
        method: verify alter collection external source via property rejected
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        _minio_client, cfg = minio_env
        schema = self.create_schema(
            client,
            external_source=build_external_source(cfg, "old"),
            external_spec=build_external_spec(cfg),
        )[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(schema, "vec", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="vec")
        self.create_collection(client, collection_name=coll, schema=schema)
        before = self.describe_collection(client, coll)[0]
        self.alter_collection_properties(
            client,
            collection_name=coll,
            properties=properties,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "external"},
        )
        after = self.describe_collection(client, coll)[0]
        assert after.get("external_source") == before.get("external_source")
        assert after.get("external_spec") == before.get("external_spec")
        log.info("alter via property correctly rejected")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "fields",
        [
            [("__virtual_pk__", DataType.INT64, {"is_primary": True, "auto_id": False})],
            [
                ("id", DataType.INT64, {"is_primary": True, "auto_id": False}),
                ("__virtual_pk__", DataType.INT64, {}),
            ],
            [
                (
                    "__virtual_pk__",
                    DataType.VARCHAR,
                    {
                        "is_primary": True,
                        "auto_id": False,
                        "max_length": 64,
                    },
                )
            ],
        ],
        ids=["as_int64_pk", "as_scalar", "as_varchar_pk"],
    )
    def test_milvus_client_external_table_regular_collection_rejects_virtual_pk_reserved_name(self, fields):
        """
        target: test MilvusClient external table regular collection rejects virtual pk reserved name (milvus#49314)
        method: milvus#49314: __virtual_pk__ is reserved even outside external collections
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(
            client,
        )[0]
        for name, dtype, kwargs in fields:
            self.add_field(schema, name, dtype, **kwargs)
        self.add_field(schema, "embedding", DataType.FLOAT_VECTOR, dim=ct.default_dim)

        self.create_collection(
            client,
            collection_name=coll,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "__virtual_pk__"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"external_source": _PLACEHOLDER_SRC},
            {"external_spec": _PLACEHOLDER_SPEC},
        ],
        ids=["source_only", "spec_only"],
    )
    def test_milvus_client_external_table_refresh_override_requires_source_spec_pair(self, kwargs, external_prefix):
        """
        target: test MilvusClient external table refresh override requires source spec pair (#49335, milvus#49230)
        method: milvus#49230/#49335: refresh override source/spec must be atomic
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        schema = _build_basic_schema(self, client, external_prefix["url"])
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_external_collection(
            client,
            collection_name=coll,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "both provided or both omitted"},
            **kwargs,
        )


# ============================================================
# 2. Data Types (Scalar + Vector)
# ============================================================


class TestMilvusClientExternalTableDataTypes(ExternalTableTestBase):
    """E2E coverage for every supported scalar and vector data type."""

    # ---- Scalar types ------------------------------------------------

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "type_name,dtype,extra,arrow_type,value_fn",
        [
            ("bool", DataType.BOOL, {}, pa.bool_(), lambda i: i % 2 == 0),
            ("int8", DataType.INT8, {}, pa.int8(), lambda i: i % 100),
            ("int16", DataType.INT16, {}, pa.int16(), lambda i: i * 3),
            ("int32", DataType.INT32, {}, pa.int32(), lambda i: i * 7),
            ("int64", DataType.INT64, {}, pa.int64(), lambda i: i * 1000),
            ("float", DataType.FLOAT, {}, pa.float32(), lambda i: float(i) * 1.5),
            ("double", DataType.DOUBLE, {}, pa.float64(), lambda i: float(i) * 2.5),
            ("varchar", DataType.VARCHAR, {"max_length": 64}, pa.string(), lambda i: f"s_{i:05d}"),
            ("json", DataType.JSON, {}, pa.string(), lambda i: json.dumps({"k": i, "g": i % 3})),
        ],
        ids=["bool", "int8", "int16", "int32", "int64", "float", "double", "varchar", "json"],
    )
    def test_milvus_client_external_table_scalar_type_e2e(
        self, type_name, dtype, extra, arrow_type, value_fn, minio_env, external_prefix
    ):
        """
        target: test MilvusClient external table scalar type e2e
        method: End-to-end per supported scalar type
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        scalar_name = f"f_{type_name}"
        data = gen_parquet_bytes(ct.default_nb, 0, scalar_name, arrow_type, value_fn)
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", data)

        schema = self.create_schema(client, external_source=ext_url, external_spec=build_external_spec(cfg))[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(schema, scalar_name, dtype, external_field=scalar_name, **extra)
        self.add_field(schema, "embedding", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="embedding")
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)

        count = self.query_count(client, coll)
        assert count == ct.default_nb, f"[{type_name}] expected {ct.default_nb} rows, got {count}"

        res = self.query(client, coll, filter="id == 0", output_fields=["id", scalar_name])[0]
        assert len(res) == 1
        assert res[0]["id"] == 0
        log.info(f"[{type_name}] id=0 -> {res[0]}")

        vectors = _float_vectors([0], ct.default_dim).tolist()
        search_res = self.search(
            client,
            coll,
            data=vectors,
            limit=5,
            anns_field="embedding",
            output_fields=["id", scalar_name],
        )[0]
        assert len(search_res) == 1 and len(search_res[0]) > 0

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "elem_name,elem_dtype,elem_arrow,elem_extra,value_fn",
        [
            ("int64", DataType.INT64, pa.int64(), {"max_capacity": 8}, lambda i: [i, i + 1, i + 2]),
            (
                "varchar",
                DataType.VARCHAR,
                pa.string(),
                {"max_capacity": 8, "max_length": 32},
                lambda i: [f"a_{i}", f"b_{i}"],
            ),
        ],
        ids=["int64", "varchar"],
    )
    def test_milvus_client_external_table_array_type_e2e(
        self, elem_name, elem_dtype, elem_arrow, elem_extra, value_fn, minio_env, external_prefix
    ):
        """
        target: test MilvusClient external table array type e2e
        method: Array-of-<elem> field: upload, refresh, load, and query the array values
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        arr_field = f"arr_{elem_name}"

        data = gen_array_parquet_bytes(ct.default_nb, 0, arr_field, elem_arrow, value_fn)
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", data)

        schema = self.create_schema(client, external_source=ext_url, external_spec=build_external_spec(cfg))[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(
            schema, arr_field, DataType.ARRAY, element_type=elem_dtype, external_field=arr_field, **elem_extra
        )
        self.add_field(schema, "embedding", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="embedding")

        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)
        assert self.query_count(client, coll) == ct.default_nb

        rows = self.query(client, coll, filter="id == 5", output_fields=["id", arr_field])[0]
        assert len(rows) == 1
        expected = value_fn(5)
        assert rows[0][arr_field] == expected, f"got {rows[0][arr_field]}, expected {expected}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_all_scalar_types_in_one_collection(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table all scalar types in one collection
        method: Single collection with every scalar type + one FloatVector; cross-field filters and queries work correctly
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        nb = 100
        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_all_scalar_parquet_bytes(nb, 0),
        )

        schema = self.create_schema(client, external_source=ext_url, external_spec=build_external_spec(cfg))[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(schema, "val_bool", DataType.BOOL, external_field="val_bool")
        self.add_field(schema, "val_int8", DataType.INT8, external_field="val_int8")
        self.add_field(schema, "val_int16", DataType.INT16, external_field="val_int16")
        self.add_field(schema, "val_int32", DataType.INT32, external_field="val_int32")
        self.add_field(schema, "val_int64", DataType.INT64, external_field="val_int64")
        self.add_field(schema, "val_float", DataType.FLOAT, external_field="val_float")
        self.add_field(schema, "val_double", DataType.DOUBLE, external_field="val_double")
        self.add_field(schema, "val_varchar", DataType.VARCHAR, max_length=64, external_field="val_varchar")
        self.add_field(schema, "val_json", DataType.JSON, external_field="val_json")
        self.add_field(schema, "embedding", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="embedding")
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)
        assert self.query_count(client, coll) == nb

        # bool filter: even ids → 50
        b = self.query(client, coll, filter="val_bool == true", output_fields=["id"])[0]
        assert len(b) == 50
        # int8 range: < 10 → 10
        i8 = self.query(client, coll, filter="val_int8 < 10", output_fields=["id"])[0]
        assert len(i8) == 10
        # varchar like: first 10 match s_0000x pattern (x = 0..9) → 10
        s = self.query(client, coll, filter='val_varchar like "s_0000%"', output_fields=["id"])[0]
        assert len(s) == 10

        # Verify specific row values for id=42
        row42 = self.query(
            client,
            coll,
            filter="id == 42",
            output_fields=[
                "id",
                "val_bool",
                "val_int8",
                "val_int16",
                "val_int32",
                "val_int64",
                "val_float",
                "val_double",
                "val_varchar",
                "val_json",
            ],
        )[0]
        assert len(row42) == 1
        r = row42[0]
        assert r["val_bool"] is True
        assert r["val_int8"] == 42
        assert r["val_int16"] == 42 * 3
        assert r["val_int32"] == 42 * 7
        assert r["val_int64"] == 42 * 1000
        assert abs(r["val_float"] - 63.0) < 0.01
        assert abs(r["val_double"] - 105.0) < 0.01
        assert r["val_varchar"] == "s_00042"
        parsed = r["val_json"] if isinstance(r["val_json"], dict) else json.loads(r["val_json"])
        assert parsed["k"] == 42

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_geometry_wkt_parquet_round_trip(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table geometry wkt parquet round trip (milvus#48627)
        method: milvus#48627: WKT-backed Geometry columns query back as geometry text
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_geometry_wkt_parquet_bytes(50, 0),
        )
        schema = self.create_schema(
            client,
            external_source=ext_url,
            external_spec=build_external_spec(cfg),
        )[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(schema, "geo", DataType.GEOMETRY, external_field="geo")
        self.add_field(schema, "embedding", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="embedding")
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)

        rows = self.query(
            client,
            coll,
            filter="id == 7",
            output_fields=["id", "geo"],
        )[0]
        assert len(rows) == 1
        assert rows[0]["id"] == 7
        assert "POINT" in str(rows[0]["geo"]).upper(), rows[0]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "case_name,milvus_dtype,field_kwargs,arrow_builder,include_embedding,known_acceptance",
        [
            (
                "int8_from_string",
                DataType.INT8,
                {},
                lambda ids: pa.array(["abcd" for _ in ids], type=pa.string()),
                True,
                True,
            ),
            (
                "int16_from_string",
                DataType.INT16,
                {},
                lambda ids: pa.array(["abcd" for _ in ids], type=pa.string()),
                True,
                True,
            ),
            (
                "int32_from_string",
                DataType.INT32,
                {},
                lambda ids: pa.array(["abcd" for _ in ids], type=pa.string()),
                True,
                True,
            ),
            (
                "int64_from_string",
                DataType.INT64,
                {},
                lambda ids: pa.array(["abcd" for _ in ids], type=pa.string()),
                True,
                True,
            ),
            (
                "float_from_string",
                DataType.FLOAT,
                {},
                lambda ids: pa.array(["abc" for _ in ids], type=pa.string()),
                True,
                True,
            ),
            (
                "double_from_string",
                DataType.DOUBLE,
                {},
                lambda ids: pa.array(["abc" for _ in ids], type=pa.string()),
                True,
                True,
            ),
            (
                "varchar_from_int64",
                DataType.VARCHAR,
                {"max_length": 64},
                lambda ids: pa.array(ids, type=pa.int64()),
                True,
                False,
            ),
            (
                "json_from_int64",
                DataType.JSON,
                {},
                lambda ids: pa.array(ids, type=pa.int64()),
                True,
                False,
            ),
            (
                "geometry_from_int64",
                DataType.GEOMETRY,
                {},
                lambda ids: pa.array(ids, type=pa.int64()),
                True,
                False,
            ),
            (
                "timestamptz_from_string",
                DataType.TIMESTAMPTZ,
                {},
                lambda ids: pa.array(["2026-04-27T00:00:00Z" for _ in ids], type=pa.string()),
                True,
                True,
            ),
            (
                "int8_from_int64_overflow",
                DataType.INT8,
                {},
                lambda ids: pa.array([128 + i for i in ids], type=pa.int64()),
                True,
                False,
            ),
            (
                "int16_from_int64_overflow",
                DataType.INT16,
                {},
                lambda ids: pa.array([40000 + i for i in ids], type=pa.int64()),
                True,
                False,
            ),
            (
                "int32_from_int64_overflow",
                DataType.INT32,
                {},
                lambda ids: pa.array([2147483648 + i for i in ids], type=pa.int64()),
                True,
                False,
            ),
            (
                "float_from_int64",
                DataType.FLOAT,
                {},
                lambda ids: pa.array(ids, type=pa.int64()),
                True,
                True,
            ),
            (
                "double_from_int64",
                DataType.DOUBLE,
                {},
                lambda ids: pa.array(ids, type=pa.int64()),
                True,
                True,
            ),
            (
                "int64_from_double",
                DataType.INT64,
                {},
                lambda ids: pa.array([float(i) + 0.5 for i in ids], type=pa.float64()),
                True,
                True,
            ),
            (
                "array_int64_from_list_string",
                DataType.ARRAY,
                {"element_type": DataType.INT64, "max_capacity": 8},
                lambda ids: pa.array([[f"s_{i}", f"s_{i + 1}"] for i in ids], type=pa.list_(pa.string())),
                True,
                True,
            ),
            (
                "array_float_from_list_string",
                DataType.ARRAY,
                {"element_type": DataType.FLOAT, "max_capacity": 8},
                lambda ids: pa.array([["1.5", "2.5"] for _ in ids], type=pa.list_(pa.string())),
                True,
                True,
            ),
            (
                "array_bool_from_list_string",
                DataType.ARRAY,
                {"element_type": DataType.BOOL, "max_capacity": 8},
                lambda ids: pa.array([["true", "false"] for _ in ids], type=pa.list_(pa.string())),
                True,
                True,
            ),
        ],
        ids=[
            "int8_from_string",
            "int16_from_string",
            "int32_from_string",
            "int64_from_string",
            "float_from_string",
            "double_from_string",
            "varchar_from_int64",
            "json_from_int64",
            "geometry_from_int64",
            "timestamptz_from_string",
            "int8_from_int64_overflow",
            "int16_from_int64_overflow",
            "int32_from_int64_overflow",
            "float_from_int64",
            "double_from_int64",
            "int64_from_double",
            "array_int64_from_list_string",
            "array_float_from_list_string",
            "array_bool_from_list_string",
        ],
    )
    def test_milvus_client_external_table_incompatible_external_arrow_type_rejected(
        self,
        case_name,
        milvus_dtype,
        field_kwargs,
        arrow_builder,
        include_embedding,
        known_acceptance,
        minio_env,
        external_prefix,
    ):
        """
        target: test MilvusClient external table incompatible external arrow type rejected
        method: Milvus field type vs external Arrow type mismatches must fail visibly
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        nb = 20
        bad_field = "bad_col"
        ids = list(range(nb))

        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_dtype_mismatch_parquet_bytes(
                nb,
                0,
                bad_field,
                arrow_builder(ids),
                include_embedding=include_embedding,
            ),
        )

        schema = self.create_schema(
            client,
            external_source=ext_url,
            external_spec=build_external_spec(cfg),
        )[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(schema, bad_field, milvus_dtype, external_field=bad_field, **field_kwargs)
        if include_embedding:
            self.add_field(schema, "embedding", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="embedding")

        self.create_collection(
            client,
            collection_name=coll,
            schema=schema,
        )

        job_id, progress = self.refresh_and_poll_terminal(client, coll, timeout=60, return_on_reason=True)
        if progress.state == "RefreshFailed" or progress.reason:
            assert progress.reason, f"[{case_name}] refresh job {job_id} failed without a reason"
            log.info(f"[{case_name}] refresh rejected mismatch: job={job_id}, reason={progress.reason}")
            return

        assert progress.state == "RefreshCompleted", f"[{case_name}] unexpected refresh state: {progress.state}"

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="embedding" if include_embedding else bad_field,
            index_type="AUTOINDEX",
            metric_type="L2",
        )
        self.create_index(client, coll, index_params)

        self.load_collection(client, coll, timeout=30)

        accepted_result = self.query(
            client,
            coll,
            filter="id >= 0",
            output_fields=["id", bad_field],
            limit=1,
        )[0]

        if not include_embedding:
            search_res = self.search(
                client,
                coll,
                data=[[0.0] * ct.default_dim],
                limit=1,
                anns_field=bad_field,
                output_fields=["id"],
            )[0]
            accepted_result = {
                "query": accepted_result,
                "search": search_res,
            }

        if known_acceptance:
            pytest.xfail(
                f"[{case_name}] current master accepts incompatible "
                f"external Arrow data; observed result: {accepted_result}"
            )
        pytest.fail(
            f"[{case_name}] incompatible external Arrow type was accepted "
            f"through refresh, load, and query; result={accepted_result}"
        )

    # ---- Vector types -----------------------------------------------

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "case_name,external_dim",
        [
            ("external_dim_larger_than_schema", ct.default_dim * 2),
            ("external_dim_smaller_than_schema", ct.default_dim // 2),
        ],
        ids=["external_dim_larger_than_schema", "external_dim_smaller_than_schema"],
    )
    def test_milvus_client_external_table_float_vector_external_dim_mismatch_rejected(
        self,
        case_name,
        external_dim,
        minio_env,
        external_prefix,
    ):
        """
        target: test MilvusClient external table float vector external dim mismatch rejected (milvus#49388)
        method: milvus#49388: external FloatVector width must match schema dim
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_basic_parquet_bytes(20, 0, dim=external_dim),
        )

        schema = _build_basic_schema(self, client, ext_url, dim=ct.default_dim, ext_spec=build_external_spec(cfg))
        self.create_collection(client, collection_name=coll, schema=schema)
        _job_id, progress = self.refresh_and_expect_failed(client, coll, timeout=60)
        log.info(f"[{case_name}] refresh rejected vector dim mismatch: {progress.reason}")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "vec_kind,vec_dtype,metric,dim",
        [
            ("float", DataType.FLOAT_VECTOR, "L2", ct.default_dim),
            ("float16", DataType.FLOAT16_VECTOR, "L2", ct.default_dim),
            ("bfloat16", DataType.BFLOAT16_VECTOR, "L2", ct.default_dim),
            ("int8", DataType.INT8_VECTOR, "L2", ct.default_dim),
        ],
        ids=["float", "float16", "bfloat16", "int8"],
    )
    def test_milvus_client_external_table_float_family_vector_e2e(
        self, vec_kind, vec_dtype, metric, dim, minio_env, external_prefix
    ):
        """
        target: test MilvusClient external table float family vector e2e
        method: Each float-family / int8 vector type: upload, refresh, index, load, search
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        vec_field = f"vec_{vec_kind}"

        data = gen_vector_variant_parquet_bytes(ct.default_nb, 0, vec_field, vec_dtype, dim)
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", data)

        schema = self.create_schema(client, external_source=ext_url, external_spec=build_external_spec(cfg))[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(schema, vec_field, vec_dtype, dim=dim, external_field=vec_field)
        self.create_collection(client, collection_name=coll, schema=schema)

        self.refresh_and_wait(client, coll)
        self.add_vector_index(client, coll, vec_field, "AUTOINDEX", metric)
        self.load_collection(client, coll)

        assert self.query_count(client, coll) == ct.default_nb

        # Construct a query vector in the right dtype/encoding expected by pymilvus.
        if vec_dtype == DataType.BINARY_VECTOR:
            return  # handled by dedicated test
        if vec_dtype == DataType.INT8_VECTOR:
            # pymilvus expects int8 vector as numpy int8 array (or bytes)
            query_vec = [np.zeros(dim, dtype=np.int8)]
        elif vec_dtype == DataType.FLOAT16_VECTOR:
            query_vec = [np.zeros(dim, dtype=np.float16)]
        elif vec_dtype == DataType.BFLOAT16_VECTOR:
            # pymilvus rejects uint16 at client side; feed raw bytes of
            # the bfloat16 zero pattern instead (dim * 2 bytes).
            query_vec = [bytes(dim * 2)]
        else:
            query_vec = [[0.0] * dim]
        search_res = self.search(
            client,
            coll,
            data=query_vec,
            limit=5,
            anns_field=vec_field,
            output_fields=["id"],
        )[0]
        assert len(search_res) == 1 and len(search_res[0]) > 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_binary_vector_e2e(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table binary vector e2e
        method: Binary vector end-to-end with HAMMING / JACCARD metric
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        data = gen_vector_variant_parquet_bytes(
            ct.default_nb,
            0,
            "bin_vec",
            DataType.BINARY_VECTOR,
            ct.default_dim,
        )
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", data)

        schema = self.create_schema(client, external_source=ext_url, external_spec=build_external_spec(cfg))[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(schema, "bin_vec", DataType.BINARY_VECTOR, dim=ct.default_dim, external_field="bin_vec")
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.add_vector_index(client, coll, "bin_vec", "BIN_FLAT", "HAMMING")
        self.load_collection(client, coll)

        assert self.query_count(client, coll) == ct.default_nb

        # Single query vector of correct length (ct.default_dim bits → ct.default_dim/8 bytes)
        query_vec = [b"\x00" * (ct.default_dim // 8)]
        search_res = self.search(
            client,
            coll,
            data=query_vec,
            limit=5,
            anns_field="bin_vec",
            output_fields=["id"],
            search_params={"metric_type": "HAMMING"},
        )[0]
        assert len(search_res) == 1 and len(search_res[0]) > 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_multi_vector_collection(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table multi vector collection
        method: Collection with FloatVector + BinaryVector: both indexable and searchable
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_multi_vector_parquet_bytes(ct.default_nb, 0),
        )
        schema = self.create_schema(client, external_source=ext_url, external_spec=build_external_spec(cfg))[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(schema, "value", DataType.FLOAT, external_field="value")
        self.add_field(schema, "dense_vec", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="dense_vec")
        self.add_field(schema, "bin_vec", DataType.BINARY_VECTOR, dim=ct.default_dim, external_field="bin_vec")
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)

        index_params = self.prepare_index_params(
            client,
        )[0]
        index_params.add_index(field_name="dense_vec", index_type="AUTOINDEX", metric_type="L2")
        index_params.add_index(field_name="bin_vec", index_type="BIN_FLAT", metric_type="HAMMING")
        self.create_index(client, coll, index_params)
        self.load_collection(client, coll)

        assert self.query_count(client, coll) == ct.default_nb

        dense_res = self.search(
            client,
            coll,
            data=[[0.0] * ct.default_dim],
            limit=3,
            anns_field="dense_vec",
            output_fields=["id"],
        )[0]
        bin_res = self.search(
            client,
            coll,
            data=[b"\x00" * (ct.default_dim // 8)],
            limit=3,
            anns_field="bin_vec",
            output_fields=["id"],
            search_params={"metric_type": "HAMMING"},
        )[0]
        assert len(dense_res[0]) == 3 and len(bin_res[0]) == 3

    # ---- Whitelist gap coverage --------------------------------------
    # The supported-type whitelist in pkg/util/typeutil/schema.go:2369-2394
    # accepts Text, Timestamptz, and ArrayOfVector for external collections.
    # The cases below probe each explicitly so the contract stays observable.

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("ts_unit", ["us", "ms"])
    def test_milvus_client_external_table_timestamptz_type_e2e(self, ts_unit, minio_env, external_prefix):
        """
        target: test MilvusClient external table timestamptz type e2e
        method: Timestamptz field: arrow timestamp in parquet → Milvus Timestamptz
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        nb = ct.default_nb
        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_timestamptz_parquet_bytes(nb, 0, ts_unit=ts_unit),
        )

        schema = self.create_schema(
            client,
            external_source=ext_url,
            external_spec=build_external_spec(cfg),
        )[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(schema, "ts", DataType.TIMESTAMPTZ, external_field="ts")
        self.add_field(schema, "embedding", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="embedding")
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)
        assert self.query_count(client, coll) == nb

        row = self.query(client, coll, filter="id == 7", output_fields=["id", "ts"])[0]
        assert len(row) == 1
        ts_val = row[0]["ts"]
        # Server returns ISO-8601 string; verify the offset matches our
        # encoding base (1700000000 + 7 = 1700000007 → "2023-11-14T22:13:27Z").
        assert isinstance(ts_val, str) and ts_val.startswith("2023-11-14")
        log.info(f"[timestamptz/{ts_unit}] id=7 -> {ts_val}")

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_external_table_array_of_vector_top_level_rejected(self, external_prefix):
        """
        target: test MilvusClient external table array of vector top level rejected
        method: verify array of vector top level rejected
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url = external_prefix["url"]
        schema = self.create_schema(
            client,
            external_source=ext_url,
            external_spec=build_external_spec(),
        )[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(
            schema,
            "vecs",
            DataType._ARRAY_OF_VECTOR,
            element_type=DataType.FLOAT_VECTOR,
            dim=ct.default_dim,
            max_capacity=4,
            external_field="vecs",
        )
        self.create_collection(
            client,
            collection_name=coll,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "array"},
        )
        log.info("[array_of_vector] rejected as expected")


# ============================================================
# 2.5 Edge cases: nullable behavior + large files
# ============================================================


class TestMilvusClientExternalTableNullableScenarios(ExternalTableTestBase):
    """External user fields are force-nullable (schema.go:2350). Probe edge
    cases of that contract."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_all_null_scalar_columns(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table all null scalar columns
        method: Every user scalar column is entirely null in parquet
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        nb = 100
        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_all_null_columns_parquet_bytes(nb, 0),
        )

        schema = self.create_schema(
            client,
            external_source=ext_url,
            external_spec=build_external_spec(cfg),
        )[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(schema, "n_int", DataType.INT32, external_field="n_int")
        self.add_field(schema, "n_float", DataType.FLOAT, external_field="n_float")
        self.add_field(schema, "n_varchar", DataType.VARCHAR, max_length=64, external_field="n_varchar")
        self.add_field(schema, "n_json", DataType.JSON, external_field="n_json")
        self.add_field(schema, "embedding", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="embedding")

        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)
        assert self.query_count(client, coll) == nb

        # Single-row peek: every user field should round-trip as None.
        rows = self.query(
            client,
            coll,
            filter="id == 5",
            output_fields=["id", "n_int", "n_float", "n_varchar", "n_json"],
        )[0]
        assert len(rows) == 1, f"missing id=5 in result: {rows}"
        r = rows[0]
        assert r["id"] == 5
        assert r["n_int"] is None
        assert r["n_float"] is None
        assert r["n_varchar"] is None
        assert r["n_json"] is None

        # `is null` filter should match every row.
        null_rows = self.query(
            client,
            coll,
            filter="n_int is null",
            output_fields=["id"],
            limit=nb,
        )[0]
        assert len(null_rows) == nb, f"is-null filter only matched {len(null_rows)}/{nb}"


class TestMilvusClientExternalTableLargeFile(ExternalTableTestBase):
    """Large single-file scenarios — multi-row-group parquet and large row
    counts that can be split into multiple Milvus segments."""

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "num_rows,row_group_size",
        [
            (5_000, 500),  # 10 row groups
            (20_000, 1_000),  # 20 row groups
        ],
        ids=["10rg", "20rg"],
    )
    def test_milvus_client_external_table_large_single_file_with_row_groups(
        self,
        num_rows,
        row_group_size,
        minio_env,
        external_prefix,
    ):
        """
        target: test MilvusClient external table large single file with row groups
        method: verify large single file with row groups
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_large_parquet_with_row_groups(num_rows, 0, row_group_size),
        )

        schema = _build_basic_schema(self, client, ext_url)
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)
        assert self.query_count(client, coll) == num_rows

        # Spot-check rows in the first, middle, and last row group.
        for sample in (0, num_rows // 2, num_rows - 1):
            rows = self.query(
                client,
                coll,
                filter=f"id == {sample}",
                output_fields=["id", "value"],
            )[0]
            assert len(rows) == 1, f"missing id={sample}"
            assert abs(rows[0]["value"] - sample * 1.5) < 1e-3

        hits = self.search(
            client,
            coll,
            data=[[0.0] * ct.default_dim],
            limit=10,
            anns_field="embedding",
            output_fields=["id"],
        )[0]
        assert len(hits[0]) == 10


# ============================================================
# 3. Indexes (Vector + Scalar)
# ============================================================


class TestMilvusClientExternalTableIndexes(ExternalTableTestBase):
    """Index creation on external collections — vector + scalar + binary."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "index_type,metric_type,params",
        [
            ("AUTOINDEX", "L2", {}),
            ("FLAT", "L2", {}),
            ("HNSW", "L2", {"M": 16, "efConstruction": 200}),
            ("IVF_FLAT", "L2", {"nlist": 16}),
            ("HNSW", "COSINE", {"M": 8}),
            ("DISKANN", "L2", {}),
        ],
        ids=["AUTOINDEX_L2", "FLAT_L2", "HNSW_L2", "IVF_FLAT_L2", "HNSW_COSINE", "DISKANN_L2"],
    )
    def test_milvus_client_external_table_vector_index(
        self, index_type, metric_type, params, minio_env, external_prefix
    ):
        """
        target: test MilvusClient external table vector index
        method: Create each supported vector index type on a FloatVector field and query
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_refreshed_basic_collection(client, minio_env, external_prefix, num_rows=ct.default_nb)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="embedding",
            index_type=index_type,
            metric_type=metric_type,
            **({"params": params} if params else {}),
        )
        self.create_index(client, coll, index_params)
        self.load_collection(client, coll)

        hits = self.search(
            client,
            coll,
            data=[[0.0] * ct.default_dim],
            limit=5,
            anns_field="embedding",
            output_fields=["id"],
        )[0][0]
        assert len(hits) == 5

        idx_list = self.list_indexes(client, collection_name=coll)[0]
        assert any("embedding" in str(i) for i in idx_list), f"embedding index not in list: {idx_list}"

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "index_type,params",
        [
            ("HNSW", {"M": 16, "efConstruction": 200}),
            ("IVF_FLAT", {"nlist": 16}),
        ],
        ids=["HNSW", "IVF_FLAT"],
    )
    def test_milvus_client_external_table_vector_index_mmap_enable(
        self, index_type, params, minio_env, external_prefix
    ):
        """
        target: test MilvusClient external table vector index mmap enable
        method: verify vector index mmap enable
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_indexed_basic_collection(
            client,
            minio_env,
            external_prefix,
            index_type=index_type,
            index_params=params,
            num_rows=ct.default_nb,
        )

        # Alter mmap=True before load
        self.alter_index_properties(
            client,
            collection_name=coll,
            index_name="embedding",
            properties={"mmap.enabled": True},
        )

        info = self.describe_index(client, collection_name=coll, index_name="embedding")[0]
        log.info(f"[{index_type}] index info after mmap=True: {info}")
        assert str(info.get("mmap.enabled", "")).lower() == "true", f"expected mmap.enabled=True, got {info}"

        self.load_collection(client, coll)
        hits_on = self.search(
            client,
            coll,
            data=[[0.0] * ct.default_dim],
            limit=5,
            anns_field="embedding",
            output_fields=["id"],
        )[0][0]
        assert len(hits_on) == 5

        # Flip back to False
        self.release_collection(client, coll)
        self.alter_index_properties(
            client,
            collection_name=coll,
            index_name="embedding",
            properties={"mmap.enabled": False},
        )
        info_off = self.describe_index(client, collection_name=coll, index_name="embedding")[0]
        assert str(info_off.get("mmap.enabled", "")).lower() == "false"

        self.load_collection(client, coll)
        hits_off = self.search(
            client,
            coll,
            data=[[0.0] * ct.default_dim],
            limit=5,
            anns_field="embedding",
            output_fields=["id"],
        )[0][0]
        assert len(hits_off) == 5

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_external_table_collection_mmap_property(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table collection mmap property
        method: Collection-level mmap.enabled toggled via alter_collection_properties and surfaced through describe_collection
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_refreshed_basic_collection(client, minio_env, external_prefix, num_rows=ct.default_nb)

        self.alter_collection_properties(
            client,
            collection_name=coll,
            properties={"mmap.enabled": True},
        )

        desc = self.describe_collection(client, coll)[0]
        props = desc.get("properties") or {}
        assert str(props.get("mmap.enabled", "")).lower() == "true", (
            f"expected collection mmap.enabled=True, got properties={props}"
        )

        self.index_and_load(client, coll)
        assert self.query_count(client, coll) == ct.default_nb

        hits = self.search(
            client,
            coll,
            data=[[0.0] * ct.default_dim],
            limit=5,
            anns_field="embedding",
            output_fields=["id"],
        )[0][0]
        assert len(hits) == 5

        # Flip back
        self.release_collection(client, coll)
        self.alter_collection_properties(
            client,
            collection_name=coll,
            properties={"mmap.enabled": False},
        )
        desc2 = self.describe_collection(client, coll)[0]
        assert str((desc2.get("properties") or {}).get("mmap.enabled", "")).lower() == "false"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index_type", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_milvus_client_external_table_binary_vector_index(self, index_type, minio_env, external_prefix):
        """
        target: test MilvusClient external table binary vector index
        method: Binary-vector index types over HAMMING
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_vector_variant_parquet_bytes(ct.default_nb, 0, "bin_vec", DataType.BINARY_VECTOR, ct.default_dim),
        )
        schema = self.create_schema(client, external_source=ext_url, external_spec=build_external_spec(cfg))[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(schema, "bin_vec", DataType.BINARY_VECTOR, dim=ct.default_dim, external_field="bin_vec")
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        params = {"nlist": 16} if index_type == "BIN_IVF_FLAT" else {}
        self.add_vector_index(client, coll, "bin_vec", index_type, "HAMMING", **({"params": params} if params else {}))
        self.load_collection(client, coll)

        hits = self.search(
            client,
            coll,
            data=[b"\x00" * (ct.default_dim // 8)],
            limit=5,
            anns_field="bin_vec",
            output_fields=["id"],
            search_params={"metric_type": "HAMMING"},
        )[0][0]
        assert len(hits) == 5

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "scalar_index_type,field_name",
        [
            ("INVERTED", "value"),
            ("STL_SORT", "value"),
            ("BITMAP", "id"),
        ],
    )
    def test_milvus_client_external_table_scalar_index(self, scalar_index_type, field_name, minio_env, external_prefix):
        """
        target: test MilvusClient external table scalar index
        method: Create a scalar index on an external collection field
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_refreshed_basic_collection(client, minio_env, external_prefix)

        index_params = self.prepare_index_params(
            client,
        )[0]
        index_params.add_index(field_name="embedding", index_type="AUTOINDEX", metric_type="L2")
        index_params.add_index(field_name=field_name, index_type=scalar_index_type)
        self.create_index(client, coll, index_params)

        self.load_collection(client, coll)
        assert self.query_count(client, coll) == ct.default_nb

        idx_list = self.list_indexes(client, collection_name=coll)[0]
        log.info(f"[{scalar_index_type}] list_indexes: {idx_list}")
        field_indexes = self.list_indexes(client, collection_name=coll, field_name=field_name)[0]
        assert field_indexes, f"{scalar_index_type} index missing for field {field_name}: {idx_list}"
        index_info = self.describe_index(client, collection_name=coll, index_name=str(field_indexes[0]))[0]
        assert index_info.get("field_name") == field_name, f"unexpected scalar index field: {index_info}"
        assert index_info.get("index_type") == scalar_index_type, f"unexpected scalar index type: {index_info}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_external_table_drop_and_recreate_index(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table drop and recreate index
        method: Drop the vector index, then recreate with different params
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_indexed_basic_collection(client, minio_env, external_prefix)
        self.load_collection(client, coll)
        assert self.query_count(client, coll) == ct.default_nb

        self.release_collection(client, coll)
        # Drop index by name (Milvus auto-assigns or uses field name)
        idx_list = self.list_indexes(client, collection_name=coll)[0]
        target = next((n for n in idx_list if "embedding" in str(n)), "embedding")
        self.drop_index(client, collection_name=coll, index_name=str(target))

        # Recreate with HNSW
        self.add_vector_index(client, coll, "embedding", "HNSW", "IP", params={"M": 16})
        self.load_collection(client, coll)

        hits = self.search(
            client,
            coll,
            data=[[0.0] * ct.default_dim],
            limit=3,
            anns_field="embedding",
            output_fields=["id"],
        )[0][0]
        assert len(hits) == 3

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_external_table_list_and_describe_index(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table list and describe index
        method: verify list and describe index
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_indexed_basic_collection(client, minio_env, external_prefix)

        indexes = self.list_indexes(client, collection_name=coll)[0]
        assert len(indexes) >= 1, f"expected at least 1 index, got {indexes}"
        log.info(f"list_indexes: {indexes}")

        name = str(indexes[0])
        info = self.describe_index(client, collection_name=coll, index_name=name)[0]
        log.info(f"describe_index({name}) -> {info}")
        assert info is not None


# ============================================================
# 4. Blocked Write Operations (DML + DDL)
# ============================================================


class TestMilvusClientExternalTableWriteBlocked(ExternalTableTestBase):
    """All DML + partition/field DDL should be rejected on external collections."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "op_name,err_msg",
        [
            ("insert", "not supported for external collection"),
            ("upsert", "not supported for external collection"),
            ("delete", "not supported for external collection"),
            ("flush", "not supported for external collection"),
            ("create_partition", "not supported for external collection"),
            ("drop_partition", "not supported for external collection"),
            ("compact", "not supported for external collection"),
            ("add_collection_field", "not supported for external collection"),
            ("truncate_collection", "not supported on external collections"),
        ],
        ids=[
            "insert",
            "upsert",
            "delete",
            "flush",
            "create_partition",
            "drop_partition",
            "compact",
            "add_collection_field",
            "truncate_collection",
        ],
    )
    def test_milvus_client_external_table_write_operation_rejected(self, op_name, err_msg, minio_env, external_prefix):
        """
        target: test MilvusClient external table write operation rejected
        method: DML, partition, and DDL operations are blocked on external collections
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_loaded_basic_collection(client, minio_env, external_prefix, num_rows=50)
        check_items = {ct.err_code: 1100, ct.err_msg: err_msg}
        if op_name == "insert":
            self.insert(
                client,
                collection_name=coll,
                data=[
                    {
                        "id": 99999,
                        "value": 1.0,
                        "embedding": [0.0] * ct.default_dim,
                    }
                ],
                check_task=CheckTasks.err_res,
                check_items=check_items,
            )
        elif op_name == "upsert":
            self.upsert(
                client,
                collection_name=coll,
                data=[
                    {
                        "__virtual_pk__": 0,
                        "id": 0,
                        "value": 0.0,
                        "embedding": [0.0] * ct.default_dim,
                    }
                ],
                check_task=CheckTasks.err_res,
                check_items=check_items,
            )
        elif op_name == "delete":
            self.delete(
                client, collection_name=coll, filter="id >= 0", check_task=CheckTasks.err_res, check_items=check_items
            )
        elif op_name == "flush":
            self.flush(client, collection_name=coll, check_task=CheckTasks.err_res, check_items=check_items)
        elif op_name == "create_partition":
            self.create_partition(
                client,
                collection_name=coll,
                partition_name="p1",
                check_task=CheckTasks.err_res,
                check_items=check_items,
            )
        elif op_name == "drop_partition":
            self.drop_partition(
                client,
                collection_name=coll,
                partition_name="_default",
                check_task=CheckTasks.err_res,
                check_items=check_items,
            )
        elif op_name == "compact":
            self.compact(client, collection_name=coll, check_task=CheckTasks.err_res, check_items=check_items)
        elif op_name == "add_collection_field":
            self.add_collection_field(
                client,
                collection_name=coll,
                field_name="new_field",
                data_type=DataType.INT64,
                nullable=True,
                check_task=CheckTasks.err_res,
                check_items=check_items,
            )
        elif op_name == "truncate_collection":
            self.truncate_collection(
                client,
                collection_name=coll,
                check_task=CheckTasks.err_res,
                check_items=check_items,
            )
        log.info(f"[{op_name}] correctly rejected")

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_truncate_collection_rejected_keeps_external_source(
        self, minio_env, external_prefix
    ):
        """
        target: test MilvusClient external table truncate collection rejected keeps external source (milvus#49343)
        method: milvus#49343: truncate_collection is forbidden for external collections
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_loaded_basic_collection(client, minio_env, external_prefix, num_rows=50)
        before = self.describe_collection(client, coll)[0]
        self.truncate_collection(
            client,
            collection_name=coll,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "external"},
        )
        after = self.describe_collection(client, coll)[0]
        assert after.get("external_source") == before.get("external_source")
        assert after.get("external_spec") == before.get("external_spec")

        self.release_collection(client, coll)
        self.refresh_and_wait(client, coll)
        self.load_collection(client, coll)
        assert self.query_count(client, coll) == 50


# ============================================================
# 5. Refresh semantics + job management
# ============================================================


class TestMilvusClientExternalTableRefresh(ExternalTableTestBase):
    """External refresh: basic, incremental, atomic source override, concurrent, jobs."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_refresh_basic_picks_up_data(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table refresh basic picks up data
        method: Initial refresh sees uploaded data; uploading more grows count
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = self.prepare_loaded_basic_collection(
            client,
            minio_env,
            external_prefix,
            filename="part0.parquet",
            num_rows=ct.default_nb,
        )
        ext_key = external_prefix["key"]
        assert self.query_count(client, coll) == ct.default_nb

        # Append more data, re-refresh, require release+load.
        self.release_collection(client, coll)
        upload_basic_data(
            minio_client, cfg, ext_key, filename="part1.parquet", num_rows=ct.default_nb, start_id=ct.default_nb
        )
        self.refresh_and_wait(client, coll)
        self.load_collection(client, coll)

        assert self.query_count(client, coll) == ct.default_nb * 2

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_refresh_incremental_add_remove(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table refresh incremental add remove
        method: Add a file, remove a file — row count tracks fragment changes
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        for idx, start in enumerate([0, 500]):
            upload_parquet(
                minio_client, cfg["bucket"], f"{ext_key}/data_{idx}.parquet", gen_basic_parquet_bytes(500, start)
            )

        schema = _build_basic_schema(self, client, ext_url)
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)
        assert self.query_count(client, coll) == 1000

        self.release_collection(client, coll)
        minio_client.remove_object(cfg["bucket"], f"{ext_key}/data_1.parquet")
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data_2.parquet", gen_basic_parquet_bytes(300, 2000))
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)

        assert self.query_count(client, coll) == 800
        assert len(self.query(client, coll, filter="id >= 0 && id < 500", output_fields=["id"])[0]) == 500
        assert len(self.query(client, coll, filter="id >= 500 && id < 1000", output_fields=["id"])[0]) == 0
        assert len(self.query(client, coll, filter="id >= 2000 && id < 2300", output_fields=["id"])[0]) == 300

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_refresh_override_source(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table refresh override source
        method: refresh(external_source=NEW) atomically rebinds the collection
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_key = external_prefix["key"]

        key_a, key_b = f"{ext_key}/src_a", f"{ext_key}/src_b"
        url_a = build_external_source(cfg, key_a)
        url_b = build_external_source(cfg, key_b)
        upload_parquet(minio_client, cfg["bucket"], f"{key_a}/data.parquet", gen_basic_parquet_bytes(100, 0))
        upload_parquet(minio_client, cfg["bucket"], f"{key_b}/data.parquet", gen_basic_parquet_bytes(100, 5000))

        expected_spec = build_external_spec(cfg)
        schema = _build_basic_schema(self, client, url_a, ext_spec=expected_spec)
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)
        ids_a = sorted(r["id"] for r in self.query(client, coll, filter="id >= 0", output_fields=["id"], limit=200)[0])
        assert ids_a == list(range(0, 100))

        self.release_collection(client, coll)
        self.refresh_and_wait(client, coll, external_source=url_b, external_spec=expected_spec)
        self.load_collection(client, coll)

        ids_b = sorted(r["id"] for r in self.query(client, coll, filter="id >= 0", output_fields=["id"], limit=200)[0])
        assert ids_b == list(range(5000, 5100))

        info = self.describe_collection(client, coll)[0]
        assert info.get("external_source") == url_b, (
            f"override source was not persisted: {info.get('external_source')!r}"
        )
        assert_external_spec_persisted(info.get("external_spec"), expected_spec, "override external_spec")

        fresh_client = self._client()
        fresh_info = fresh_client.describe_collection(coll)
        assert fresh_info.get("external_source") == url_b, (
            f"fresh client sees stale source: {fresh_info.get('external_source')!r}"
        )
        assert_external_spec_persisted(fresh_info.get("external_spec"), expected_spec, "fresh external_spec")

        self.release_collection(client, coll)
        self.refresh_and_wait(client, coll)
        self.load_collection(client, coll)
        ids_reuse = sorted(
            r["id"] for r in self.query(client, coll, filter="id >= 0", output_fields=["id"], limit=200)[0]
        )
        assert ids_reuse == list(range(5000, 5100)), "reuse refresh reverted to the pre-override source"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_failed_refresh_override_keeps_previous_data_and_spec(
        self, minio_env, external_prefix
    ):
        """
        target: test MilvusClient external table failed refresh override keeps previous data and spec
        method: A failed source override must not publish new metadata or drop old data
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_key = external_prefix["key"]

        good_key = f"{ext_key}/good"
        good_url = build_external_source(cfg, good_key)
        expected_spec = build_external_spec(cfg)
        upload_basic_data(minio_client, cfg, good_key, num_rows=100, start_id=0)

        schema = _build_basic_schema(self, client, good_url, ext_spec=expected_spec)
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)
        assert sorted(
            r["id"] for r in self.query(client, coll, filter="id >= 0", output_fields=["id"], limit=100)[0]
        ) == list(range(100))

        self.release_collection(client, coll)
        missing_bucket = f"nosuchbucket-{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
        bad_url = f"s3://{cfg['address']}/{missing_bucket}/external/path/"
        self.refresh_and_expect_failed(
            client,
            coll,
            timeout=60,
            external_source=bad_url,
            external_spec=expected_spec,
            reason_terms=("bucket", "nosuchbucket", "no_such_bucket"),
        )

        info = self.describe_collection(client, coll)[0]
        assert info.get("external_source") == good_url, (
            f"failed refresh override polluted external_source: {info.get('external_source')!r}"
        )
        assert_external_spec_persisted(info.get("external_spec"), expected_spec, "failed override external_spec")

        self.load_collection(client, coll)
        ids = sorted(r["id"] for r in self.query(client, coll, filter="id >= 0", output_fields=["id"], limit=100)[0])
        assert ids == list(range(100)), f"old data unavailable after failed override: {ids[:10]}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_external_table_refresh_second_concurrent_returns_existing_or_rejects(
        self, minio_env, external_prefix
    ):
        """
        target: test MilvusClient external table refresh second concurrent returns existing or rejects (milvus#49231)
        method: milvus#49231: duplicate refresh must not return a dropped new job_id
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_large_parquet_with_row_groups(20_000, 0, row_group_size=500),
        )

        schema = _build_basic_schema(self, client, ext_url)
        self.create_collection(client, collection_name=coll, schema=schema)
        job_first = self.refresh_external_collection(client, collection_name=coll)[0]
        self.refresh_external_collection(
            client,
            collection_name=coll,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "in progress"},
        )
        first_progress = self.wait_refresh_progress(client, job_first)
        assert first_progress.state == "RefreshCompleted"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_list_refresh_jobs(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table list refresh jobs
        method: After refreshes, list_refresh_external_collection_jobs returns them
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", gen_basic_parquet_bytes(100, 0))

        schema = _build_basic_schema(self, client, ext_url)
        self.create_collection(client, collection_name=coll, schema=schema)
        job_id = self.refresh_and_wait(client, coll)
        jobs = self.list_refresh_external_collection_jobs(client, collection_name=coll)[0]
        assert len(jobs) >= 1, f"expected ≥1 job, got {jobs}"
        job_ids = [j.job_id for j in jobs]
        assert job_id in job_ids, f"job {job_id} missing from {job_ids}"
        log.info(f"refresh jobs: {[(j.job_id, j.state) for j in jobs]}")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "num_files,rows_per_file",
        [
            (1, 500),  # single large file
            (5, 100),  # small uniform files
            (10, 50),  # many tiny files
        ],
        ids=["1x500", "5x100", "10x50"],
    )
    def test_milvus_client_external_table_refresh_many_files(
        self, num_files, rows_per_file, minio_env, external_prefix
    ):
        """
        target: test MilvusClient external table refresh many files
        method: Refresh aggregates rows correctly across many parquet files in one prefix
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        # Upload num_files sibling parquet files at <prefix>/file_<n>.parquet
        for idx in range(num_files):
            upload_parquet(
                minio_client,
                cfg["bucket"],
                f"{ext_key}/file_{idx:03d}.parquet",
                gen_basic_parquet_bytes(rows_per_file, idx * rows_per_file),
            )

        schema = _build_basic_schema(self, client, ext_url)
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)

        total = num_files * rows_per_file
        assert self.query_count(client, coll) == total

        # Spot-check a row from each file survived ingestion
        for idx in range(num_files):
            first_id = idx * rows_per_file
            rows = self.query(
                client,
                coll,
                filter=f"id == {first_id}",
                output_fields=["id", "value"],
            )[0]
            assert len(rows) == 1, f"row id={first_id} missing after refresh"
            assert abs(rows[0]["value"] - first_id * 1.5) < 1e-4

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "case_name", ["non_existent_collection", "normal_collection"], ids=["non_existent", "normal"]
    )
    def test_milvus_client_external_table_refresh_invalid_target_rejected(self, case_name):
        """
        target: test MilvusClient external table refresh invalid target rejected
        method: Refresh on a non-existent or normal collection must fail at RPC
        expected: behavior matches the case assertion.
        """
        client = self._client()
        if case_name == "non_existent_collection":
            coll = "non_existent_xyz"
        else:
            coll = cf.gen_collection_name_by_testcase_name()
            schema = self.create_schema(
                client,
            )[0]
            self.add_field(schema, "id", DataType.INT64, is_primary=True)
            self.add_field(schema, "vec", DataType.FLOAT_VECTOR, dim=4)
            self.create_collection(client, collection_name=coll, schema=schema)
        err_hint = "collection" if case_name == "non_existent_collection" else "external"
        self.refresh_external_collection(
            client,
            collection_name=coll,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: err_hint},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_refresh_empty_source_behavior(self, external_prefix):
        """
        target: test MilvusClient external table refresh empty source behavior
        method: Refresh on an empty external source — accepted job either completes with 0 rows or fails with a clear reason
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url = external_prefix["url"]
        schema = _build_basic_schema(self, client, ext_url)
        self.create_collection(client, collection_name=coll, schema=schema)
        job_id = self.refresh_external_collection(client, collection_name=coll)[0]
        progress = self.wait_refresh_progress(client, job_id, timeout=60)
        assert progress.state in ("RefreshCompleted", "RefreshFailed"), (
            f"empty-source refresh stuck in {progress.state} (expected terminal state)"
        )
        log.info(f"empty-source refresh terminal state={progress.state}")

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_refresh_empty_external_source_rejected_or_failed(self, external_prefix):
        """
        target: test MilvusClient external table refresh empty external source rejected or failed (milvus#49230)
        method: milvus#49230: external_source="" must not leave refresh Pending forever
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        schema = _build_basic_schema(self, client, external_prefix["url"])
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_external_collection(
            client,
            coll,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "both provided or both omitted"},
            external_source="",
            external_spec=build_external_spec(),
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "fmt",
        BASIC_FORMATS,
        ids=BASIC_FORMAT_IDS,
    )
    def test_milvus_client_external_table_refresh_zero_row_format_terminal(self, fmt, minio_env, external_prefix):
        """
        target: test MilvusClient external table refresh zero row format terminal (milvus#49225)
        method: milvus#49225: 0-row external files must not crash or hang refresh
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        source, ext_spec = write_basic_format_dataset(
            fmt,
            minio_client,
            cfg,
            ext_url,
            ext_key,
            batches=[(0, 0)],
        )
        schema = _build_basic_schema(self, client, source, ext_spec=ext_spec)

        self.create_collection(client, collection_name=coll, schema=schema)
        job_id = self.refresh_external_collection(client, collection_name=coll)[0]
        progress = self.wait_refresh_progress(client, job_id, timeout=60)
        assert progress.state in ("RefreshCompleted", "RefreshFailed"), (
            f"[{fmt}] zero-row refresh stuck in {progress.state}"
        )

        if progress.state == "RefreshCompleted":
            self.index_and_load(client, coll)
            assert self.query_count(client, coll) == 0
        else:
            assert progress.reason, f"[{fmt}] failed zero-row refresh should expose a reason"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_refresh_nonexistent_bucket_fails_terminal(self, minio_env):
        """
        target: test MilvusClient external table refresh nonexistent bucket fails terminal (milvus#49233)
        method: milvus#49233: NoSuchBucket must fail terminally, not retry forever
        expected: behavior matches the case assertion.
        """
        _minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        missing_bucket = f"nosuchbucket-{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
        ext_url = f"s3://{cfg['address']}/{missing_bucket}/external/path/"

        schema = _build_basic_schema(self, client, ext_url, ext_spec=build_external_spec(cfg))
        self.create_collection(client, collection_name=coll, schema=schema)
        job_id, progress = self.refresh_and_expect_failed(
            client,
            coll,
            timeout=60,
            reason_terms=("bucket", "no_such_bucket", "nosuchbucket"),
        )
        assert progress.state == "RefreshFailed", f"NoSuchBucket refresh should fail terminally, got {progress.state}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_external_table_refresh_schema_mismatch_caught_eventually(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table refresh schema mismatch caught eventually
        method: verify refresh schema mismatch caught eventually
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=100)

        # Mismatched mappings — parquet has columns id/value/embedding;
        # the schema points at wrong_col_a/wrong_col_b.
        schema = self.create_schema(client, external_source=ext_url, external_spec=build_external_spec(cfg))[0]
        self.add_field(schema, "x", DataType.INT64, external_field="wrong_col_a")
        self.add_field(schema, "vec", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="wrong_col_b")

        self.create_collection(client, collection_name=coll, schema=schema)

        job_id = self.refresh_external_collection(client, collection_name=coll)[0]

        progress = self.wait_refresh_progress(client, job_id, timeout=60, return_on_reason=True)
        if progress.state == "RefreshFailed" or progress.reason:
            log.info(f"refresh caught the mismatch: state={progress.state}, reason={progress.reason}")
            return

        # Refresh accepted; the mismatch must surface at create_index/load/query.
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name="vec", index_type="AUTOINDEX", metric_type="L2")
        self.create_index(client, coll, idx)
        self.load_collection(client, coll, timeout=60)
        self.query(
            client,
            coll,
            filter="x == 0",
            output_fields=["x"],
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "wrong_col"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_case_mismatched_external_fields_rejected(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table case mismatched external fields rejected (milvus#49232)
        method: milvus#49232: case-mismatched parquet column names must not load silently
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_basic_data(minio_client, cfg, ext_key, num_rows=50)

        schema = self.create_schema(
            client,
            external_source=ext_url,
            external_spec=build_external_spec(cfg),
        )[0]
        self.add_field(schema, "id", DataType.INT64, external_field="ID")
        self.add_field(schema, "value", DataType.FLOAT, external_field="Value")
        self.add_field(schema, "embedding", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="Embedding")

        self.create_collection(client, collection_name=coll, schema=schema)

        job_id = self.refresh_external_collection(client, collection_name=coll)[0]
        progress = self.wait_refresh_progress(client, job_id, timeout=60)
        if progress.state == "RefreshFailed":
            return
        assert progress.state == "RefreshCompleted", f"case-mismatch refresh stuck in {progress.state}"

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="embedding", index_type="AUTOINDEX", metric_type="L2")
        self.create_index(client, coll, index_params)
        self.load_collection(
            client,
            coll,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "Embedding"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_schemaless_reader_extra_columns_ignored(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table schemaless reader extra columns ignored
        method: Parquet has 9 scalar columns plus embedding; the external schema only projects 3 of them
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        nb = 100
        # gen_all_scalar_parquet_bytes writes id + 9 scalars + embedding.
        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_all_scalar_parquet_bytes(nb, 0),
        )

        # Project only id, val_int32, embedding — the other 7 scalar columns
        # exist in the file but are not declared in the schema.
        schema = self.create_schema(client, external_source=ext_url, external_spec=build_external_spec(cfg))[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(schema, "val_int32", DataType.INT32, external_field="val_int32")
        self.add_field(schema, "embedding", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="embedding")
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)
        assert self.query_count(client, coll) == nb

        res = self.query(client, coll, filter="id == 5", output_fields=["id", "val_int32"])[0]
        assert len(res) == 1
        assert res[0]["id"] == 5
        assert res[0]["val_int32"] == 5 * 7  # value_fn for val_int32 was i*7

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_refresh_scans_flat_prefix_only(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table refresh scans flat prefix only
        method: verify refresh scans flat prefix only
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        # 3 files directly under the prefix — should be ingested
        flat_files = [
            ("flat_a.parquet", 100, 0),
            ("flat_b.parquet", 100, 100),
            ("flat_c.parquet", 100, 200),
        ]
        for name, nrows, start_id in flat_files:
            upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/{name}", gen_basic_parquet_bytes(nrows, start_id))

        # 2 files inside subdirs — should be ignored by refresh
        nested_files = [
            ("year=2025/month=01/nested_a.parquet", 100, 10000),
            ("subdir/nested_b.parquet", 100, 20000),
        ]
        for rel, nrows, start_id in nested_files:
            upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/{rel}", gen_basic_parquet_bytes(nrows, start_id))

        schema = _build_basic_schema(self, client, ext_url)
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)

        # Only the 3 flat files should count (300 rows), not the 2 nested
        assert self.query_count(client, coll) == 300

        # Nested-file id ranges must be absent
        for _, _, start_id in nested_files:
            got = self.query(
                client,
                coll,
                filter=f"id >= {start_id} && id < {start_id + 100}",
                output_fields=["id"],
                limit=200,
            )[0]
            assert len(got) == 0, f"ids [{start_id}..{start_id + 100}) unexpectedly present (subdir ingested)"


# ============================================================
# 6. DQL — query / search / hybrid_search / iterators / get / pagination
# ============================================================


class TestMilvusClientExternalTableDQL(ExternalTableTestBase):
    """Read-side operations on external collections."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "filter_expr,expected",
        [
            ("", ct.default_nb),
            ("id < 10", 10),
            ("id >= 100 && id < 150", 50),
            ("value > 0.0 && id < 50", 49),  # value=1.5*id, id==0 excluded
            ("id in [0, 1, 2, 3, 99]", 5),
        ],
    )
    def test_milvus_client_external_table_query_filter(self, filter_expr, expected, minio_env, external_prefix):
        """
        target: test MilvusClient external table query filter
        method: Scalar filters return the expected row count
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_loaded_basic_collection(client, minio_env, external_prefix)

        if filter_expr == "":
            assert self.query_count(client, coll) == expected
        else:
            res = self.query(client, coll, filter=filter_expr, output_fields=["id"], limit=ct.default_nb)[0]
            assert len(res) == expected

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("metric", ["L2", "IP", "COSINE"])
    def test_milvus_client_external_table_search_metric(self, metric, minio_env, external_prefix):
        """
        target: test MilvusClient external table search metric
        method: Search under each metric returns topK with correctly ordered distances
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_indexed_basic_collection(
            client,
            minio_env,
            external_prefix,
            index_type="FLAT",
            metric_type=metric,
        )
        self.load_collection(client, coll)

        hits = self.search(
            client,
            coll,
            data=_float_vectors([0], ct.default_dim).tolist(),
            limit=5,
            anns_field="embedding",
            output_fields=["id"],
        )[0][0]
        assert len(hits) == 5
        distances = [h["distance"] for h in hits]
        if metric == "L2":
            assert distances == sorted(distances)
        else:
            assert distances == sorted(distances, reverse=True)
        expected_top_id = ct.default_nb - 1 if metric == "IP" else 0
        assert hits[0]["id"] == expected_top_id, (
            f"{metric} top hit mismatch: got id={hits[0]['id']}, expected {expected_top_id}"
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_search_with_scalar_filter(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table search with scalar filter
        method: Vector search combined with scalar filter (hybrid via `search(filter=...)`)
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_loaded_basic_collection(client, minio_env, external_prefix)

        hits = self.search(
            client,
            coll,
            data=[[0.0] * ct.default_dim],
            limit=10,
            anns_field="embedding",
            output_fields=["id"],
            filter="id >= 50 && id < 100",
        )[0][0]
        assert len(hits) == 10
        assert all(50 <= h["id"] < 100 for h in hits), f"filter violated: {[h['id'] for h in hits]}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_hybrid_search_multi_vector(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table hybrid search multi vector
        method: hybrid_search over two vector fields with RRF reranker
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_parquet(
            minio_client, cfg["bucket"], f"{ext_key}/data.parquet", gen_multi_vector_parquet_bytes(ct.default_nb, 0)
        )

        schema = self.create_schema(client, external_source=ext_url, external_spec=build_external_spec(cfg))[0]
        self.add_field(schema, "id", DataType.INT64, external_field="id")
        self.add_field(schema, "value", DataType.FLOAT, external_field="value")
        self.add_field(schema, "dense_vec", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="dense_vec")
        self.add_field(schema, "bin_vec", DataType.BINARY_VECTOR, dim=ct.default_dim, external_field="bin_vec")
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        idx = self.prepare_index_params(
            client,
        )[0]
        idx.add_index(field_name="dense_vec", index_type="AUTOINDEX", metric_type="L2")
        idx.add_index(field_name="bin_vec", index_type="BIN_FLAT", metric_type="HAMMING")
        self.create_index(client, coll, idx)
        self.load_collection(client, coll)

        req1 = AnnSearchRequest(
            data=[[0.0] * ct.default_dim],
            anns_field="dense_vec",
            limit=10,
            param={"metric_type": "L2"},
        )
        req2 = AnnSearchRequest(
            data=[b"\x00" * (ct.default_dim // 8)],
            anns_field="bin_vec",
            limit=10,
            param={"metric_type": "HAMMING"},
        )
        hits = self.hybrid_search(
            client,
            collection_name=coll,
            reqs=[req1, req2],
            ranker=RRFRanker(),
            limit=5,
            output_fields=["id"],
        )[0]
        assert len(hits) == 1 and len(hits[0]) == 5, f"hybrid_search returned {hits}"
        ids = [h["id"] for h in hits[0]]
        assert len(set(ids)) == len(ids), f"hybrid_search returned duplicate ids: {ids}"
        assert all(0 <= row_id < ct.default_nb for row_id in ids), f"hybrid_search ids out of range: {ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_query_iterator(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table query iterator
        method: query_iterator lazily paginates through rows in batch_size chunks
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_loaded_basic_collection(client, minio_env, external_prefix, num_rows=ct.default_nb)

        it = self.query_iterator(
            client,
            collection_name=coll,
            filter="id >= 0",
            batch_size=50,
            output_fields=["id"],
        )[0]
        total = 0
        while True:
            batch = it.next()
            if not batch:
                break
            total += len(batch)
            assert len(batch) <= 50
        it.close()
        assert total == ct.default_nb, f"iterator yielded {total}, expected {ct.default_nb}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_search_iterator(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table search iterator
        method: search_iterator lazily paginates vector search results
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_loaded_basic_collection(client, minio_env, external_prefix, num_rows=ct.default_nb)

        it = self.search_iterator(
            client,
            collection_name=coll,
            data=[[0.0] * ct.default_dim],
            anns_field="embedding",
            batch_size=20,
            limit=80,
            output_fields=["id"],
        )[0]
        total = 0
        while True:
            batch = it.next()
            if not batch:
                break
            total += len(batch)
        it.close()
        assert total == 80, f"iterator yielded {total}, expected 80"

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_get_by_virtual_pk(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table get by virtual pk
        method: get() by virtual PK returns the requested rows
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_loaded_basic_collection(client, minio_env, external_prefix, num_rows=50)

        # Resolve virtual PKs of the first 5 rows via query
        first_five = self.query(
            client,
            coll,
            filter="id >= 0 && id < 5",
            output_fields=["id", "__virtual_pk__"],
            limit=5,
        )[0]
        vpks = [r["__virtual_pk__"] for r in first_five]
        assert len(vpks) == 5

        fetched = self.get(client, collection_name=coll, ids=vpks, output_fields=["id"])[0]
        assert len(fetched) == 5
        assert {r["id"] for r in fetched} == {r["id"] for r in first_five}

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_external_table_pagination_offset_limit(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table pagination offset limit
        method: query(offset=..., limit=...) paginates correctly
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_loaded_basic_collection(client, minio_env, external_prefix, num_rows=50)

        page1 = self.query(client, coll, filter="id >= 0", output_fields=["id"], offset=0, limit=10)[0]
        page2 = self.query(client, coll, filter="id >= 0", output_fields=["id"], offset=10, limit=10)[0]
        assert len(page1) == 10 and len(page2) == 10
        ids1 = {r["id"] for r in page1}
        ids2 = {r["id"] for r in page2}
        assert ids1.isdisjoint(ids2), "paginated pages should not overlap"
        assert all(0 <= row_id < 50 for row_id in ids1 | ids2), f"paginated ids out of range: {ids1 | ids2}"


# ============================================================
# 7. Lifecycle: alias, release/reload, drop+recreate
# ============================================================


class TestMilvusClientExternalTableLifecycle(ExternalTableTestBase):
    """Alias lifecycle and coarse lifecycle edges."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_alias_create_alter_drop(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table alias create alter drop
        method: Alias can be created, rebound, and dropped for external collections
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll_a = cf.gen_collection_name_by_testcase_name() + "_a"
        coll_b = cf.gen_collection_name_by_testcase_name() + "_b"
        ext_key = external_prefix["key"]
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/a/data.parquet", gen_basic_parquet_bytes(30, 0))
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/b/data.parquet", gen_basic_parquet_bytes(40, 10000))

        url_a = build_external_source(cfg, f"{ext_key}/a")
        url_b = build_external_source(cfg, f"{ext_key}/b")
        schema_a = _build_basic_schema(self, client, url_a)
        schema_b = _build_basic_schema(self, client, url_b)
        self.create_collection(client, collection_name=coll_a, schema=schema_a)
        self.create_collection(client, collection_name=coll_b, schema=schema_b)

        alias = f"alias_{random.randint(10000, 99999)}"
        self.refresh_and_wait(client, coll_a)
        self.refresh_and_wait(client, coll_b)
        self.index_and_load(client, coll_a)
        self.index_and_load(client, coll_b)

        self.create_alias(client, collection_name=coll_a, alias=alias)
        via_alias_a = self.query(client, alias, filter="id >= 0", output_fields=["id"], limit=100)[0]
        assert len(via_alias_a) == 30

        self.alter_alias(client, collection_name=coll_b, alias=alias)
        via_alias_b = self.query(client, alias, filter="id >= 0", output_fields=["id"], limit=100)[0]
        assert len(via_alias_b) == 40
        assert all(r["id"] >= 10000 for r in via_alias_b)

        self.drop_alias(client, alias=alias)
        self.query(
            client,
            alias,
            filter="id >= 0",
            output_fields=["id"],
            limit=1,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "collection"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_external_table_refresh_requires_release_to_see_new_data(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table refresh requires release to see new data
        method: Without release+load, queries still see pre-refresh data
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = self.prepare_loaded_basic_collection(
            client,
            minio_env,
            external_prefix,
            filename="part0.parquet",
            num_rows=100,
        )
        ext_key = external_prefix["key"]
        assert self.query_count(client, coll) == 100

        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/part1.parquet", gen_basic_parquet_bytes(100, 100))
        self.refresh_and_wait(client, coll)

        stale = self.query_count(client, coll)
        assert stale == 100, f"without release+load expected 100, got {stale}"

        self.release_collection(client, coll)
        self.load_collection(client, coll)
        assert self.query_count(client, coll) == 200

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_external_table_concurrent_query_during_refresh_release_load(
        self, minio_env, external_prefix
    ):
        """
        target: test MilvusClient external table concurrent query during refresh release load
        method: Continuous query thread keeps running while the main thread uploads a new file, refreshes, releases, and reloads
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = self.prepare_loaded_basic_collection(
            client,
            minio_env,
            external_prefix,
            filename="data0.parquet",
            num_rows=500,
        )
        ext_key = external_prefix["key"]
        assert self.query_count(client, coll) == 500

        # Upload a new file and submit refresh while the old loaded snapshot is still queryable.
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data1.parquet", gen_basic_parquet_bytes(300, 500))
        job_id = self.refresh_external_collection(client, collection_name=coll)[0]
        assert self.query_count(client, coll) == 500, "loaded snapshot should remain stable during refresh"
        progress = self.wait_refresh_progress(client, job_id)
        assert progress.state == "RefreshCompleted", f"refresh job {job_id} did not complete: {progress}"
        self.release_collection(client, coll)
        self.load_collection(client, coll)
        assert self.query_count(client, coll) == 800, "release+load should publish refreshed snapshot"

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_external_table_file_deleted_then_reload(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table file deleted then reload
        method: Delete a source parquet file (without a refresh), then release+load
        expected: reload fails because the published refresh metadata still references the deleted file.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data0.parquet", gen_basic_parquet_bytes(100, 0))
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data1.parquet", gen_basic_parquet_bytes(100, 100))

        schema = _build_basic_schema(self, client, ext_url)
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)
        assert self.query_count(client, coll) == 200

        # Delete a backing file with no refresh in between.
        minio_client.remove_object(cfg["bucket"], f"{ext_key}/data1.parquet")
        log.info("deleted data1.parquet without a follow-up refresh")

        self.release_collection(client, coll)
        self.load_collection(
            client,
            coll,
            timeout=10,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: "wait for loading collection timeout"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_external_table_drop_and_recreate_same_name(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table drop and recreate same name
        method: Drop then recreate with the same name should succeed and serve data
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_loaded_basic_collection(client, minio_env, external_prefix, num_rows=50)
        ext_url = external_prefix["url"]
        assert self.query_count(client, coll) == 50

        self.drop_collection(client, coll)
        assert not self.has_collection(client, coll)[0]

        schema2 = _build_basic_schema(self, client, ext_url)
        self.create_collection(client, collection_name=coll, schema=schema2)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)
        assert self.query_count(client, coll) == 50


# ============================================================
# 8. Cross-bucket external sources
# ============================================================


class TestMilvusClientExternalTableCrossBucket(ExternalTableTestBase):
    """External data living in a separate MinIO bucket from Milvus's own."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_cross_bucket_source(self, minio_env):
        """
        target: test MilvusClient external table cross bucket source
        method: End-to-end refresh/load/query against a different bucket
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        cross_bucket = "external-cross-bucket"
        if not minio_client.bucket_exists(cross_bucket):
            minio_client.make_bucket(cross_bucket)

        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        key_prefix = f"external-e2e-cross/{coll}"
        nb = 500
        for i in range(2):
            upload_parquet(
                minio_client,
                cross_bucket,
                f"{key_prefix}/data{i}.parquet",
                gen_basic_parquet_bytes(nb, i * nb),
            )
        external_source = f"s3://{cfg['address']}/{cross_bucket}/{key_prefix}/"
        schema = _build_basic_schema(
            self,
            client,
            external_source,
            ext_spec=build_external_spec(cfg),
        )
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)
        assert self.query_count(client, coll) == nb * 2
        log.info(f"cross-bucket: {nb * 2} rows loaded from {cross_bucket} while milvus uses {cfg['bucket']}")
        cleanup_minio_prefix(minio_client, cross_bucket, f"{key_prefix}/")


# ============================================================
# 9. Parquet compression codecs
# ============================================================


class TestMilvusClientExternalTableParquetCodecs(ExternalTableTestBase):
    """Parquet compression codec compatibility for external collections.

    Milvus's bundled Arrow needs `arrow:with_snappy=True` and
    `arrow:with_lz4=True` (added in Part8-1/4 #49061). If either is
    dropped or rebuilt without codec support, compressed parquet
    silently breaks at query time. This test guards against regressions.
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("codec", ["snappy", "lz4", "gzip", "zstd", "none"])
    def test_milvus_client_external_table_parquet_codec_readable(self, codec, minio_env, external_prefix):
        """
        target: test MilvusClient external table parquet codec readable
        method: Upload a parquet compressed with `codec`, refresh, query the count back
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name() + f"_{codec}"
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        nb = 256
        # pyarrow accepts 'NONE' (uppercase) for uncompressed; lowercase 'none' fails.
        pq_codec = "NONE" if codec == "none" else codec
        data = gen_parquet_bytes_with_codec(nb, 0, pq_codec)
        upload_parquet(minio_client, cfg["bucket"], f"{ext_key}/data.parquet", data)

        schema = _build_basic_schema(self, client, ext_url)
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)
        count = self.query_count(client, coll)
        assert count == nb, f"codec={codec}: expected {nb}, got {count}"
        log.info(f"codec={codec}: {nb} rows round-tripped")


# ============================================================
# 10. Read-only / admin operations allowed on external collections
# ============================================================


class TestMilvusClientExternalTableReadOps(ExternalTableTestBase):
    """Operations normal collections support that should also work on
    external collections: stats, state, partitions, segments, rename,
    refresh_load, property alter/drop, database scoping."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "op_name",
        [
            "list_partitions",
            "get_collection_stats",
            "get_partition_stats",
            "list_segments",
            "refresh_load",
        ],
        ids=[
            "list_partitions",
            "get_collection_stats",
            "get_partition_stats",
            "list_segments",
            "refresh_load",
        ],
    )
    def test_milvus_client_external_table_read_operation(self, op_name, minio_env, external_prefix):
        """
        target: test MilvusClient external table read operation
        method: Read/admin operations supported by normal collections work on external collections
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_loaded_basic_collection(client, minio_env, external_prefix)

        if op_name == "list_partitions":
            parts = self.list_partitions(client, collection_name=coll)[0]
            assert list(parts) == ["_default"], f"expected only _default, got {parts}"
            assert self.has_partition(client, collection_name=coll, partition_name="_default")[0] is True
            assert self.has_partition(client, collection_name=coll, partition_name="nonexistent_xyz")[0] is False
        elif op_name == "get_collection_stats":
            stats = self.get_collection_stats(client, collection_name=coll)[0]
            log.info(f"collection stats: {stats}")
            row_count = int(stats.get("row_count", 0))
            assert row_count == ct.default_nb, f"expected row_count={ct.default_nb}, got {row_count}"
        elif op_name == "get_partition_stats":
            stats = self.get_partition_stats(client, collection_name=coll, partition_name="_default")[0]
            log.info(f"partition stats: {stats}")
            row_count = int(stats.get("row_count", 0))
            assert row_count == ct.default_nb
        elif op_name == "list_segments":
            persistent = self.list_persistent_segments(client, collection_name=coll)[0]
            assert persistent is not None
            log.info(f"persistent segments (len={len(list(persistent))}): {persistent}")
        elif op_name == "refresh_load":
            self.refresh_load(client, collection_name=coll)
            assert self.query_count(client, coll) == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_get_load_state_transitions(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table get load state transitions
        method: get_load_state reports Loaded after load, NotLoad after release
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_indexed_basic_collection(client, minio_env, external_prefix, num_rows=ct.default_nb)

        # Before load
        state = self.get_load_state(client, collection_name=coll)[0]
        log.info(f"[pre-load] state={state}")
        assert "not" in str(state).lower() or "notload" in str(state).lower().replace("_", "")

        # After load
        self.load_collection(client, coll)
        state2 = self.get_load_state(client, collection_name=coll)[0]
        log.info(f"[post-load] state={state2}")
        assert "load" in str(state2).lower() and "not" not in str(state2).lower().split("state")[-1]

        # After release
        self.release_collection(client, coll)
        state3 = self.get_load_state(client, collection_name=coll)[0]
        log.info(f"[post-release] state={state3}")
        assert "not" in str(state3).lower() or "notload" in str(state3).lower().replace("_", "")

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_external_table_partition_load_release_default(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table partition load release default
        method: verify partition load release default
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_indexed_basic_collection(client, minio_env, external_prefix, num_rows=ct.default_nb)

        self.load_partitions(client, collection_name=coll, partition_names=["_default"])
        assert self.query_count(client, coll) == ct.default_nb

        self.release_partitions(client, collection_name=coll, partition_names=["_default"])
        state = self.get_load_state(client, collection_name=coll)[0]
        log.info(f"state after release_partitions: {state}")

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_rename_collection(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table rename collection
        method: rename_collection should succeed; new name answers queries
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_loaded_basic_collection(client, minio_env, external_prefix, num_rows=50)
        new_name = coll + "_renamed"
        self.rename_collection(client, old_name=coll, new_name=new_name)

        assert not self.has_collection(client, coll)[0]
        assert self.has_collection(client, new_name)[0]
        assert self.query_count(client, new_name) == 50

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_external_table_alter_and_drop_collection_description(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table alter and drop collection description
        method: alter_collection_properties(description=...) surfaces via describe; drop_collection_properties removes the property key
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_refreshed_basic_collection(client, minio_env, external_prefix, num_rows=20)
        self.alter_collection_properties(
            client,
            collection_name=coll,
            properties={"collection.description": "ext-table description"},
        )

        desc = self.describe_collection(client, coll)[0]
        props = desc.get("properties") or {}
        log.info(f"after alter: properties={props}, description={desc.get('description')}")
        desc_value = desc.get("description") or props.get("collection.description")
        assert desc_value == "ext-table description", f"description was not updated: {desc}"

        self.drop_collection_properties(
            client,
            collection_name=coll,
            property_keys=["collection.description"],
        )
        desc_after_drop = self.describe_collection(client, coll)[0]
        props_after_drop = desc_after_drop.get("properties") or {}
        assert desc_after_drop.get("description") == "ext-table description", (
            f"description should remain top-level collection metadata: {desc_after_drop}"
        )
        assert "collection.description" not in props_after_drop, (
            f"collection.description property should not be persisted in properties: {desc_after_drop}"
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_external_table_drop_index_property(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table drop index property
        method: drop_index_properties removes mmap.enabled that was set earlier
        expected: behavior matches the case assertion.
        """
        client = self._client()
        coll = self.prepare_loaded_basic_collection(client, minio_env, external_prefix)
        self.release_collection(client, coll)
        self.alter_index_properties(
            client,
            collection_name=coll,
            index_name="embedding",
            properties={"mmap.enabled": True},
        )

        info = self.describe_index(client, collection_name=coll, index_name="embedding")[0]
        assert str(info.get("mmap.enabled", "")).lower() == "true"

        self.drop_index_properties(
            client,
            collection_name=coll,
            index_name="embedding",
            property_keys=["mmap.enabled"],
        )

        info2 = self.describe_index(client, collection_name=coll, index_name="embedding")[0]
        log.info(f"after drop mmap property: {info2}")
        # Property should be absent or default "false"
        assert str(info2.get("mmap.enabled", "false")).lower() in ("false", "")

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_client_external_table_external_collection_in_custom_database(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table external collection in custom database
        method: Create an external collection inside a non-default database
        expected: behavior matches the case assertion.
        """
        client = self._client()
        db_name = f"ext_db_{random.randint(10000, 99999)}"

        self.create_database(client, db_name=db_name)

        coll = cf.gen_collection_name_by_testcase_name()
        self.use_database(client, db_name=db_name)
        self.prepare_loaded_basic_collection(
            client,
            minio_env,
            external_prefix,
            num_rows=30,
            collection_name=coll,
        )
        assert self.query_count(client, coll) == 30

        dbs = self.list_databases(
            client,
        )[0]
        assert db_name in list(dbs)

        self.drop_collection(client, coll)
        self.use_database(client, db_name="default")
        self.drop_database(client, db_name=db_name)


# ============================================================
# 9. External file formats — parquet / lance-table / iceberg-table
# ============================================================


class TestMilvusClientExternalTableFormats(ExternalTableTestBase):
    """Coverage for the `external_spec.format` string values."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("fmt", BASIC_FORMATS, ids=BASIC_FORMAT_IDS)
    def test_milvus_client_external_table_basic_format_matrix(self, fmt, minio_env, external_prefix):
        """
        target: test MilvusClient external table basic format matrix
        method: Every format uses the shared writer/spec/source registry
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        batches = [(idx * FORMAT_ROWS_PER_FILE, FORMAT_ROWS_PER_FILE) for idx in range(FORMAT_NUM_FILES)]
        source, ext_spec = write_basic_format_dataset(
            fmt,
            minio_client,
            cfg,
            ext_url,
            ext_key,
            batches,
        )
        schema = _build_basic_schema(self, client, source, ext_spec=ext_spec)
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)
        _assert_basic_format_rows(self, client, coll, fmt, batches)

        hits = self.search(
            client,
            coll,
            data=[[0.0] * ct.default_dim],
            limit=3,
            anns_field="embedding",
            output_fields=["id"],
        )[0][0]
        assert len(hits) == 3
        assert all(0 <= hit["id"] < FORMAT_TOTAL_ROWS for hit in hits), f"[{fmt}] invalid search ids: {hits}"

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("fmt", ["iceberg-table"], ids=["iceberg"])
    def test_milvus_client_external_table_cluster_nightly_heavy_format_smoke(self, fmt, minio_env, external_prefix):
        """
        target: test MilvusClient external table cluster nightly heavy format smoke
        method: Opt-in cluster/nightly smoke for formats that have exposed QueryNode-only bugs
        expected: behavior matches the case assertion.
        """
        if os.environ.get("EXTERNAL_TABLE_CLUSTER_SMOKE", "false").lower() != "true":
            pytest.skip("set EXTERNAL_TABLE_CLUSTER_SMOKE=true on a cluster deployment")

        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]
        batches = [(0, ct.default_nb), (ct.default_nb, ct.default_nb)]
        source, ext_spec = write_basic_format_dataset(fmt, minio_client, cfg, ext_url, ext_key, batches)
        schema = _build_basic_schema(self, client, source, ext_spec=ext_spec)
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        self.index_and_load(client, coll)
        _assert_basic_format_rows(self, client, coll, fmt, batches)
        self.release_collection(client, coll)
        self.load_collection(client, coll)
        assert self.query_count(client, coll) == ct.default_nb * 2


# ============================================================
# 10. Full-matrix coverage: every DataType × every Index × every Format
# ============================================================


class TestMilvusClientExternalTableFullMatrix(ExternalTableTestBase):
    """End-to-end coverage of every supported DataType × Index combination
    on every external file format. A single collection per format carries
    21 fields wired to 8 scalar indexes + 9 vector indexes (4 FloatVector
    variants for AUTOINDEX/FLAT/HNSW/IVF_FLAT, plus Float16/BFloat16/
    Int8/Binary vectors with their canonical indexes)."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_full_matrix_parquet(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table full matrix parquet
        method: Parquet × full DataType × full Index matrix
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        upload_parquet(
            minio_client,
            cfg["bucket"],
            f"{ext_key}/data.parquet",
            gen_full_matrix_parquet_bytes(FULL_MATRIX_NB, 0),
        )
        schema = build_full_matrix_schema(self, client, ext_url)
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        create_full_matrix_indexes(self, client, coll)
        self.load_collection(client, coll)
        _full_matrix_assert_basic(self, client, coll, FULL_MATRIX_NB)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_full_matrix_lance(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table full matrix lance
        method: Lance × full DataType × full Index matrix
        expected: behavior matches the case assertion.
        """
        import tempfile

        import lance

        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        columns = _full_matrix_arrow_columns(FULL_MATRIX_NB, 0)
        table = pa.table(columns)

        with tempfile.TemporaryDirectory(prefix="ext_lance_fm_") as tmpdir:
            local_path = os.path.join(tmpdir, "dataset.lance")
            lance.write_dataset(table, local_path)
            for root, _dirs, files in os.walk(local_path):
                for fname in files:
                    absolute = os.path.join(root, fname)
                    relative = os.path.relpath(absolute, local_path)
                    minio_client.fput_object(
                        cfg["bucket"],
                        f"{ext_key}/{relative}",
                        absolute,
                    )

        schema = build_full_matrix_schema(
            self,
            client,
            ext_url,
            ext_spec=build_external_spec(cfg, fmt="lance-table"),
        )
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        create_full_matrix_indexes(self, client, coll)
        self.load_collection(client, coll)
        _full_matrix_assert_basic(self, client, coll, FULL_MATRIX_NB)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_full_matrix_vortex(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table full matrix vortex
        method: Vortex × full DataType × full Index matrix
        expected: behavior matches the case assertion.
        """
        minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_url, ext_key = external_prefix["url"], external_prefix["key"]

        table = pa.table(
            _full_matrix_arrow_columns(
                FULL_MATRIX_NB,
                0,
                excluded_fields=VORTEX_FULL_MATRIX_EXCLUDED_FIELDS,
                vortex_compatible=True,
            )
        )
        write_vortex_table(minio_client, cfg["bucket"], f"{ext_key}/data.vortex", table)
        schema = build_full_matrix_schema(
            self,
            client,
            ext_url,
            ext_spec=build_external_spec(cfg, fmt="vortex"),
            excluded_fields=VORTEX_FULL_MATRIX_EXCLUDED_FIELDS,
        )
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        create_full_matrix_indexes(self, client, coll, excluded_fields=VORTEX_FULL_MATRIX_EXCLUDED_FIELDS)
        self.load_collection(client, coll)
        _full_matrix_assert_basic(
            self, client, coll, FULL_MATRIX_NB, excluded_fields=VORTEX_FULL_MATRIX_EXCLUDED_FIELDS
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_external_table_full_matrix_iceberg(self, minio_env, external_prefix):
        """
        target: test MilvusClient external table full matrix iceberg
        method: Iceberg × full DataType × full Index matrix
        expected: behavior matches the case assertion.
        """
        _minio_client, cfg = minio_env
        client = self._client()
        coll = cf.gen_collection_name_by_testcase_name()
        ext_key = external_prefix["key"]

        snapshot_id, ext_url = _build_iceberg_full_matrix_table(
            ext_key,
            cfg,
            num_rows=FULL_MATRIX_NB,
        )
        ext_spec = build_external_spec(
            cfg,
            fmt="iceberg-table",
            snapshot_id=int(snapshot_id),
        )
        schema = build_iceberg_full_matrix_schema(self, client, ext_url, ext_spec)
        self.create_collection(client, collection_name=coll, schema=schema)
        self.refresh_and_wait(client, coll)
        create_iceberg_full_matrix_indexes(self, client, coll)
        self.load_collection(client, coll)
        _iceberg_full_matrix_assert(self, client, coll, FULL_MATRIX_NB)
