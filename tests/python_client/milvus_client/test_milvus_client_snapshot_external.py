"""Milvus snapshot as a ``milvus-table`` external source E2E tests.

The cases in this module intentionally use only public MilvusClient APIs. They
validate the user-visible snapshot contract without inspecting snapshot files,
segment IDs, or StorageV3 manifests.
"""

import json
import math
import time
from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.external_table_common import build_external_spec, get_minio_config
from ml_dtypes import bfloat16
from pymilvus import DataType, Function, FunctionType, MilvusException

pytestmark = pytest.mark.xdist_group(name="milvus_table_external")

DIM = 16
CORE_ROWS = 3_000
CORE_DELETE_ROWS = 100
SMALL_ROWS = 200
FLUSH_GAP_SECONDS = 10
REFRESH_TIMEOUT = 300
PIN_TTL_SECONDS = 3_600
REFRESH_POLL_SECONDS = 2

CORE_FIELD_MAPPING = {
    "source_pk": "pk",
    "vector": "embedding",
    "group_alias": "group_id",
    "score_alias": "score",
    "tag_alias": "tag",
    "payload_alias": "payload",
    "numbers_alias": "numbers",
}


@dataclass
class SnapshotReference:
    """Public snapshot information needed by an external collection and cleanup."""

    source_collection: str
    snapshot_name: str
    external_source: str = ""
    pin_id: int | None = None


def _value(obj, name, default=None):
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _row_value(row, name, default=None):
    if isinstance(row, dict):
        if name in row:
            return row[name]
        for container_name in ("entity", "fields"):
            container = row.get(container_name)
            if isinstance(container, dict) and name in container:
                return container[name]
        return default
    return getattr(row, name, default)


def _row_has_field(row, name):
    if isinstance(row, dict):
        if name in row:
            return True
        return any(
            isinstance(row.get(container_name), dict) and name in row[container_name]
            for container_name in ("entity", "fields")
        )
    return hasattr(row, name)


def _hit_id(hit):
    if isinstance(hit, dict) and "id" in hit:
        return hit["id"]
    return getattr(hit, "id", None)


def _json_value(value):
    return json.loads(value) if isinstance(value, str) else value


def _timestamp_value(value):
    assert isinstance(value, str), f"expected TIMESTAMPTZ string, got {value!r}"
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _assert_search_hits(hits, limit, metric_type):
    assert len(hits) == limit, f"expected {limit} hits, got {len(hits)}"
    ids = [_hit_id(hit) for hit in hits]
    assert all(hit_id is not None for hit_id in ids), f"search returned a hit without an ID: {hits!r}"
    assert len(ids) == len(set(ids)), f"search returned duplicate IDs: {ids!r}"

    distances = []
    for hit in hits:
        distance = _row_value(hit, "distance")
        assert distance is not None, f"search returned a hit without distance: {hit!r}"
        distance = float(distance)
        assert math.isfinite(distance), f"search returned non-finite distance: {distance!r}"
        distances.append(distance)

    metric_type = metric_type.upper()
    assert metric_type in {"L2", "HAMMING", "IP", "COSINE", "BM25"}, f"unsupported metric ordering check: {metric_type}"
    if metric_type in {"IP", "COSINE", "BM25"}:
        assert distances == sorted(distances, reverse=True), f"{metric_type} distances are not descending: {distances}"
    else:
        assert distances == sorted(distances), f"{metric_type} distances are not ascending: {distances}"


def _snapshot_external_source(snapshot_location, minio_cfg):
    """Resolve the exact metadata JSON URI returned by describe_snapshot."""
    if not snapshot_location:
        raise AssertionError("describe_snapshot returned an empty s3_location")
    if "://" in snapshot_location:
        return snapshot_location
    relative_path = snapshot_location.lstrip("/")
    return f"minio://{minio_cfg['address']}/{minio_cfg['bucket']}/{relative_path}"


def _core_vector(row_id, marker=0):
    identity = row_id + marker * 10_000
    return [
        float(identity) / 1_000.0,
        *[float((identity + index * 17) % 257) / 257.0 for index in range(1, DIM)],
    ]


def _core_row(row_id, pk_type=DataType.INT64, marker=0):
    pk = row_id if pk_type == DataType.INT64 else f"key_{row_id:08d}"
    return {
        "pk": pk,
        "embedding": _core_vector(row_id, marker=marker),
        "group_id": row_id % 10,
        "score": float(row_id) / 10.0 if marker == 0 else -float(row_id + marker),
        "tag": f"tag_{row_id % 7}" if marker == 0 else f"replacement_{marker}",
        "payload": {
            "row": row_id,
            "group": row_id % 10,
            "marker": marker,
            "nested": {"even": row_id % 2 == 0},
        },
        "numbers": [row_id + marker, row_id + marker + 1, row_id + marker + 2],
    }


def _mixed_float_vector(row_id):
    return np.asarray(_core_vector(row_id), dtype=np.float32)


def _mixed_float16_vector(row_id):
    return np.asarray(_core_vector(row_id), dtype=np.float16)


def _mixed_bfloat16_vector(row_id):
    return np.asarray(_core_vector(row_id), dtype=np.float32).astype(bfloat16)


def _mixed_binary_vector(row_id):
    return int(row_id).to_bytes(DIM // 8, byteorder="little", signed=False)


def _mixed_row(row_id):
    day = row_id % 28 + 1
    return {
        "pk": row_id,
        "bool_field": row_id % 2 == 0,
        "int8_field": row_id % 100 - 50,
        "int32_field": row_id * 7,
        "float_field": float(row_id) / 10.0,
        "double_field": float(row_id) / 3.0,
        "varchar_field": f"value_{row_id:04d}",
        "timestamptz_field": f"2026-02-{day:02d}T12:34:56Z",
        "json_field": {"row": row_id, "even": row_id % 2 == 0},
        "geometry_field": f"POINT ({row_id % 360} {row_id % 180})",
        "array_int64": [row_id, row_id + 1, row_id + 2],
        "float_vector": _mixed_float_vector(row_id),
        "float16_vector": _mixed_float16_vector(row_id),
        "bfloat16_vector": _mixed_bfloat16_vector(row_id),
        "binary_vector": _mixed_binary_vector(row_id),
    }


class MilvusTableExternalTestBase(TestMilvusClientV2Base):
    """Shared public-API helpers for snapshot-backed external collections."""

    skip_global_role_cleanup = True

    def setup_method(self, method):
        super().setup_method(method)
        self.tear_down_collection_names = []
        self.resource_group_list = []
        self._snapshot_refs = []
        self._last_flush_at = None

    def teardown_method(self, method):
        cleanup_errors = []
        client = None
        try:
            client = self._client()
            for ref in reversed(self._snapshot_refs):
                if ref.pin_id is None:
                    continue
                try:
                    client.unpin_snapshot_data(pin_id=ref.pin_id, timeout=120)
                    ref.pin_id = None
                except Exception as exc:
                    cleanup_errors.append(f"unpin {ref.pin_id}: {exc!r}")

            for ref in reversed(self._snapshot_refs):
                try:
                    if client.has_collection(collection_name=ref.source_collection, timeout=120):
                        client.drop_snapshot(
                            snapshot_name=ref.snapshot_name,
                            collection_name=ref.source_collection,
                            timeout=120,
                        )
                except Exception as exc:
                    cleanup_errors.append(f"drop snapshot {ref.source_collection}/{ref.snapshot_name}: {exc!r}")
        except Exception as exc:
            cleanup_errors.append(f"initialize cleanup client: {exc!r}")
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception as exc:
                    cleanup_errors.append(f"close cleanup client: {exc!r}")
            super().teardown_method(method)

        assert not cleanup_errors, "milvus-table cleanup failed: " + "; ".join(cleanup_errors)

    def _name(self, suffix):
        base = cf.gen_collection_name_by_testcase_name()
        return f"{base[:220]}_{suffix}"

    def _minio_cfg(self, request):
        return get_minio_config(
            minio_host=request.config.getoption("--minio_host"),
            minio_bucket=request.config.getoption("--minio_bucket"),
        )

    def _external_spec(self, minio_cfg):
        return build_external_spec(minio_cfg, fmt="milvus-table")

    def _flush_rate_limited(self, client, collection_name):
        if self._last_flush_at is not None:
            remaining = FLUSH_GAP_SECONDS - (time.monotonic() - self._last_flush_at)
            if remaining > 0:
                time.sleep(remaining)
        self.flush(client, collection_name)
        self._last_flush_at = time.monotonic()

    def _create_snapshot_ref(self, client, collection_name, minio_cfg, pin=False, suffix="snap"):
        snapshot_name = cf.gen_unique_str(f"milvus_table_{suffix}")
        ref = SnapshotReference(collection_name, snapshot_name)
        self._snapshot_refs.append(ref)
        self.create_snapshot(
            client,
            snapshot_name,
            collection_name,
            description="PyMilvus milvus-table external source E2E",
        )
        info = self.describe_snapshot(client, snapshot_name, collection_name)[0]
        ref.external_source = _snapshot_external_source(_value(info, "s3_location", ""), minio_cfg)
        if pin:
            ref.pin_id = self.pin_snapshot_data(
                client,
                snapshot_name,
                collection_name,
                ttl_seconds=PIN_TTL_SECONDS,
            )[0]
            assert isinstance(ref.pin_id, int) and ref.pin_id > 0, f"invalid snapshot pin id: {ref.pin_id!r}"
        return ref

    def _build_core_source_schema(self, client, pk_type=DataType.INT64, dim=DIM):
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        pk_kwargs = {"is_primary": True}
        if pk_type == DataType.VARCHAR:
            pk_kwargs["max_length"] = 64
        self.add_field(schema, "pk", pk_type, **pk_kwargs)
        self.add_field(schema, "embedding", DataType.FLOAT_VECTOR, dim=dim)
        self.add_field(schema, "group_id", DataType.INT64)
        self.add_field(schema, "score", DataType.FLOAT)
        self.add_field(schema, "tag", DataType.VARCHAR, max_length=128)
        self.add_field(schema, "payload", DataType.JSON)
        self.add_field(
            schema,
            "numbers",
            DataType.ARRAY,
            element_type=DataType.INT64,
            max_capacity=8,
        )
        return schema

    def _build_core_target_schema(
        self,
        client,
        snapshot_ref,
        minio_cfg,
        *,
        real_pk,
        pk_type=DataType.INT64,
        vector_dim=DIM,
        omit_external_field=None,
        pk_external_field="pk",
        group_external_field="group_id",
    ):
        schema = self.create_schema(
            client,
            auto_id=False,
            enable_dynamic_field=False,
            external_source=snapshot_ref.external_source,
            external_spec=self._external_spec(minio_cfg),
        )[0]

        pk_kwargs = {"external_field": pk_external_field}
        if real_pk:
            pk_kwargs["is_primary"] = True
        if pk_type == DataType.VARCHAR:
            pk_kwargs["max_length"] = 64
        if omit_external_field == "source_pk":
            pk_kwargs.pop("external_field")
        self.add_field(schema, "source_pk", pk_type, **pk_kwargs)

        field_specs = [
            ("vector", DataType.FLOAT_VECTOR, "embedding", {"dim": vector_dim}),
            ("group_alias", DataType.INT64, group_external_field, {}),
            ("score_alias", DataType.FLOAT, "score", {}),
            ("tag_alias", DataType.VARCHAR, "tag", {"max_length": 128}),
            ("payload_alias", DataType.JSON, "payload", {}),
            (
                "numbers_alias",
                DataType.ARRAY,
                "numbers",
                {"element_type": DataType.INT64, "max_capacity": 8},
            ),
        ]
        for target_name, data_type, source_name, kwargs in field_specs:
            if omit_external_field != target_name:
                kwargs["external_field"] = source_name
            self.add_field(schema, target_name, data_type, **kwargs)
        return schema

    def _create_core_source(self, client, collection_name, rows, pk_type=DataType.INT64, dim=DIM):
        schema = self._build_core_source_schema(client, pk_type=pk_type, dim=dim)
        self.create_collection(client, collection_name=collection_name, schema=schema)
        if rows:
            self.insert(client, collection_name, rows)
        return schema

    def _wait_refresh(self, client, job_id, expect_failure=False, reason_terms=()):
        deadline = time.monotonic() + REFRESH_TIMEOUT
        last = None
        while time.monotonic() < deadline:
            last = self.get_refresh_external_collection_progress(client, job_id=job_id)[0]
            state = _value(last, "state", "")
            if state in ("RefreshCompleted", "RefreshFailed"):
                if expect_failure:
                    assert state == "RefreshFailed", f"refresh {job_id} unexpectedly completed"
                    reason = _value(last, "reason", "") or ""
                    assert reason, f"refresh {job_id} failed without a reason"
                    if reason_terms:
                        lowered = reason.lower()
                        assert any(term.lower() in lowered for term in reason_terms), (
                            f"refresh {job_id} reason {reason!r} did not contain any of {reason_terms}"
                        )
                else:
                    assert state == "RefreshCompleted", f"refresh {job_id} failed: {_value(last, 'reason', '')}"
                return last
            time.sleep(REFRESH_POLL_SECONDS)
        raise AssertionError(f"refresh {job_id} did not finish in {REFRESH_TIMEOUT}s; last={last!r}")

    def _refresh(self, client, collection_name, *, snapshot_ref=None, minio_cfg=None):
        kwargs = {}
        if snapshot_ref is not None:
            kwargs["external_source"] = snapshot_ref.external_source
            kwargs["external_spec"] = self._external_spec(minio_cfg)
        job_id = self.refresh_external_collection(client, collection_name, **kwargs)[0]
        self._wait_refresh(client, job_id)
        return job_id

    def _refresh_expect_failure(self, client, collection_name, snapshot_ref, minio_cfg, reason_terms):
        job_id = self.refresh_external_collection(
            client,
            collection_name,
            external_source=snapshot_ref.external_source,
            external_spec=self._external_spec(minio_cfg),
        )[0]
        return self._wait_refresh(client, job_id, expect_failure=True, reason_terms=reason_terms)

    def _index_and_load(self, client, collection_name, indexes=None):
        # These snapshot-contract tests assert exact hit IDs and distances.
        # Approximate index coverage must opt in with recall-tolerant assertions.
        indexes = indexes or [("vector", "FLAT", "L2", {})]
        index_params = self.prepare_index_params(client)[0]
        for field_name, index_type, metric_type, params in indexes:
            index_params.add_index(
                field_name=field_name,
                index_type=index_type,
                metric_type=metric_type,
                params=params,
            )
        self.create_index(client, collection_name, index_params)
        self.load_collection(client, collection_name)

    def _count(self, client, collection_name):
        rows = self.query(
            client,
            collection_name,
            filter="",
            output_fields=["count(*)"],
            consistency_level="Strong",
        )[0]
        return int(rows[0]["count(*)"])

    def _assert_collection_absent(self, client, collection_name):
        if collection_name not in self.tear_down_collection_names:
            self.tear_down_collection_names.append(collection_name)
        assert self.has_collection(client, collection_name)[0] is False, (
            f"collection {collection_name!r} exists after create returned an error"
        )

    def _query_source_pks(self, client, collection_name, pks, pk_type=DataType.INT64, output_fields=None):
        encoded = ",".join(json.dumps(pk) if pk_type == DataType.VARCHAR else str(pk) for pk in pks)
        return self.query(
            client,
            collection_name,
            filter=f"source_pk in [{encoded}]",
            output_fields=output_fields or list(CORE_FIELD_MAPPING),
            consistency_level="Strong",
            limit=max(len(pks), 1) + 10,
        )[0]

    def _assert_core_row(self, actual, expected):
        assert _row_value(actual, "source_pk") == expected["pk"]
        assert _row_value(actual, "group_alias") == expected["group_id"]
        assert math.isclose(float(_row_value(actual, "score_alias")), expected["score"], abs_tol=1e-5)
        assert _row_value(actual, "tag_alias") == expected["tag"]
        assert _json_value(_row_value(actual, "payload_alias")) == expected["payload"]
        assert list(_row_value(actual, "numbers_alias")) == expected["numbers"]

    def _search_core(self, client, collection_name, vector, limit=5, filter=None):
        results = self.search(
            client,
            collection_name,
            data=[vector],
            anns_field="vector",
            limit=limit,
            filter=filter,
            output_fields=["source_pk", "group_alias", "tag_alias", "payload_alias"],
            search_params={"metric_type": "L2", "params": {}},
        )[0]
        assert len(results) == 1
        hits = results[0]
        _assert_search_hits(hits, limit, "L2")
        for hit in hits:
            for field_name in ("source_pk", "group_alias", "tag_alias", "payload_alias"):
                assert _row_value(hit, field_name) is not None, (
                    f"search hit is missing output field {field_name!r}: {hit!r}"
                )
        return hits

    def _assert_schema_alignment(self, source_info, target_info, mapping, target_owned=()):
        source_fields = {field["name"]: field for field in source_info["fields"]}
        target_fields = {field["name"]: field for field in target_info["fields"]}
        source_ids = {int(field["field_id"]) for field in source_info["fields"]}

        for target_name, source_name in mapping.items():
            target_field = target_fields[target_name]
            source_field = source_fields[source_name]
            assert target_field.get("external_field") == source_name
            assert int(target_field["field_id"]) == int(source_field["field_id"])

        for target_name in target_owned:
            assert int(target_fields[target_name]["field_id"]) not in source_ids

    def _core_signature(self, client, collection_name, sample_ids):
        rows = self._query_source_pks(
            client,
            collection_name,
            sample_ids,
            output_fields=[
                "source_pk",
                "group_alias",
                "score_alias",
                "tag_alias",
                "payload_alias",
            ],
        )
        normalized_rows = sorted(
            (
                _row_value(row, "source_pk"),
                _row_value(row, "group_alias"),
                round(float(_row_value(row, "score_alias")), 5),
                _row_value(row, "tag_alias"),
                _json_value(_row_value(row, "payload_alias")),
            )
            for row in rows
        )
        hits = self._search_core(client, collection_name, _core_vector(sample_ids[0]), limit=3)
        normalized_hits = [(_hit_id(hit), _row_value(hit, "source_pk")) for hit in hits]
        return normalized_rows, normalized_hits


class TestMilvusClientMilvusTableExternal(MilvusTableExternalTestBase):
    """Milvus snapshot external-source behavior exposed through MilvusClient."""

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_table_snapshot_real_pk_after_source_drop(self, request):
        """
        target: use a pinned Milvus snapshot after the source collection is dropped
        method: flush inserts/deletes, pin snapshot, drop source, refresh a renamed real-PK target
        expected: surviving rows, values, real IDs, search, and aligned field IDs remain correct
        """
        client = self._client()
        cfg = self._minio_cfg(request)
        source = self._name("real_src")
        target = self._name("real_ext")
        rows = [_core_row(row_id) for row_id in range(CORE_ROWS)]
        self._create_core_source(client, source, rows)
        self._flush_rate_limited(client, source)
        self.delete(client, source, ids=list(range(CORE_DELETE_ROWS)))
        self._flush_rate_limited(client, source)
        source_info = self.describe_collection(client, source)[0]
        snapshot = self._create_snapshot_ref(client, source, cfg, pin=True, suffix="real")

        self.drop_collection(client, source)
        schema = self._build_core_target_schema(client, snapshot, cfg, real_pk=True)
        self.create_collection(client, collection_name=target, schema=schema)
        self._refresh(client, target)
        self._index_and_load(client, target)

        assert self._count(client, target) == CORE_ROWS - CORE_DELETE_ROWS
        assert self._query_source_pks(client, target, [0, CORE_DELETE_ROWS - 1]) == []
        survivor = CORE_DELETE_ROWS + 23
        queried = self._query_source_pks(client, target, [survivor])
        assert len(queried) == 1
        self._assert_core_row(queried[0], _core_row(survivor))
        fetched = self.get(
            client,
            target,
            ids=[survivor],
            output_fields=[
                "source_pk",
                "group_alias",
                "score_alias",
                "tag_alias",
                "payload_alias",
                "numbers_alias",
            ],
        )[0]
        assert len(fetched) == 1
        self._assert_core_row(fetched[0], _core_row(survivor))

        search_id = CORE_DELETE_ROWS + 221
        hits = self._search_core(client, target, _core_vector(search_id), limit=3)
        assert _hit_id(hits[0]) == search_id
        assert _row_value(hits[0], "source_pk") == search_id
        assert float(_row_value(hits[0], "distance")) == pytest.approx(0.0, abs=1e-5)

        left_id = CORE_DELETE_ROWS + 321
        right_id = left_id + 1
        non_self_vector = [(left + right) / 2.0 for left, right in zip(_core_vector(left_id), _core_vector(right_id))]
        non_self_hits = self._search_core(client, target, non_self_vector, limit=5, filter="group_alias in [1, 2]")
        assert {_row_value(hit, "source_pk") for hit in non_self_hits[:2]} == {
            left_id,
            right_id,
        }
        assert all(_row_value(hit, "group_alias") in (1, 2) for hit in non_self_hits)
        assert all(float(_row_value(hit, "distance")) > 0 for hit in non_self_hits)

        target_info = self.describe_collection(client, target)[0]
        assert target_info["external_source"] == snapshot.external_source
        self._assert_schema_alignment(source_info, target_info, CORE_FIELD_MAPPING)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_table_snapshot_virtual_pk_user_reads(self, request):
        """
        target: expose a Milvus snapshot through a virtual-primary-key target
        method: refresh renamed fields without declaring a user primary key, then search and get
        expected: source PK is readable data while search/get use the synthesized virtual PK
        """
        client = self._client()
        cfg = self._minio_cfg(request)
        source = self._name("virtual_src")
        target = self._name("virtual_ext")
        rows = [_core_row(row_id) for row_id in range(CORE_ROWS)]
        self._create_core_source(client, source, rows)
        self._flush_rate_limited(client, source)
        self.delete(client, source, ids=list(range(CORE_DELETE_ROWS)))
        self._flush_rate_limited(client, source)
        source_info = self.describe_collection(client, source)[0]
        snapshot = self._create_snapshot_ref(client, source, cfg, suffix="virtual")

        schema = self._build_core_target_schema(client, snapshot, cfg, real_pk=False)
        self.create_collection(client, collection_name=target, schema=schema)
        self._refresh(client, target)
        self._index_and_load(client, target)

        assert self._count(client, target) == CORE_ROWS - CORE_DELETE_ROWS
        assert self._query_source_pks(client, target, [0, CORE_DELETE_ROWS - 1]) == []

        source_pk = CORE_DELETE_ROWS + 321
        hits = self._search_core(client, target, _core_vector(source_pk), limit=1)
        virtual_pk = _hit_id(hits[0])
        assert _row_value(hits[0], "source_pk") == source_pk
        assert virtual_pk != source_pk
        fetched = self.get(
            client,
            target,
            ids=[virtual_pk],
            output_fields=[
                "source_pk",
                "group_alias",
                "score_alias",
                "tag_alias",
                "payload_alias",
                "numbers_alias",
            ],
        )[0]
        assert len(fetched) == 1
        self._assert_core_row(fetched[0], _core_row(source_pk))

        target_info = self.describe_collection(client, target)[0]
        fields = {field["name"]: field for field in target_info["fields"]}
        assert fields["__virtual_pk__"]["is_primary"] is True
        assert fields["__virtual_pk__"]["auto_id"] is True
        assert fields["source_pk"].get("is_primary", False) is False
        self._assert_schema_alignment(
            source_info,
            target_info,
            CORE_FIELD_MAPPING,
            target_owned=("__virtual_pk__",),
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_table_snapshot_excludes_unflushed_dml(self, request):
        """
        target: keep the snapshot boundary at flushed source data
        method: flush a baseline, then delete, update-upsert, and insert-upsert without flushing
        expected: target contains original baseline rows and excludes all unflushed changes
        """
        client = self._client()
        cfg = self._minio_cfg(request)
        source = self._name("unflushed_src")
        target = self._name("unflushed_ext")
        rows = [_core_row(row_id) for row_id in range(SMALL_ROWS)]
        self._create_core_source(client, source, rows)
        self._flush_rate_limited(client, source)
        self._index_and_load(client, source, indexes=[("embedding", "FLAT", "L2", {})])

        self.delete(client, source, ids=[0])
        self.upsert(client, source, [_core_row(1, marker=9), _core_row(SMALL_ROWS, marker=9)])

        source_rows = self.query(
            client,
            source,
            filter=f"pk in [0, 1, {SMALL_ROWS}]",
            output_fields=["pk", "group_id", "score", "tag", "payload", "numbers"],
            consistency_level="Strong",
            limit=10,
        )[0]
        rows_by_pk = {_row_value(row, "pk"): row for row in source_rows}
        assert set(rows_by_pk) == {1, SMALL_ROWS}
        for row_id in (1, SMALL_ROWS):
            expected = _core_row(row_id, marker=9)
            actual = rows_by_pk[row_id]
            assert _row_value(actual, "group_id") == expected["group_id"]
            assert float(_row_value(actual, "score")) == pytest.approx(expected["score"], abs=1e-5)
            assert _row_value(actual, "tag") == expected["tag"]
            assert _json_value(_row_value(actual, "payload")) == expected["payload"]
            assert list(_row_value(actual, "numbers")) == expected["numbers"]

        snapshot = self._create_snapshot_ref(client, source, cfg, suffix="unflushed")

        schema = self._build_core_target_schema(client, snapshot, cfg, real_pk=True)
        self.create_collection(client, collection_name=target, schema=schema)
        self._refresh(client, target)
        self._index_and_load(client, target)

        assert self._count(client, target) == SMALL_ROWS
        row_zero = self._query_source_pks(client, target, [0])
        row_one = self._query_source_pks(client, target, [1])
        assert len(row_zero) == len(row_one) == 1
        self._assert_core_row(row_zero[0], _core_row(0))
        self._assert_core_row(row_one[0], _core_row(1))
        assert self._query_source_pks(client, target, [SMALL_ROWS]) == []

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("real_pk", [True, False], ids=["real_pk", "virtual_pk"])
    def test_milvus_table_delete_before_reinsert_visibility(self, request, real_pk):
        """
        target: preserve delete-before-reinsert timestamp ordering
        method: flush a row, flush its delete, then upsert and flush a replacement with the same PK
        expected: both real- and virtual-PK targets expose exactly the newer replacement row
        """
        client = self._client()
        cfg = self._minio_cfg(request)
        source = self._name("reinsert_src")
        target = self._name("reinsert_ext")
        row_id = 7
        replacement = _core_row(row_id, marker=5)
        self._create_core_source(client, source, [_core_row(row_id)])
        self._flush_rate_limited(client, source)
        self.delete(client, source, ids=[row_id])
        self._flush_rate_limited(client, source)
        self.upsert(client, source, [replacement])
        self._flush_rate_limited(client, source)
        snapshot = self._create_snapshot_ref(client, source, cfg, suffix="reinsert")

        schema = self._build_core_target_schema(client, snapshot, cfg, real_pk=real_pk)
        self.create_collection(client, collection_name=target, schema=schema)
        self._refresh(client, target)
        self._index_and_load(client, target)

        assert self._count(client, target) == 1
        queried = self._query_source_pks(client, target, [row_id])
        assert len(queried) == 1
        self._assert_core_row(queried[0], replacement)
        hits = self._search_core(client, target, replacement["embedding"], limit=1)
        assert _row_value(hits[0], "source_pk") == row_id
        if real_pk:
            assert _hit_id(hits[0]) == row_id
        else:
            assert _hit_id(hits[0]) != row_id

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("real_pk", [True, False], ids=["real_pk", "virtual_pk"])
    def test_milvus_table_refresh_applies_snapshot_deletes(self, request, real_pk):
        """
        target: apply a later snapshot's deletes through refresh
        method: load snapshot A, flush deletes into snapshot B, then refresh B twice
        expected: deletes, source URI, search/query results, and counts are correct and idempotent
        """
        client = self._client()
        cfg = self._minio_cfg(request)
        source = self._name("delete_refresh_src")
        target = self._name("delete_refresh_ext")
        split = CORE_ROWS // 2
        self._create_core_source(client, source, [])
        self.insert(client, source, [_core_row(row_id) for row_id in range(split)])
        self._flush_rate_limited(client, source)
        self.insert(
            client,
            source,
            [_core_row(row_id) for row_id in range(split, CORE_ROWS)],
        )
        self._flush_rate_limited(client, source)
        snapshot_a = self._create_snapshot_ref(client, source, cfg, suffix="delete_a")

        schema = self._build_core_target_schema(client, snapshot_a, cfg, real_pk=real_pk)
        self.create_collection(client, collection_name=target, schema=schema)
        self._refresh(client, target)
        self._index_and_load(client, target)
        assert self._count(client, target) == CORE_ROWS

        self.delete(client, source, ids=list(range(CORE_DELETE_ROWS)))
        self._flush_rate_limited(client, source)
        snapshot_b = self._create_snapshot_ref(client, source, cfg, suffix="delete_b")

        self.release_collection(client, target)
        self._refresh(client, target, snapshot_ref=snapshot_b, minio_cfg=cfg)
        self.load_collection(client, target)
        assert self._count(client, target) == CORE_ROWS - CORE_DELETE_ROWS
        assert self._query_source_pks(client, target, [0, CORE_DELETE_ROWS - 1]) == []
        first_signature = self._core_signature(client, target, [CORE_DELETE_ROWS, 500, CORE_ROWS - 1])
        assert self.describe_collection(client, target)[0]["external_source"] == snapshot_b.external_source

        self.release_collection(client, target)
        self._refresh(client, target, snapshot_ref=snapshot_b, minio_cfg=cfg)
        self.load_collection(client, target)
        assert self._count(client, target) == CORE_ROWS - CORE_DELETE_ROWS
        assert self._core_signature(client, target, [CORE_DELETE_ROWS, 500, CORE_ROWS - 1]) == first_signature

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_table_refresh_adds_rows_and_is_idempotent(self, request):
        """
        target: add newly flushed source rows through a later snapshot
        method: refresh snapshot A, append rows, refresh snapshot B, then repeat B
        expected: new rows appear once and repeated refresh leaves data unchanged
        """
        client = self._client()
        cfg = self._minio_cfg(request)
        source = self._name("append_refresh_src")
        target = self._name("append_refresh_ext")
        initial_rows = [_core_row(row_id) for row_id in range(SMALL_ROWS)]
        self._create_core_source(client, source, initial_rows)
        self._flush_rate_limited(client, source)
        snapshot_a = self._create_snapshot_ref(client, source, cfg, suffix="append_a")

        schema = self._build_core_target_schema(client, snapshot_a, cfg, real_pk=True)
        self.create_collection(client, collection_name=target, schema=schema)
        self._refresh(client, target)
        self._index_and_load(client, target)
        assert self._count(client, target) == SMALL_ROWS

        added_ids = list(range(SMALL_ROWS, SMALL_ROWS + 50))
        self.insert(client, source, [_core_row(row_id) for row_id in added_ids])
        self._flush_rate_limited(client, source)
        snapshot_b = self._create_snapshot_ref(client, source, cfg, suffix="append_b")

        self.release_collection(client, target)
        self._refresh(client, target, snapshot_ref=snapshot_b, minio_cfg=cfg)
        self.load_collection(client, target)
        assert self._count(client, target) == SMALL_ROWS + len(added_ids)
        added_rows = self._query_source_pks(client, target, [added_ids[0], added_ids[-1]])
        assert {_row_value(row, "source_pk") for row in added_rows} == {
            added_ids[0],
            added_ids[-1],
        }
        first_signature = self._core_signature(client, target, [0, SMALL_ROWS, added_ids[-1]])

        self.release_collection(client, target)
        self._refresh(client, target, snapshot_ref=snapshot_b, minio_cfg=cfg)
        self.load_collection(client, target)
        assert self._count(client, target) == SMALL_ROWS + len(added_ids)
        assert self._core_signature(client, target, [0, SMALL_ROWS, added_ids[-1]]) == first_signature

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_table_varchar_real_pk(self, request):
        """
        target: use a renamed VARCHAR source primary key as the target primary key
        method: flush VARCHAR rows/deletes, refresh, query survivors, and run filtered search
        expected: deleted keys are absent and search result IDs are real VARCHAR primary keys
        """
        client = self._client()
        cfg = self._minio_cfg(request)
        source = self._name("varchar_src")
        target = self._name("varchar_ext")
        rows = [_core_row(row_id, pk_type=DataType.VARCHAR) for row_id in range(SMALL_ROWS)]
        self._create_core_source(client, source, rows, pk_type=DataType.VARCHAR)
        self._flush_rate_limited(client, source)
        deleted = [f"key_{row_id:08d}" for row_id in range(20)]
        self.delete(client, source, ids=deleted)
        self._flush_rate_limited(client, source)
        source_info = self.describe_collection(client, source)[0]
        snapshot = self._create_snapshot_ref(client, source, cfg, suffix="varchar")

        schema = self._build_core_target_schema(
            client,
            snapshot,
            cfg,
            real_pk=True,
            pk_type=DataType.VARCHAR,
        )
        self.create_collection(client, collection_name=target, schema=schema)
        self._refresh(client, target)
        self._index_and_load(client, target)

        assert self._count(client, target) == SMALL_ROWS - len(deleted)
        assert self._query_source_pks(client, target, deleted, pk_type=DataType.VARCHAR) == []
        survivor_id = 23
        survivor_key = f"key_{survivor_id:08d}"
        rows = self._query_source_pks(
            client,
            target,
            [survivor_key],
            pk_type=DataType.VARCHAR,
        )
        assert len(rows) == 1
        self._assert_core_row(rows[0], _core_row(survivor_id, pk_type=DataType.VARCHAR))

        hits = self._search_core(
            client,
            target,
            _core_vector(survivor_id),
            limit=5,
            filter="group_alias == 3",
        )
        assert _hit_id(hits[0]) == survivor_key
        assert all(isinstance(_hit_id(hit), str) for hit in hits)
        assert all(_row_value(hit, "group_alias") == 3 for hit in hits)
        self._assert_schema_alignment(
            source_info,
            self.describe_collection(client, target)[0],
            CORE_FIELD_MAPPING,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_table_failed_schema_refresh_preserves_old_snapshot(self, request):
        """
        target: preserve the current external source and data after a bad refresh
        method: load snapshot A, then refresh an incompatible-dimension snapshot B
        expected: refresh reaches RefreshFailed and the collection remains readable from A
        """
        client = self._client()
        cfg = self._minio_cfg(request)
        source_a = self._name("schema_a_src")
        source_b = self._name("schema_b_src")
        target = self._name("schema_ext")
        self._create_core_source(client, source_a, [_core_row(i) for i in range(SMALL_ROWS)])
        self._flush_rate_limited(client, source_a)
        snapshot_a = self._create_snapshot_ref(client, source_a, cfg, suffix="schema_a")

        dim_b = DIM // 2
        rows_b = []
        for row_id in range(50):
            row = _core_row(row_id)
            row["embedding"] = row["embedding"][:dim_b]
            rows_b.append(row)
        self._create_core_source(client, source_b, rows_b, dim=dim_b)
        self._flush_rate_limited(client, source_b)
        snapshot_b = self._create_snapshot_ref(client, source_b, cfg, suffix="schema_b")

        schema = self._build_core_target_schema(client, snapshot_a, cfg, real_pk=True)
        self.create_collection(client, collection_name=target, schema=schema)
        self._refresh(client, target)
        self._index_and_load(client, target)
        assert self._count(client, target) == SMALL_ROWS

        self.release_collection(client, target)
        self._refresh_expect_failure(
            client,
            target,
            snapshot_b,
            cfg,
            reason_terms=("schema", "definition mismatch", "dimension"),
        )
        target_info = self.describe_collection(client, target)[0]
        assert target_info["external_source"] == snapshot_a.external_source
        self.load_collection(client, target)
        assert self._count(client, target) == SMALL_ROWS
        rows = self._query_source_pks(client, target, [42])
        assert len(rows) == 1
        self._assert_core_row(rows[0], _core_row(42))

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "case_name,expected_message",
        [
            pytest.param(
                "vector_dimension_mismatch",
                "definition mismatch",
                id="dimension_mismatch",
            ),
            pytest.param("missing_external_field", "external_field", id="missing_external_field"),
            pytest.param(
                "primary_key_maps_non_primary",
                "definition mismatch",
                id="pk_maps_non_pk",
            ),
        ],
    )
    def test_milvus_table_create_rejects_invalid_mapping(self, request, case_name, expected_message):
        """
        target: reject invalid target-to-snapshot field mappings at collection creation
        method: use a dimension mismatch, missing mapping, or swapped PK/non-PK mapping
        expected: create fails atomically with an input-oriented schema message
        """
        client = self._client()
        cfg = self._minio_cfg(request)
        source = self._name("invalid_src")
        target = self._name("invalid_ext")
        self._create_core_source(client, source, [])
        snapshot = self._create_snapshot_ref(client, source, cfg, suffix="invalid")

        kwargs = {"real_pk": True}
        if case_name == "vector_dimension_mismatch":
            kwargs["vector_dim"] = DIM // 2
        elif case_name == "missing_external_field":
            kwargs["omit_external_field"] = "tag_alias"
        else:
            kwargs["pk_external_field"] = "group_id"
            kwargs["group_external_field"] = "pk"
        schema = self._build_core_target_schema(client, snapshot, cfg, **kwargs)
        if target not in self.tear_down_collection_names:
            self.tear_down_collection_names.append(target)
        with pytest.raises(MilvusException) as exc_info:
            client.create_collection(collection_name=target, schema=schema)
        assert exc_info.value.code == 1100
        assert expected_message in str(exc_info.value)
        self._assert_collection_absent(client, target)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_table_rejects_non_json_source(self, request):
        """
        target: require a concrete snapshot metadata JSON path at create and refresh time
        method: create from a prefix, then override a valid target with the same invalid prefix
        expected: create fails atomically and failed refresh preserves the old source and data
        """
        client = self._client()
        cfg = self._minio_cfg(request)
        source = self._name("prefix_src")
        invalid_target = self._name("prefix_invalid_ext")
        valid_target = self._name("prefix_valid_ext")
        self._create_core_source(client, source, [_core_row(row_id) for row_id in range(SMALL_ROWS)])
        self._flush_rate_limited(client, source)
        snapshot = self._create_snapshot_ref(client, source, cfg, suffix="prefix")
        prefix_ref = SnapshotReference(
            source_collection=source,
            snapshot_name=snapshot.snapshot_name,
            external_source=snapshot.external_source.rsplit("/", 1)[0],
        )
        schema = self._build_core_target_schema(client, prefix_ref, cfg, real_pk=True)
        self.create_collection(
            client,
            collection_name=invalid_target,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "snapshot metadata JSON path"},
        )
        self._assert_collection_absent(client, invalid_target)

        valid_schema = self._build_core_target_schema(client, snapshot, cfg, real_pk=True)
        self.create_collection(client, collection_name=valid_target, schema=valid_schema)
        self._refresh(client, valid_target)
        self._index_and_load(client, valid_target)
        assert self._count(client, valid_target) == SMALL_ROWS

        self.release_collection(client, valid_target)
        self._refresh_expect_failure(
            client,
            valid_target,
            prefix_ref,
            cfg,
            reason_terms=("snapshot metadata json path", "json path"),
        )
        assert self.describe_collection(client, valid_target)[0]["external_source"] == snapshot.external_source
        self.load_collection(client, valid_target)
        assert self._count(client, valid_target) == SMALL_ROWS

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_table_rejects_external_snapshot_chaining(self, request):
        """
        target: reject external snapshot chaining at create and refresh time
        method: snapshot an external target, refresh the target from it, then create a second layer
        expected: both operations reject chaining and the original target remains readable
        """
        client = self._client()
        cfg = self._minio_cfg(request)
        source = self._name("chain_src")
        external_a = self._name("chain_ext_a")
        external_b = self._name("chain_ext_b")
        self._create_core_source(client, source, [_core_row(i) for i in range(50)])
        self._flush_rate_limited(client, source)
        source_snapshot = self._create_snapshot_ref(client, source, cfg, suffix="chain_source")

        schema_a = self._build_core_target_schema(client, source_snapshot, cfg, real_pk=True)
        self.create_collection(client, collection_name=external_a, schema=schema_a)
        self._refresh(client, external_a)
        self._index_and_load(client, external_a)
        assert self._count(client, external_a) == 50
        external_snapshot = self._create_snapshot_ref(client, external_a, cfg, suffix="chain_external")

        self.release_collection(client, external_a)
        self._refresh_expect_failure(
            client,
            external_a,
            external_snapshot,
            cfg,
            reason_terms=("external collection", "source snapshot"),
        )
        assert self.describe_collection(client, external_a)[0]["external_source"] == source_snapshot.external_source
        self.load_collection(client, external_a)
        assert self._count(client, external_a) == 50

        schema_b = self._build_core_target_schema(client, external_snapshot, cfg, real_pk=True)
        self.create_collection(
            client,
            collection_name=external_b,
            schema=schema_b,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "cannot use an external collection snapshot as source"},
        )
        self._assert_collection_absent(client, external_b)

    @pytest.mark.tags(CaseLabel.L2)
    def test_milvus_table_mixed_types_round_trip(self, request):
        """
        target: round-trip nullable scalar, complex, and vector types
        method: map renamed fields, query null/non-null rows, run typed filters, then search each vector
        expected: values, nulls, filters, and Float/Float16/BFloat16/Binary searches remain correct
        """
        client = self._client()
        cfg = self._minio_cfg(request)
        source = self._name("mixed_src")
        target = self._name("mixed_ext")

        source_schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        self.add_field(source_schema, "pk", DataType.INT64, is_primary=True)
        self.add_field(source_schema, "bool_field", DataType.BOOL, nullable=True)
        self.add_field(source_schema, "int8_field", DataType.INT8, nullable=True)
        self.add_field(source_schema, "int32_field", DataType.INT32, nullable=True)
        self.add_field(source_schema, "float_field", DataType.FLOAT, nullable=True)
        self.add_field(source_schema, "double_field", DataType.DOUBLE, nullable=True)
        self.add_field(
            source_schema,
            "varchar_field",
            DataType.VARCHAR,
            max_length=128,
            nullable=True,
        )
        self.add_field(source_schema, "timestamptz_field", DataType.TIMESTAMPTZ, nullable=True)
        self.add_field(source_schema, "json_field", DataType.JSON, nullable=True)
        self.add_field(source_schema, "geometry_field", DataType.GEOMETRY, nullable=True)
        self.add_field(
            source_schema,
            "array_int64",
            DataType.ARRAY,
            element_type=DataType.INT64,
            max_capacity=8,
            nullable=True,
        )
        self.add_field(source_schema, "float_vector", DataType.FLOAT_VECTOR, dim=DIM)
        self.add_field(source_schema, "float16_vector", DataType.FLOAT16_VECTOR, dim=DIM)
        self.add_field(source_schema, "bfloat16_vector", DataType.BFLOAT16_VECTOR, dim=DIM)
        self.add_field(source_schema, "binary_vector", DataType.BINARY_VECTOR, dim=DIM)
        self.create_collection(client, collection_name=source, schema=source_schema)
        null_row_id = 17
        nullable_source_fields = (
            "bool_field",
            "int8_field",
            "int32_field",
            "float_field",
            "double_field",
            "varchar_field",
            "timestamptz_field",
            "json_field",
            "geometry_field",
            "array_int64",
        )
        mixed_rows = [_mixed_row(i) for i in range(SMALL_ROWS)]
        for field_name in nullable_source_fields:
            mixed_rows[null_row_id][field_name] = None
        self.insert(client, source, mixed_rows)
        self._flush_rate_limited(client, source)
        source_info = self.describe_collection(client, source)[0]
        snapshot = self._create_snapshot_ref(client, source, cfg, suffix="mixed")

        target_schema = self.create_schema(
            client,
            auto_id=False,
            enable_dynamic_field=False,
            external_source=snapshot.external_source,
            external_spec=self._external_spec(cfg),
        )[0]
        mixed_mapping = {
            "source_pk": "pk",
            "bool_alias": "bool_field",
            "int8_alias": "int8_field",
            "int32_alias": "int32_field",
            "float_alias": "float_field",
            "double_alias": "double_field",
            "varchar_alias": "varchar_field",
            "timestamptz_alias": "timestamptz_field",
            "json_alias": "json_field",
            "geometry_alias": "geometry_field",
            "array_alias": "array_int64",
            "float_vector_alias": "float_vector",
            "float16_vector_alias": "float16_vector",
            "bfloat16_vector_alias": "bfloat16_vector",
            "binary_vector_alias": "binary_vector",
        }
        self.add_field(
            target_schema,
            "source_pk",
            DataType.INT64,
            is_primary=True,
            external_field="pk",
        )
        self.add_field(
            target_schema,
            "bool_alias",
            DataType.BOOL,
            nullable=True,
            external_field="bool_field",
        )
        self.add_field(
            target_schema,
            "int8_alias",
            DataType.INT8,
            nullable=True,
            external_field="int8_field",
        )
        self.add_field(
            target_schema,
            "int32_alias",
            DataType.INT32,
            nullable=True,
            external_field="int32_field",
        )
        self.add_field(
            target_schema,
            "float_alias",
            DataType.FLOAT,
            nullable=True,
            external_field="float_field",
        )
        self.add_field(
            target_schema,
            "double_alias",
            DataType.DOUBLE,
            nullable=True,
            external_field="double_field",
        )
        self.add_field(
            target_schema,
            "varchar_alias",
            DataType.VARCHAR,
            max_length=128,
            nullable=True,
            external_field="varchar_field",
        )
        self.add_field(
            target_schema,
            "timestamptz_alias",
            DataType.TIMESTAMPTZ,
            nullable=True,
            external_field="timestamptz_field",
        )
        self.add_field(
            target_schema,
            "json_alias",
            DataType.JSON,
            nullable=True,
            external_field="json_field",
        )
        self.add_field(
            target_schema,
            "geometry_alias",
            DataType.GEOMETRY,
            nullable=True,
            external_field="geometry_field",
        )
        self.add_field(
            target_schema,
            "array_alias",
            DataType.ARRAY,
            element_type=DataType.INT64,
            max_capacity=8,
            nullable=True,
            external_field="array_int64",
        )
        self.add_field(
            target_schema,
            "float_vector_alias",
            DataType.FLOAT_VECTOR,
            dim=DIM,
            external_field="float_vector",
        )
        self.add_field(
            target_schema,
            "float16_vector_alias",
            DataType.FLOAT16_VECTOR,
            dim=DIM,
            external_field="float16_vector",
        )
        self.add_field(
            target_schema,
            "bfloat16_vector_alias",
            DataType.BFLOAT16_VECTOR,
            dim=DIM,
            external_field="bfloat16_vector",
        )
        self.add_field(
            target_schema,
            "binary_vector_alias",
            DataType.BINARY_VECTOR,
            dim=DIM,
            external_field="binary_vector",
        )
        self.create_collection(client, collection_name=target, schema=target_schema)
        self._refresh(client, target)
        self._index_and_load(
            client,
            target,
            indexes=[
                ("float_vector_alias", "FLAT", "L2", {}),
                ("float16_vector_alias", "FLAT", "L2", {}),
                ("bfloat16_vector_alias", "FLAT", "L2", {}),
                ("binary_vector_alias", "BIN_FLAT", "HAMMING", {}),
            ],
        )

        assert self._count(client, target) == SMALL_ROWS
        row_id = 42
        rows = self.query(
            client,
            target,
            filter=f"source_pk == {row_id}",
            output_fields=[
                "source_pk",
                "bool_alias",
                "int8_alias",
                "int32_alias",
                "float_alias",
                "double_alias",
                "varchar_alias",
                "timestamptz_alias",
                "json_alias",
                "geometry_alias",
                "array_alias",
            ],
        )[0]
        assert len(rows) == 1
        row = rows[0]
        expected = _mixed_row(row_id)
        assert row["bool_alias"] == expected["bool_field"]
        assert row["int8_alias"] == expected["int8_field"]
        assert row["int32_alias"] == expected["int32_field"]
        assert float(row["float_alias"]) == pytest.approx(expected["float_field"], abs=1e-5)
        assert float(row["double_alias"]) == pytest.approx(expected["double_field"], abs=1e-9)
        assert row["varchar_alias"] == expected["varchar_field"]
        assert _timestamp_value(row["timestamptz_alias"]) == _timestamp_value(expected["timestamptz_field"])
        assert _json_value(row["json_alias"]) == expected["json_field"]
        assert row["geometry_alias"] == expected["geometry_field"]
        assert list(row["array_alias"]) == expected["array_int64"]

        filter_cases = (
            ("scalar", f"source_pk == {row_id} && int32_alias == {row_id * 7}"),
            ("json", f'source_pk == {row_id} && json_alias["row"] == {row_id}'),
            ("array", f"source_pk == {row_id} && array_contains(array_alias, {row_id + 1})"),
            (
                "geometry",
                f"source_pk == {row_id} && ST_EQUALS(geometry_alias, '{expected['geometry_field']}')",
            ),
        )
        for label, filter_expr in filter_cases:
            filtered = self.query(
                client,
                target,
                filter=filter_expr,
                output_fields=["source_pk"],
                limit=10,
            )[0]
            assert [_row_value(item, "source_pk") for item in filtered] == [row_id], (
                f"{label} filter returned unexpected rows: {filtered!r}"
            )

        nullable_aliases = (
            "bool_alias",
            "int8_alias",
            "int32_alias",
            "float_alias",
            "double_alias",
            "varchar_alias",
            "timestamptz_alias",
            "json_alias",
            "geometry_alias",
            "array_alias",
        )
        null_rows = self.query(
            client,
            target,
            filter=f"source_pk == {null_row_id}",
            output_fields=["source_pk", *nullable_aliases],
        )[0]
        assert len(null_rows) == 1
        assert _row_value(null_rows[0], "source_pk") == null_row_id
        assert all(_row_has_field(null_rows[0], field_name) for field_name in nullable_aliases)
        assert all(_row_value(null_rows[0], field_name) is None for field_name in nullable_aliases)

        vector_cases = [
            ("float_vector_alias", _mixed_float_vector(row_id), "L2", 1e-5),
            ("float16_vector_alias", _mixed_float16_vector(row_id), "L2", 1e-3),
            ("bfloat16_vector_alias", _mixed_bfloat16_vector(row_id), "L2", 1e-2),
            ("binary_vector_alias", _mixed_binary_vector(row_id), "HAMMING", 0.0),
        ]
        for field_name, query_vector, metric_type, tolerance in vector_cases:
            results = self.search(
                client,
                target,
                data=[query_vector],
                anns_field=field_name,
                limit=1,
                output_fields=["source_pk", "varchar_alias"],
                search_params={"metric_type": metric_type, "params": {}},
            )[0]
            assert len(results) == 1
            hits = results[0]
            _assert_search_hits(hits, 1, metric_type)
            hit = hits[0]
            assert _hit_id(hit) == row_id
            assert _row_value(hit, "source_pk") == row_id
            assert float(_row_value(hit, "distance")) == pytest.approx(0.0, abs=tolerance)

        self._assert_schema_alignment(
            source_info,
            self.describe_collection(client, target)[0],
            mixed_mapping,
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(
        reason=(
            "Known bug #52303: milvus-table refresh cannot materialize BM25 input from StorageV3 TEXT LOB references"
        ),
        strict=True,
    )
    def test_milvus_table_target_owned_bm25_output(self, request):
        """
        target: generate a target-owned BM25 field from externally mapped TEXT
        method: snapshot TEXT rows, add an unmapped sparse function output, refresh, index, and search
        expected: TEXT_MATCH and BM25 work while the sparse field keeps a target-owned field ID
        """
        client = self._client()
        cfg = self._minio_cfg(request)
        source = self._name("bm25_src")
        target = self._name("bm25_ext")
        analyzer_params = {"tokenizer": "standard"}

        source_schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        self.add_field(source_schema, "pk", DataType.INT64, is_primary=True)
        self.add_field(source_schema, "embedding", DataType.FLOAT_VECTOR, dim=DIM)
        self.add_field(
            source_schema,
            "text_field",
            DataType.TEXT,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=analyzer_params,
        )
        self.create_collection(client, collection_name=source, schema=source_schema)
        rows = [
            {
                "pk": row_id,
                "embedding": _core_vector(row_id),
                "text_field": f"milvus snapshot topic{row_id % 17} document {row_id}",
            }
            for row_id in range(SMALL_ROWS)
        ]
        self.insert(client, source, rows)
        self._flush_rate_limited(client, source)
        source_info = self.describe_collection(client, source)[0]
        snapshot = self._create_snapshot_ref(client, source, cfg, suffix="bm25")

        target_schema = self.create_schema(
            client,
            auto_id=False,
            enable_dynamic_field=False,
            external_source=snapshot.external_source,
            external_spec=self._external_spec(cfg),
        )[0]
        self.add_field(
            target_schema,
            "source_pk",
            DataType.INT64,
            is_primary=True,
            external_field="pk",
        )
        self.add_field(
            target_schema,
            "vector",
            DataType.FLOAT_VECTOR,
            dim=DIM,
            external_field="embedding",
        )
        self.add_field(
            target_schema,
            "text_alias",
            DataType.TEXT,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=analyzer_params,
            external_field="text_field",
        )
        self.add_field(target_schema, "bm25_sparse", DataType.SPARSE_FLOAT_VECTOR)
        target_schema.add_function(
            Function(
                name="target_bm25",
                function_type=FunctionType.BM25,
                input_field_names=["text_alias"],
                output_field_names=["bm25_sparse"],
                params={},
            )
        )
        self.create_collection(client, collection_name=target, schema=target_schema)
        self._refresh(client, target)
        self._index_and_load(
            client,
            target,
            indexes=[
                ("vector", "FLAT", "L2", {}),
                ("bm25_sparse", "SPARSE_INVERTED_INDEX", "BM25", {}),
            ],
        )

        # This intentionally covers current product behavior beyond the
        # original design document's TEXT_MATCH non-goal.
        topic = "topic7"
        text_rows = self.query(
            client,
            target,
            filter=f'TEXT_MATCH(text_alias, "{topic}")',
            output_fields=["source_pk", "text_alias"],
            limit=30,
        )[0]
        expected_topic_rows = len([row_id for row_id in range(SMALL_ROWS) if row_id % 17 == 7])
        assert len(text_rows) == expected_topic_rows
        assert all(topic in row["text_alias"].split() for row in text_rows)

        search_results = self.search(
            client,
            target,
            data=[topic],
            anns_field="bm25_sparse",
            limit=10,
            output_fields=["source_pk", "text_alias"],
            search_params={"metric_type": "BM25", "params": {}},
        )[0]
        assert len(search_results) == 1
        hits = search_results[0]
        _assert_search_hits(hits, 10, "BM25")
        for hit in hits:
            assert _row_value(hit, "source_pk") is not None
            assert topic in _row_value(hit, "text_alias").split()

        target_info = self.describe_collection(client, target)[0]
        fields = {field["name"]: field for field in target_info["fields"]}
        bm25_field = fields["bm25_sparse"]
        assert bm25_field.get("external_field") in (None, "")
        assert bm25_field.get("is_function_output") is True
        self._assert_schema_alignment(
            source_info,
            target_info,
            {"source_pk": "pk", "vector": "embedding", "text_alias": "text_field"},
            target_owned=("bm25_sparse",),
        )
        functions = {function["name"]: function for function in target_info.get("functions", [])}
        assert functions["target_bm25"]["input_field_names"] == ["text_alias"]
        assert functions["target_bm25"]["output_field_names"] == ["bm25_sparse"]
