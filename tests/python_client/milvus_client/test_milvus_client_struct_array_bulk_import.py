# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Struct Array bulk import E2E coverage."""

import copy
import csv
import json
import os
import time
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from base.struct_array import MetadataStructArrayTestBase, StructArrayTestBase
from common import common_func as cf
from common.common_type import CaseLabel
from common.external_table_common import build_external_source, build_external_spec, get_minio_config
from common.struct_array_data import (
    CAPACITY,
    child_schema,
    make_metadata_row,
    make_nullable_row,
    make_required_elements,
    normalized_vector,
)
from minio import Minio
from pymilvus import DataType
from pymilvus.bulk_writer import bulk_import, get_import_progress
from pymilvus.client.prepare import Prepare
from utils.util_log import test_log as log


class TestMilvusClientStructArrayBulkImport(MetadataStructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("file_format", ["json", "parquet"])
    @pytest.mark.parametrize("profile", ["mixed", "all_null"])
    def test_struct_array_import_mutation_evolution_and_compaction(
        self, file_format, profile, minio_host, minio_bucket, tmp_path
    ):
        """
        target: preserve imported row masks and old-row backfill across subsequent writes and compaction
        method: import long NULL runs and varied lengths, mutate old/new PKs, add a struct and compact
        expected: complete input-derived row map and predicate sets agree after every phase
        """
        client = self._client()
        name = self.create_metadata_collection(client, vectors=False, scalar_index="INVERTED")
        rows = [make_metadata_row(i, vectors=False) for i in range(1500)]
        for row in rows:
            if profile == "all_null" or 100 <= row["id"] < 500 or 900 <= row["id"] < 1100:
                row["clips"] = None
        model = {row["id"]: copy.deepcopy(row) for row in rows}
        local = tmp_path / f"{name}.{file_format}"
        if file_format == "json":
            local.write_text(json.dumps(rows))
        else:
            element = pa.struct([("value", pa.int64()), ("tag", pa.string()), ("flag", pa.bool_())])
            schema = pa.schema(
                [
                    ("id", pa.int64()),
                    ("revision", pa.int64()),
                    ("bucket", pa.int64()),
                    ("normal_vector", pa.list_(pa.float32())),
                    ("note", pa.string()),
                    ("clips", pa.list_(element)),
                ]
            )
            pq.write_table(pa.Table.from_pylist(rows, schema=schema), local, row_group_size=127)
        storage = Minio(f"{minio_host}:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
        key = f"bulkinsert_data/struct_contract/{local.name}"
        storage.fput_object(minio_bucket, key, str(local))
        url = f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        try:
            result = bulk_import(url=url, collection_name=name, files=[[key]], api_key=cf.param_info.param_token)
            result.raise_for_status()
            payload = result.json()
            assert payload.get("code", 0) == 0, payload
            job_id = payload["data"]["jobId"]
            deadline = time.monotonic() + 300
            completed = False
            while time.monotonic() < deadline:
                response = get_import_progress(url=url, job_id=job_id, api_key=cf.param_info.param_token)
                response.raise_for_status()
                progress = response.json()["data"]
                assert progress["state"] != "Failed", progress
                if progress["state"] == "Completed":
                    assert progress["progress"] == 100
                    completed = True
                    break
                time.sleep(2)
            assert completed, f"Import did not complete: {job_id}"
            log.info(f"STRUCT_IMPORT format={file_format} profile={profile} completed_job={job_id} rows={len(rows)}")
            assert self.wait_for_index_ready(client, name, index_name="normal_vector", timeout=300)
            self.refresh_load(client, name)
            self.assert_metadata_rows(client, name, model)
            self.assert_required_predicates(client, name, model)

            # A single batch combines imported rows and new keys while omitting the imported struct.
            patches = []
            for number in list(range(0, 1500, 13)) + list(range(1500, 1530)):
                source = make_metadata_row(number, 5, vectors=False)
                patch = {key: source[key] for key in ("id", "revision", "bucket", "normal_vector")}
                expected = copy.deepcopy(model.get(number, {**source, "clips": None, "note": None}))
                expected.update(copy.deepcopy(patch))
                model[number] = expected
                patches.append(patch)
            self.upsert(client, name, patches, partial_update=True)
            replacements = [make_metadata_row(i, 7, vectors=False) for i in range(1, 1500, 11)]
            self.upsert(client, name, replacements)
            model.update({row["id"]: copy.deepcopy(row) for row in replacements})
            deleted = list(range(2, 1500, 9))
            self.delete(client, name, ids=deleted)
            for pk in deleted:
                del model[pk]
            self.reload_collection(client, name)
            self.assert_metadata_rows(client, name, model)
            self.assert_required_predicates(client, name, model)

            self.add_collection_struct_field(
                client, name, "added", self.required_child_schema(client, vectors=False), CAPACITY
            )
            for row in model.values():
                row["added"] = None
            self.refresh_load(client, name)
            self.assert_metadata_rows(client, name, model)
            patches = [{"id": pk, "added": make_required_elements(pk, 9, vectors=False)} for pk in sorted(model)[::5]]
            self.upsert(client, name, patches, partial_update=True)
            for patch in patches:
                model[patch["id"]].update(copy.deepcopy(patch))
            self.reload_collection(client, name)
            self.assert_metadata_rows(client, name, model)
            assert self.wait_for_index_ready(client, name, index_name="normal_vector", timeout=300)
            compact_id = -1
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                compact_id, _ = self.compact(client, name)
                if compact_id > 0:
                    break
                time.sleep(2)
            assert compact_id > 0
            assert self.wait_for_compaction_ready(client, compact_id, timeout=300)
            log.info(f"STRUCT_IMPORT_COMPACT collection={name} completed_compaction={compact_id}")
            self.reload_collection(client, name)
            self.assert_metadata_rows(client, name, model)
            self.assert_required_predicates(client, name, model)
        finally:
            storage.remove_object(minio_bucket, key)


def _write_file(path, rows):
    if path.suffix == ".json":
        path.write_text(json.dumps(rows), encoding="utf-8")
    elif path.suffix == ".jsonl":
        path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
    elif path.suffix == ".csv":
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=["id", "revision", "normal_vector", "clips"])
            writer.writeheader()
            writer.writerows(
                {**row, "normal_vector": json.dumps(row["normal_vector"]), "clips": json.dumps(row["clips"])}
                for row in rows
            )
    else:
        element = pa.struct(
            [
                pa.field("score", pa.int64()),
                pa.field("emb", pa.list_(pa.float32())),
                pa.field("tags", pa.list_(pa.int64())),
            ]
        )
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("revision", pa.int64()),
                pa.field("normal_vector", pa.list_(pa.float32())),
                pa.field("clips", pa.list_(element)),
            ]
        )
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


class TestMilvusClientStructArrayImportValidation(StructArrayTestBase):
    @pytest.mark.tags(CaseLabel.L2)
    def test_external_struct_rejection_with_real_source_control(self, tmp_path, minio_host, minio_bucket):
        """
        target: reject Struct fields on External Collections after validating real source access
        method: refresh/query owned Parquet data, then Create an external Struct schema on that source
        expected: ordinary control succeeds and Struct rejection is independent of storage/auth errors
        """
        client = self._client()
        config = get_minio_config(minio_host=minio_host, minio_bucket=minio_bucket)
        storage = Minio(
            config["address"], access_key=config["access_key"], secret_key=config["secret_key"], secure=False
        )
        prefix = f"bulkinsert_data/struct_external/{uuid4().hex}"
        key = f"{prefix}/data.parquet"
        path = tmp_path / "external.parquet"
        table = pa.table(
            {
                "id": pa.array([10, 20], type=pa.int64()),
                "vector": pa.array([[0.25] * 8, [0.5] * 8], type=pa.list_(pa.float32(), 8)),
            }
        )
        pq.write_table(table, path)
        storage.fput_object(minio_bucket, key, str(path))
        good_name, bad_name = f"struct_ext_control_{uuid4().hex}", f"struct_ext_bad_{uuid4().hex}"
        try:
            schema = client.create_schema(
                external_source=build_external_source(config, prefix), external_spec=build_external_spec(config)
            )
            schema.add_field("id", DataType.INT64, external_field="id")
            schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8, external_field="vector")
            client.create_collection(good_name, schema=schema)
            job_id = client.refresh_external_collection(good_name)
            deadline = time.monotonic() + 180
            while True:
                progress = client.get_refresh_external_collection_progress(job_id)
                if progress.state in {"RefreshCompleted", "RefreshFailed"}:
                    break
                assert time.monotonic() < deadline, progress
                time.sleep(1)
            assert progress.state == "RefreshCompleted", progress
            index = client.prepare_index_params()
            index.add_index("vector", index_type="FLAT", metric_type="L2")
            client.create_index(good_name, index)
            client.load_collection(good_name)
            assert {r["id"] for r in client.query(good_name, filter="id >= 0", output_fields=["id"])} == {10, 20}
            children = client.create_struct_field_schema()
            children.add_field("score", DataType.INT64, nullable=True)
            schema.add_field(
                "clips",
                DataType.ARRAY,
                element_type=DataType.STRUCT,
                struct_schema=children,
                max_capacity=8,
                nullable=True,
            )
            request = Prepare.create_collection_request(bad_name, schema)
            status = client._get_connection()._stub.CreateCollection(request, timeout=30)
            assert status.code == 1100 and "does not support struct fields" in status.reason, status
            assert not client.has_collection(bad_name)
            assert {r["id"] for r in client.query(good_name, filter="id >= 0", output_fields=["id"])} == {10, 20}
        finally:
            client.drop_collection(good_name)
            client.drop_collection(bad_name)
            storage.remove_object(minio_bucket, key)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("format_name", ["json", "jsonl", "csv", "parquet"])
    @pytest.mark.parametrize("violation", ["scalar", "vector", "inner", "leaf", "element", "missing"])
    def test_disallowed_null_import_fails_and_corrected_file_recovers(
        self, tmp_path, minio_host, minio_bucket, format_name, violation
    ):
        """
        target: reject NULL at every required Struct layer in all documented import formats
        method: upload raw mixed valid/invalid rows, wait for Failed, then import corrected input
        expected: data-related failure, no visible failed-job rows, exact original and repaired values
        """
        client = self._client()
        name = self.create_struct_collection(
            client,
            {
                "score": child_schema(DataType.INT64, False),
                "emb": child_schema(DataType.FLOAT_VECTOR, False),
                "tags": child_schema(
                    DataType.ARRAY, False, element_type=DataType.INT64, max_capacity=4, element_nullable=False
                ),
            },
        )
        rows = [make_nullable_row(n, [{"score": n, "emb": normalized_vector(n), "tags": [n, n + 1]}]) for n in range(3)]
        model = self.insert_rows_with_model(client, name, rows)
        incoming = [
            make_nullable_row(n, [{"score": n, "emb": normalized_vector(n), "tags": [n, n + 1]}])
            for n in (100, 101, 102)
        ]
        bad = copy.deepcopy(incoming)
        element = bad[1]["clips"][0]
        if violation in ("scalar", "vector", "inner"):
            element[{"scalar": "score", "vector": "emb", "inner": "tags"}[violation]] = None
        elif violation == "leaf":
            element["tags"] = [1, None]
        elif violation == "element":
            bad[1]["clips"] = [None]
        else:
            del element["score"]
        endpoint = minio_host if ":" in minio_host else f"{minio_host}:9000"
        storage = Minio(
            endpoint,
            access_key=os.getenv("MILVUS_MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MILVUS_MINIO_SECRET_KEY", "minioadmin"),
            secure=False,
        )
        url = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
        objects = []
        try:
            for label, data, expected_state in [("bad", bad, "Failed"), ("repaired", incoming, "Completed")]:
                path = tmp_path / f"{label}.{format_name}"
                _write_file(path, data)
                key = f"bulkinsert_data/struct_null/{uuid4().hex}/{path.name}"
                objects.append(key)
                storage.fput_object(minio_bucket, key, str(path))
                response = bulk_import(
                    url=url, collection_name=name, files=[[key]], api_key=cf.param_info.param_token
                ).json()
                assert response["code"] == 0, response
                job_id = response["data"]["jobId"]
                deadline = time.monotonic() + 180
                while True:
                    progress = get_import_progress(url=url, job_id=job_id, api_key=cf.param_info.param_token).json()
                    assert progress["code"] == 0, progress
                    state = progress["data"]["state"]
                    if state in {"Failed", "Completed"}:
                        break
                    assert time.monotonic() < deadline, progress
                    time.sleep(1)
                log.info(f"Struct NULL import {format_name}/{violation}/{label}: {progress}")
                assert state == expected_state, progress
                if label == "bad":
                    reason = progress["data"].get("reason", "").lower()
                    assert any(word in reason for word in ("null", "missing", "required", "struct", "array")), progress
                    self.assert_rows(client, name, model)
                else:
                    model.update({row["id"]: row for row in incoming})
                    self.reload_collection(client, name)
                    self.assert_rows(client, name, model)
        finally:
            for key in objects:
                storage.remove_object(minio_bucket, key)
