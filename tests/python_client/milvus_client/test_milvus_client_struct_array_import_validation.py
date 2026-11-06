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

"""Struct array import validation regression cases."""

import copy
import csv
import json
import os
import time
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from common import common_func as cf
from common.common_type import CaseLabel
from common.external_table_common import build_external_source, build_external_spec, get_minio_config
from common.struct_array_nullable_helpers import StructArrayNullableBase, _child, _record, _vector
from minio import Minio
from pymilvus import DataType
from pymilvus.bulk_writer import bulk_import, get_import_progress
from pymilvus.client.prepare import Prepare
from utils.util_log import test_log as log


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


class TestMilvusClientStructArrayImportValidation(StructArrayNullableBase):
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
        name = self._create(
            client,
            {
                "score": _child(DataType.INT64, False),
                "emb": _child(DataType.FLOAT_VECTOR, False),
                "tags": _child(
                    DataType.ARRAY, False, element_type=DataType.INT64, max_capacity=4, element_nullable=False
                ),
            },
        )
        rows = [_record(n, [{"score": n, "emb": _vector(n), "tags": [n, n + 1]}]) for n in range(3)]
        model = self._seed(client, name, rows)
        incoming = [_record(n, [{"score": n, "emb": _vector(n), "tags": [n, n + 1]}]) for n in (100, 101, 102)]
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
                    self._check(client, name, model)
                else:
                    model.update({row["id"]: row for row in incoming})
                    self._reload(client, name)
                    self._check(client, name, model)
        finally:
            for key in objects:
                storage.remove_object(minio_bucket, key)
