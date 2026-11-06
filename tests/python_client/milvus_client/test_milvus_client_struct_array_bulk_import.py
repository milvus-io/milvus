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

"""Struct array bulk import regression cases."""

import copy
import json
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from common import common_func as cf
from common.common_type import CaseLabel
from common.struct_array_row_model import CAPACITY, StructArrayRowModelBase, _profile, _row
from minio import Minio
from pymilvus.bulk_writer import bulk_import, get_import_progress
from utils.util_log import test_log as log


class TestMilvusClientStructArrayBulkImport(StructArrayRowModelBase):
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
        name = self._create_contract_collection(client, vectors=False, scalar_index="INVERTED")
        rows = [_row(i, vectors=False) for i in range(1500)]
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
            self._assert_model(client, name, model)
            self._assert_predicates(client, name, model)

            # A single batch combines imported rows and new keys while omitting the imported struct.
            patches = []
            for number in list(range(0, 1500, 13)) + list(range(1500, 1530)):
                source = _row(number, 5, vectors=False)
                patch = {key: source[key] for key in ("id", "revision", "bucket", "normal_vector")}
                expected = copy.deepcopy(model.get(number, {**source, "clips": None, "note": None}))
                expected.update(copy.deepcopy(patch))
                model[number] = expected
                patches.append(patch)
            self.upsert(client, name, patches, partial_update=True)
            replacements = [_row(i, 7, vectors=False) for i in range(1, 1500, 11)]
            self.upsert(client, name, replacements)
            model.update({row["id"]: copy.deepcopy(row) for row in replacements})
            deleted = list(range(2, 1500, 9))
            self.delete(client, name, ids=deleted)
            for pk in deleted:
                del model[pk]
            self._reload(client, name)
            self._assert_model(client, name, model)
            self._assert_predicates(client, name, model)

            self.add_collection_struct_field(
                client, name, "added", self._struct_schema(client, vectors=False), CAPACITY
            )
            for row in model.values():
                row["added"] = None
            self.refresh_load(client, name)
            self._assert_model(client, name, model)
            patches = [{"id": pk, "added": _profile(pk, 9, vectors=False)} for pk in sorted(model)[::5]]
            self.upsert(client, name, patches, partial_update=True)
            for patch in patches:
                model[patch["id"]].update(copy.deepcopy(patch))
            self._reload(client, name)
            self._assert_model(client, name, model)
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
            self._reload(client, name)
            self._assert_model(client, name, model)
            self._assert_predicates(client, name, model)
        finally:
            storage.remove_object(minio_bucket, key)
