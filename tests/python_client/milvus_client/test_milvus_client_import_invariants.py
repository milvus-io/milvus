import time
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from minio import Minio
from pymilvus import DataType
from pymilvus.bulk_writer import abort_import, bulk_import, commit_import, get_import_progress


class TestMilvusClientImportInvariantsIndependent(TestMilvusClientV2Base):
    @staticmethod
    def _import_url():
        return f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"

    def _create_import_collection(self, client, collection_name):
        schema = self.create_schema(client, auto_id=True, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field("tag", DataType.VARCHAR, max_length=64)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

    @staticmethod
    def _upload_parquet(storage_client, bucket_name, tmp_path, rows, file_name):
        schema = pa.schema(
            [
                pa.field("tag", pa.string(), nullable=False),
                pa.field("vector", pa.list_(pa.float32()), nullable=False),
            ]
        )
        file_path = tmp_path / file_name
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), file_path)
        object_name = f"bulkinsert_data/{uuid4()}/{file_name}"
        storage_client.fput_object(bucket_name, object_name, str(file_path))
        return object_name

    @staticmethod
    def _wait_for_state(url, job_id, expected_states, timeout=300):
        deadline = time.monotonic() + timeout
        last_data = None
        while time.monotonic() < deadline:
            response = get_import_progress(url=url, job_id=job_id, api_key=cf.param_info.param_token)
            last_data = response.json()["data"]
            if last_data["state"] in expected_states:
                return last_data
            if last_data["state"] == "Failed":
                raise AssertionError(f"Import job {job_id} failed: {last_data.get('reason')}")
            time.sleep(2)
        raise AssertionError(f"Import job {job_id} did not reach {expected_states}: {last_data}")

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_multi_file_auto_id_preserves_rows_and_file_progress(self, minio_host, minio_bucket, tmp_path):
        """
        target: multi-file autoID import and per-file progress
        method: import two populated Parquet files and one empty Parquet file in one job
        expected: every row has one unique ID and each file reports its own row count
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_multi_file_auto_id")
        self._create_import_collection(client, collection_name)
        storage_client = Minio(f"{minio_host}:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
        groups = [
            [{"tag": f"first_{i}", "vector": [float(i)] * 8} for i in range(3)],
            [],
            [{"tag": f"last_{i}", "vector": [float(i + 3)] * 8} for i in range(5)],
        ]
        files = []
        try:
            for index, rows in enumerate(groups):
                files.append(
                    self._upload_parquet(storage_client, minio_bucket, tmp_path, rows, f"part_{index}.parquet")
                )

            response = bulk_import(
                url=self._import_url(),
                collection_name=collection_name,
                files=[[file_name] for file_name in files],
                api_key=cf.param_info.param_token,
            )
            job_id = response.json()["data"]["jobId"]
            progress = self._wait_for_state(self._import_url(), job_id, {"Completed"})
            assert progress["importedRows"] == progress["totalRows"] == 8, progress

            details = progress["details"]
            assert len(details) == len(files), progress
            for file_name, rows in zip(files, groups):
                matching = [detail for detail in details if file_name in detail["fileName"]]
                assert len(matching) == 1, progress
                assert matching[0]["totalRows"] == len(rows), matching[0]
                assert matching[0]["importedRows"] == len(rows), matching[0]

            client.refresh_load(collection_name=collection_name)
            results = self.query(
                client,
                collection_name,
                filter='tag != ""',
                output_fields=["id", "tag"],
                limit=100,
            )[0]
            expected_tags = {row["tag"] for rows in groups for row in rows}
            assert {row["tag"] for row in results} == expected_tags, results
            assert len({row["id"] for row in results}) == len(expected_tags), results
        finally:
            for file_name in files:
                storage_client.remove_object(minio_bucket, file_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("action,terminal_state", [("commit", "Completed"), ("abort", "Failed")])
    def test_import_empty_file_manual_job_reaches_terminal_state(
        self, minio_host, minio_bucket, tmp_path, action, terminal_state
    ):
        """
        target: zero-row manual import lifecycle
        method: create a manual import from an empty Parquet file, then commit or abort
        expected: the job reaches the requested terminal state without visible rows
        """
        client = self._client()
        collection_name = cf.gen_unique_str("import_empty_manual")
        self._create_import_collection(client, collection_name)
        storage_client = Minio(f"{minio_host}:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
        file_name = self._upload_parquet(storage_client, minio_bucket, tmp_path, [], "empty.parquet")
        try:
            response = bulk_import(
                url=self._import_url(),
                collection_name=collection_name,
                files=[[file_name]],
                options={"auto_commit": "false"},
                api_key=cf.param_info.param_token,
            )
            job_id = response.json()["data"]["jobId"]
            progress = self._wait_for_state(self._import_url(), job_id, {"Uncommitted"})
            assert progress["importedRows"] == progress["totalRows"] == 0, progress

            operation = commit_import if action == "commit" else abort_import
            operation(url=self._import_url(), job_id=job_id, api_key=cf.param_info.param_token)
            progress = self._wait_for_state(self._import_url(), job_id, {terminal_state})
            assert progress["importedRows"] == progress["totalRows"] == 0, progress

            client.refresh_load(collection_name=collection_name)
            results = self.query(client, collection_name, filter="id >= 0", output_fields=["id"])[0]
            assert results == [], results
        finally:
            storage_client.remove_object(minio_bucket, file_name)
