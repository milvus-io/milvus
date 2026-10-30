import time
from copy import deepcopy
from uuid import uuid4

import pyarrow.parquet as pq
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from minio import Minio
from pymilvus import DataType
from pymilvus.bulk_writer import BulkFileType, LocalBulkWriter, bulk_import, get_import_progress


class TestMilvusClientImportIndependent(TestMilvusClientV2Base):
    @staticmethod
    def _import_url():
        return cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"

    @staticmethod
    def _wait_for_state(url, job_id, expected_states, timeout=300):
        deadline = time.monotonic() + timeout
        last_data = None
        while time.monotonic() < deadline:
            response = get_import_progress(url=url, job_id=job_id, api_key=cf.param_info.param_token)
            payload = response.json()
            assert payload["code"] == 0, payload
            last_data = payload["data"]
            if last_data["state"] in expected_states:
                return last_data
            if last_data["state"] in {"Failed", "Completed"}:
                raise AssertionError(f"Import job {job_id} unexpectedly terminated: {last_data}")
            time.sleep(2)
        raise AssertionError(f"Import job {job_id} did not reach {expected_states}: {last_data}")

    @pytest.fixture
    def upload_import_file(self, minio_host, minio_bucket, tmp_path):
        storage_client = Minio(f"{minio_host}:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
        prefix = f"bulkinsert_data/{uuid4()}"
        objects = []

        def upload(file_path):
            object_name = f"{prefix}/{file_path.relative_to(tmp_path).as_posix()}"
            objects.append(object_name)
            storage_client.fput_object(minio_bucket, object_name, str(file_path))
            return object_name

        yield upload
        for object_name in objects:
            storage_client.remove_object(minio_bucket, object_name)

    def _import_and_wait(self, collection_name, files, options=None, expected_state="Completed", partition_name=""):
        response = bulk_import(
            url=self._import_url(),
            collection_name=collection_name,
            partition_name=partition_name,
            files=files,
            options=options or {},
            api_key=cf.param_info.param_token,
        ).json()
        assert response["code"] == 0, response
        return self._wait_for_state(self._import_url(), response["data"]["jobId"], {expected_state})

    def _assert_import_rows(self, client, collection_name, expected_rows):
        results = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=list(expected_rows[0]),
            limit=len(expected_rows) + 1,
            check_task=CheckTasks.check_query_results,
            check_items={"exp_limit": len(expected_rows)},
        )[0]
        expected_by_id = {row["id"]: row for row in expected_rows}
        assert len(expected_by_id) == len(expected_rows)
        assert {row["id"] for row in results} == set(expected_by_id)
        for row in results:
            # These fixtures use exactly representable floats, so no rounding tolerance is needed.
            assert row == expected_by_id[row["id"]], {"actual": row, "expected": expected_by_id[row["id"]]}

    @pytest.mark.tags(CaseLabel.L1)
    # All-null sparse inserts are tracked separately in milvus-io/milvus#54330.
    @pytest.mark.parametrize(
        "all_null, ingress",
        [
            pytest.param(False, "insert", id="mixed_values-insert"),
            pytest.param(False, "bulk_import", id="mixed_values-bulk_import"),
            pytest.param(True, "bulk_import", id="all_null-bulk_import"),
        ],
    )
    def test_bulk_writer_nullable_struct_and_sparse_match_insert(self, all_null, ingress, upload_import_file, tmp_path):
        """
        target: nullable StructArray and sparse-vector equivalence across insert and bulk import
        method: write explicit nulls, omitted fields, empty arrays and populated values through both paths
        expected: Parquet nulls and every queried value match the source, including after release/load
        """
        client = self._client()
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        struct = self.create_struct_field_schema(client)[0]
        struct.add_field("value", DataType.INT64)
        struct.add_field("embedding", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field(
            "chunks", DataType.ARRAY, element_type=DataType.STRUCT, struct_schema=struct, max_capacity=2, nullable=True
        )
        schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR, nullable=True)
        indexes = self.prepare_index_params(client)[0]
        indexes.add_index("vector", index_type="FLAT", metric_type="L2")
        indexes.add_index(
            "chunks[embedding]", index_type="HNSW", metric_type="MAX_SIM_COSINE", params={"M": 8, "efConstruction": 64}
        )
        indexes.add_index("sparse", index_type="SPARSE_INVERTED_INDEX", metric_type="IP")
        collection_name = cf.gen_unique_str(f"nullable_{ingress}")
        self.create_collection(client, collection_name, schema=schema, index_params=indexes)

        rows = [
            {"id": 0, "vector": [0.0, 0.25, 0.5, 1.0], "chunks": None, "sparse": None},
            {"id": 1, "vector": [1.0, 0.25, 0.5, 1.0]},
        ]
        if not all_null:
            rows.extend(
                [
                    {"id": 2, "vector": [2.0, 0.25, 0.5, 1.0], "chunks": [], "sparse": {1: 0.5}},
                    {
                        "id": 3,
                        "vector": [3.0, 0.25, 0.5, 1.0],
                        "chunks": [{"value": 7, "embedding": [1.0, 0.5, 0.25, 0.0]}],
                        "sparse": None,
                    },
                    {"id": 4, "vector": [4.0, 0.25, 0.5, 1.0], "chunks": None, "sparse": {3: 0.25}},
                    {
                        "id": 5,
                        "vector": [5.0, 0.25, 0.5, 1.0],
                        "chunks": [{"value": 11, "embedding": [0.0, 0.25, 0.5, 1.0]}],
                        "sparse": {2: 0.125},
                    },
                ]
            )
        # Freeze the source oracle before either SDK path can normalize its input.
        expected = [
            dict(deepcopy(row), chunks=deepcopy(row.get("chunks")), sparse=deepcopy(row.get("sparse"))) for row in rows
        ]
        if ingress == "insert":
            result = self.insert(client, collection_name, deepcopy(rows))[0]
            assert result["insert_count"] == len(rows), result
        else:
            with LocalBulkWriter(schema=schema, local_path=str(tmp_path), file_type=BulkFileType.PARQUET) as writer:
                for row in deepcopy(rows):
                    writer.append_row(row)
                writer.commit()
                assert len(writer.batch_files) == 1 and len(writer.batch_files[0]) == 1
                table = pq.read_table(writer.batch_files[0][0])
                for field_name in ("chunks", "sparse"):
                    assert table.column(field_name).null_count == sum(row[field_name] is None for row in expected)
                files = [[upload_import_file(tmp_path / writer.batch_files[0][0])]]
                progress = self._import_and_wait(collection_name, files)
                assert progress["importedRows"] == progress["totalRows"] == len(rows), progress
            self.refresh_load(client, collection_name)
        self._assert_import_rows(client, collection_name, expected)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        self._assert_import_rows(client, collection_name, expected)
