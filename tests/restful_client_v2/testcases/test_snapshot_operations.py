import time
from uuid import uuid4

import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name


class TestSnapshotOperations(TestBase):
    def setup_method(self):
        self._snapshots_to_cleanup = []
        self._export_prefixes_to_cleanup = []

    def teardown_method(self):
        try:
            for snapshot_payload in self._snapshots_to_cleanup:
                self.snapshot_client.snapshot_drop(snapshot_payload)
        finally:
            try:
                super().teardown_method()
            finally:
                # Export bundles outlive snapshots; remove only this test's unique prefixes.
                for prefix in self._export_prefixes_to_cleanup:
                    storage = self.storage_client
                    for obj in storage.client.list_objects(storage.bucket_name, prefix=prefix + "/", recursive=True):
                        storage.client.remove_object(storage.bucket_name, obj.object_name)

    def _flush_collection(self, collection_name, timeout=90):
        deadline = time.monotonic() + timeout
        rsp = None
        while time.monotonic() < deadline:
            rsp = self.collection_client.flush(collection_name)
            if rsp["code"] == 0:
                return
            assert rsp["code"] == 1807, rsp
            time.sleep(2)
        pytest.fail(f"collection flush timed out: {rsp}")

    def _assert_snapshot_rows(self, collection_name, expected_rows):
        rsp = self.collection_client.collection_load(collection_name=collection_name)
        assert rsp["code"] == 0, rsp
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            rsp = self.collection_client.collection_load_state(collection_name=collection_name)
            assert rsp["code"] == 0, rsp
            if rsp["data"]["loadState"] == "LoadStateLoaded":
                break
            time.sleep(1)
        else:
            pytest.fail(f"restored collection did not load: {rsp}")

        rsp = self.vector_client.vector_query(
            {
                "collectionName": collection_name,
                "filter": "",
                "outputFields": ["count(*)"],
                "consistencyLevel": "Strong",
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["data"] == [{"count(*)": len(expected_rows)}], rsp
        rsp = self.vector_client.vector_query(
            {
                "collectionName": collection_name,
                "filter": "id >= 0",
                "outputFields": list(expected_rows[0]),
                "limit": len(expected_rows) + 1,
                "consistencyLevel": "Strong",
            }
        )
        assert rsp["code"] == 0, rsp
        # Exactly representable floats allow strict checks, including every vector component.
        assert sorted(rsp["data"], key=lambda row: row["id"]) == expected_rows, rsp
        # Vector IS NULL predicates are unsupported; row values and search check vector validity.
        for field in ("label", "score", "items"):
            rsp = self.vector_client.vector_query(
                {
                    "collectionName": collection_name,
                    "filter": f"{field} is null",
                    "outputFields": ["id"],
                    "limit": len(expected_rows) + 1,
                    "consistencyLevel": "Strong",
                }
            )
            assert rsp["code"] == 0, rsp
            assert sorted(row["id"] for row in rsp["data"]) == [
                row["id"] for row in expected_rows if row[field] is None
            ], rsp

        rsp = self.vector_client.vector_search(
            {
                "collectionName": collection_name,
                "data": [[1.0] * 8],
                "annsField": "optional_vector",
                "limit": len(expected_rows),
                "consistencyLevel": "Strong",
                "searchParams": {"metricType": "L2"},
            }
        )
        assert rsp["code"] == 0, rsp
        expected_ids = {row["id"] for row in expected_rows if row["optional_vector"] is not None}
        assert len(rsp["data"]) == len(expected_ids), rsp
        assert {hit["id"] for hit in rsp["data"]} == expected_ids, rsp

    @pytest.mark.tags(CaseLabel.L0)
    def test_snapshot_lifecycle_and_restore(self):
        """
        target: preserve snapshot data and schema through local and exported restore
        method: snapshot mixed nullable rows, export, drop the source, and restore the bundle
        expected: every field, default, NULL position, and searchable vector survives restore
        """
        source_collection = gen_collection_name(prefix="rest_snapshot_src")
        target_collection = gen_collection_name(prefix="rest_snapshot_dst")
        external_target_collection = gen_collection_name(prefix="rest_snapshot_external_dst")
        snapshot_name = f"rest_snapshot_{uuid4().hex}"
        snapshot_payload = {
            "collectionName": source_collection,
            "snapshotName": snapshot_name,
        }
        fields = [
            {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
            {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": "8"}},
            {
                "fieldName": "optional_vector",
                "dataType": "FloatVector",
                "nullable": True,
                "elementTypeParams": {"dim": "8"},
            },
            {"fieldName": "label", "dataType": "VarChar", "nullable": True, "elementTypeParams": {"max_length": "64"}},
            {"fieldName": "score", "dataType": "Float", "nullable": True},
            {
                "fieldName": "items",
                "dataType": "Array",
                "elementDataType": "Int64",
                "nullable": True,
                "elementTypeParams": {"max_capacity": "8"},
            },
            {"fieldName": "status", "dataType": "Int64", "nullable": True, "defaultValue": 42},
        ]
        rsp = self.collection_client.collection_create(
            {
                "collectionName": source_collection,
                "schema": {"autoId": False, "enableDynamicField": False, "fields": fields},
                "indexParams": [
                    {"fieldName": field, "indexName": field, "indexType": "FLAT", "metricType": "L2"}
                    for field in ("vector", "optional_vector")
                ],
            }
        )
        assert rsp["code"] == 0, rsp
        rows = [
            {
                "id": i,
                "vector": [i / 8] * 8,
                "optional_vector": None if i % 3 == 0 else [i / 4] * 8,
                "label": None if i % 3 == 0 else ("" if i % 3 == 1 else f"row_{i}"),
                "score": None if i % 4 == 0 else i / 2,
                "items": None if i % 3 == 0 else ([] if i % 3 == 1 else [i, -i]),
                "status": None if i % 3 == 0 else (0 if i == 2 else i),
            }
            for i in range(16)
        ]
        for row in rows:
            if row["id"] % 3 == 1:
                del row["status"]
        expected_rows = [dict(row, status=42 if row.get("status") is None else row["status"]) for row in rows]
        # Separate flushes exercise multiple sealed batches with different NULL positions.
        for batch in (rows[:8], rows[8:]):
            rsp = self.vector_client.vector_insert({"collectionName": source_collection, "data": batch})
            assert rsp["code"] == 0, rsp
            assert rsp["data"]["insertCount"] == len(batch), rsp
            self._flush_collection(source_collection)
        self._assert_snapshot_rows(source_collection, expected_rows)
        rsp = self.collection_client.collection_describe(source_collection)
        assert rsp["code"] == 0, rsp
        source_fields = rsp["data"]["fields"]
        self.collection_client.name_list.append(("default", target_collection))
        self.collection_client.name_list.append(("default", external_target_collection))

        create_payload = {
            **snapshot_payload,
            "description": "RESTful v2 snapshot lifecycle coverage",
            "compactionProtectionSeconds": 60,
        }
        rsp = self.snapshot_client.snapshot_create(create_payload)
        assert rsp["code"] == 0, rsp
        self._snapshots_to_cleanup.append(snapshot_payload)

        rsp = self.snapshot_client.snapshot_list({"collectionName": source_collection})
        assert rsp["code"] == 0, rsp
        assert snapshot_name in rsp["data"], rsp

        rsp = self.snapshot_client.snapshot_describe(snapshot_payload)
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["snapshotName"] == snapshot_name, rsp
        assert rsp["data"]["collectionName"] == source_collection, rsp
        assert rsp["data"]["description"] == create_payload["description"], rsp
        assert isinstance(rsp["data"]["partitionNames"], list), rsp
        create_ts = rsp["data"]["createTs"]
        assert isinstance(create_ts, (int, str)) and int(create_ts) > 0, rsp
        source_snapshot_uri = rsp["data"]["s3Location"]
        assert source_snapshot_uri, rsp

        export_prefix = f"snapshot_export_{uuid4().hex}"
        self._export_prefixes_to_cleanup.append(export_prefix)
        rsp = self.snapshot_client.snapshot_export(
            {
                **snapshot_payload,
                "targetS3Path": export_prefix,
            }
        )
        assert rsp["code"] == 0, rsp
        export_job_id = rsp["data"]["jobId"]
        assert isinstance(export_job_id, (int, str)) and int(export_job_id) > 0, rsp
        export_job_id = str(export_job_id)

        deadline = time.time() + 180
        while time.time() < deadline:
            rsp = self.snapshot_client.get_export_snapshot_state(export_job_id)
            assert rsp["code"] == 0, rsp
            state = rsp["data"]["state"]
            if state == "ExportSnapshotCompleted":
                break
            assert state != "ExportSnapshotFailed", rsp
            time.sleep(2)
        else:
            pytest.fail(f"snapshot export did not complete: {rsp}")

        assert str(rsp["data"]["jobId"]) == export_job_id, rsp
        assert rsp["data"]["snapshotName"] == snapshot_name, rsp
        assert rsp["data"]["collectionName"] == source_collection, rsp
        assert int(rsp["data"]["totalBytes"]) > 0, rsp
        snapshot_metadata_uri = rsp["data"]["snapshotMetadataURI"]
        assert snapshot_metadata_uri and snapshot_metadata_uri != source_snapshot_uri, rsp
        assert f"/{export_prefix}/" in snapshot_metadata_uri, rsp

        rsp = self.snapshot_client.snapshot_pin({**snapshot_payload, "ttlSeconds": 60})
        assert rsp["code"] == 0, rsp
        pin_id = rsp["data"]["pinId"]
        assert isinstance(pin_id, (int, str)) and int(pin_id) > 0, rsp

        rsp = self.snapshot_client.snapshot_unpin({"pinId": str(pin_id)})
        assert rsp["code"] == 0, rsp

        rsp = self.snapshot_client.snapshot_restore(
            {
                "snapshotName": snapshot_name,
                "sourceCollectionName": source_collection,
                "targetCollectionName": target_collection,
            }
        )
        assert rsp["code"] == 0, rsp
        job_id = rsp["data"]["jobId"]
        assert isinstance(job_id, (int, str)) and int(job_id) > 0, rsp
        job_id = str(job_id)

        deadline = time.time() + 180
        while time.time() < deadline:
            rsp = self.snapshot_client.get_restore_snapshot_state(job_id)
            assert rsp["code"] == 0, rsp
            state = rsp["data"]["state"]
            if state == "RestoreSnapshotCompleted":
                break
            assert state != "RestoreSnapshotFailed", rsp
            time.sleep(2)
        else:
            pytest.fail(f"snapshot restore did not complete: {rsp}")

        assert str(rsp["data"]["jobId"]) == job_id, rsp
        assert rsp["data"]["snapshotName"] == snapshot_name, rsp
        assert rsp["data"]["collectionName"] == target_collection, rsp

        rsp = self.snapshot_client.list_restore_snapshot_jobs({"collectionName": target_collection})
        assert rsp["code"] == 0, rsp
        matching_jobs = [record for record in rsp["data"]["records"] if str(record["jobId"]) == job_id]
        assert len(matching_jobs) == 1, rsp
        assert matching_jobs[0]["snapshotName"] == snapshot_name, rsp
        assert matching_jobs[0]["collectionName"] == target_collection, rsp
        assert matching_jobs[0]["state"] == "RestoreSnapshotCompleted", rsp

        rsp = self.collection_client.collection_has(collection_name=target_collection)
        assert rsp["code"] == 0 and rsp["data"]["has"], rsp

        self._assert_snapshot_rows(target_collection, expected_rows)
        rsp = self.collection_client.collection_describe(target_collection)
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["fields"] == source_fields, rsp
        rsp = self.collection_client.collection_drop({"collectionName": target_collection})
        assert rsp["code"] == 0, rsp

        rsp = self.snapshot_client.snapshot_drop(snapshot_payload)
        assert rsp["code"] == 0, rsp
        self._snapshots_to_cleanup.remove(snapshot_payload)

        rsp = self.snapshot_client.snapshot_list({"collectionName": source_collection})
        assert rsp["code"] == 0, rsp
        assert snapshot_name not in rsp["data"], rsp

        rsp = self.collection_client.collection_drop({"collectionName": source_collection})
        assert rsp["code"] == 0, rsp
        rsp = self.collection_client.collection_has(collection_name=source_collection)
        assert rsp["code"] == 0 and not rsp["data"]["has"], rsp

        # Restore after removing both the source snapshot and its collection.
        rsp = self.snapshot_client.snapshot_restore_external(
            {
                "targetCollectionName": external_target_collection,
                "snapshotMetadataURI": snapshot_metadata_uri,
            }
        )
        assert rsp["code"] == 0, rsp
        external_restore_job_id = rsp["data"]["jobId"]
        assert isinstance(external_restore_job_id, (int, str)) and int(external_restore_job_id) > 0, rsp
        external_restore_job_id = str(external_restore_job_id)

        deadline = time.time() + 180
        while time.time() < deadline:
            rsp = self.snapshot_client.get_restore_snapshot_state(external_restore_job_id)
            assert rsp["code"] == 0, rsp
            state = rsp["data"]["state"]
            if state == "RestoreSnapshotCompleted":
                break
            assert state != "RestoreSnapshotFailed", rsp
            time.sleep(2)
        else:
            pytest.fail(f"external snapshot restore did not complete: {rsp}")

        assert str(rsp["data"]["jobId"]) == external_restore_job_id, rsp
        assert rsp["data"]["collectionName"] == external_target_collection, rsp

        rsp = self.collection_client.collection_has(collection_name=external_target_collection)
        assert rsp["code"] == 0 and rsp["data"]["has"], rsp

        self._assert_snapshot_rows(external_target_collection, expected_rows)
        rsp = self.collection_client.collection_describe(external_target_collection)
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["fields"] == source_fields, rsp
        rsp = self.collection_client.collection_release(collection_name=external_target_collection)
        assert rsp["code"] == 0, rsp
        self._assert_snapshot_rows(external_target_collection, expected_rows)
