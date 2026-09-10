import time
from uuid import uuid4

import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name


class TestSnapshotOperations(TestBase):
    def setup_method(self):
        self._snapshots_to_cleanup = []

    def teardown_method(self):
        try:
            for snapshot_payload in self._snapshots_to_cleanup:
                self.snapshot_client.snapshot_drop(snapshot_payload)
        finally:
            super().teardown_method()

    @pytest.mark.tags(CaseLabel.L0)
    def test_snapshot_lifecycle_and_restore(self):
        source_collection = gen_collection_name(prefix="rest_snapshot_src")
        target_collection = gen_collection_name(prefix="rest_snapshot_dst")
        external_target_collection = gen_collection_name(prefix="rest_snapshot_external_dst")
        snapshot_name = f"rest_snapshot_{uuid4().hex}"
        snapshot_payload = {
            "collectionName": source_collection,
            "snapshotName": snapshot_name,
        }
        self.init_collection(source_collection, dim=8, nb=10)
        self.collection_client.name_list.append(("default", target_collection))
        self.collection_client.name_list.append(("default", external_target_collection))
        rsp = self.collection_client.flush(source_collection)
        assert rsp["code"] == 0, rsp

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
        assert rsp["data"]["s3Location"], rsp

        rsp = self.snapshot_client.snapshot_export(
            {
                **snapshot_payload,
                "targetS3Path": f"snapshot_export_{uuid4().hex}",
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
        assert snapshot_metadata_uri, rsp

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

        rsp = self.snapshot_client.snapshot_drop(snapshot_payload)
        assert rsp["code"] == 0, rsp
        self._snapshots_to_cleanup.remove(snapshot_payload)

        rsp = self.snapshot_client.snapshot_list({"collectionName": source_collection})
        assert rsp["code"] == 0, rsp
        assert snapshot_name not in rsp["data"], rsp
