import time
from uuid import uuid4

import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name


class TestSnapshotOperations(TestBase):
    @pytest.mark.tags(CaseLabel.L0)
    def test_snapshot_lifecycle_and_restore(self):
        source_collection = gen_collection_name(prefix="rest_snapshot_src")
        target_collection = gen_collection_name(prefix="rest_snapshot_dst")
        snapshot_name = f"rest_snapshot_{uuid4().hex}"
        snapshot_payload = {
            "collectionName": source_collection,
            "snapshotName": snapshot_name,
        }
        snapshot_created = False

        self.init_collection(source_collection, dim=8, nb=10)
        self.collection_client.name_list.append(("default", target_collection))
        rsp = self.collection_client.flush(source_collection)
        assert rsp["code"] == 0, rsp

        try:
            create_payload = {
                **snapshot_payload,
                "description": "RESTful v2 snapshot lifecycle coverage",
                "compactionProtectionSeconds": 60,
            }
            rsp = self.snapshot_client.snapshot_create(create_payload)
            assert rsp["code"] == 0, rsp
            snapshot_created = True

            rsp = self.snapshot_client.snapshot_list({"collectionName": source_collection})
            assert rsp["code"] == 0, rsp
            assert snapshot_name in rsp["data"], rsp

            rsp = self.snapshot_client.snapshot_describe(snapshot_payload)
            assert rsp["code"] == 0, rsp
            assert rsp["data"]["snapshotName"] == snapshot_name, rsp
            assert rsp["data"]["collectionName"] == source_collection, rsp
            assert rsp["data"]["description"] == create_payload["description"], rsp
            assert isinstance(rsp["data"]["partitionNames"], list), rsp
            assert rsp["data"]["createTs"] > 0, rsp
            assert rsp["data"]["s3Location"], rsp

            rsp = self.snapshot_client.snapshot_pin({**snapshot_payload, "ttlSeconds": 60})
            assert rsp["code"] == 0, rsp
            pin_id = rsp["data"]["pinId"]
            assert isinstance(pin_id, int) and pin_id > 0, rsp

            rsp = self.snapshot_client.snapshot_unpin({"pinId": pin_id})
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
            assert isinstance(job_id, int) and job_id > 0, rsp

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

            rsp = self.collection_client.collection_has(collection_name=target_collection)
            assert rsp["code"] == 0 and rsp["data"]["has"], rsp

            rsp = self.snapshot_client.snapshot_drop(snapshot_payload)
            assert rsp["code"] == 0, rsp
            snapshot_created = False

            rsp = self.snapshot_client.snapshot_list({"collectionName": source_collection})
            assert rsp["code"] == 0, rsp
            assert snapshot_name not in rsp["data"], rsp
        finally:
            if snapshot_created:
                self.snapshot_client.snapshot_drop(snapshot_payload)
