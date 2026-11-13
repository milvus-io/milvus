import time

import pytest
import requests
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name


@pytest.mark.tags(CaseLabel.L1)
class TestCompactionTargets(TestBase):
    connect_with_pymilvus = False

    def target_request(self, action, payload):
        response = requests.post(
            f"{self.endpoint}/v2/vectordb/compaction_targets/{action}",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json=payload,
            timeout=20,
        )
        assert response.status_code == 200, response.text
        return response.json()

    @pytest.fixture(autouse=True)
    def require_tbc(self, init_client):
        result = self.target_request("list", {"collectionName": gen_collection_name()})
        if result["code"] == 3000 and "target-based compaction is disabled" in result["message"]:
            pytest.skip("requires target-based compaction")
        assert result["code"] == 0, result

    def test_rewrite_flushed_data(self):
        name = gen_collection_name()
        result = self.collection_client.collection_create({"collectionName": name, "dimension": 4})
        assert result["code"] == 0, result
        result = self.vector_client.vector_insert(
            {
                "collectionName": name,
                "data": [{"id": row, "vector": [float(row), 1.0, 0.0, 1.0]} for row in range(1024)],
            }
        )
        assert result["code"] == 0, result
        result = self.collection_client.flush(name)
        assert result["code"] == 0, result

        snapshot = {"collectionName": name, "snapshotName": "protect_rewrite_input"}
        protected = self.snapshot_client.snapshot_create({**snapshot, "compactionProtectionSeconds": 60})
        assert protected["code"] == 0, protected
        try:
            created = self.target_request("create", {"collectionName": name, "rewrite": {}})
            assert created["code"] == 0, created
            target_id = created["data"]["targetId"]
            assert isinstance(target_id, str) and int(target_id) > 0
            assert created["data"]["totalRows"] == "1024"
            assert "optimize" not in created["data"]

            while_protected = self.target_request("describe", {"targetId": target_id})
            assert while_protected["code"] == 0, while_protected
            assert while_protected["data"]["state"] == "COMPACTION_TARGET_STATE_ACTIVE"
            assert while_protected["data"]["remainingRows"] == "1024"
        finally:
            dropped_snapshot = self.snapshot_client.snapshot_drop(snapshot)
            assert dropped_snapshot["code"] == 0, dropped_snapshot

        deadline = time.monotonic() + 180
        while True:
            result = self.target_request("describe", {"targetId": target_id})
            assert result["code"] == 0, result
            target = result["data"]
            if target["state"] == "COMPACTION_TARGET_STATE_INACTIVE":
                break
            assert time.monotonic() < deadline, target
            time.sleep(1)
        assert target["stopReason"] == "satisfied"
        assert target["remainingRows"] == "0"
        assert int(target["checkedAt"]) > 0

        listed = self.target_request("list", {"collectionName": name})
        assert listed["code"] == 0, listed
        assert [item["targetId"] for item in listed["data"]] == [target_id]
        assert "remainingRows" not in listed["data"][0]

        for _ in range(2):
            dropped = self.target_request("drop", {"targetId": target_id})
            assert dropped["code"] == 0, dropped
        final = self.target_request("describe", {"targetId": target_id})
        assert final["code"] == 0, final
        assert final["data"]["stopReason"] == "satisfied"
        assert final["data"]["stoppedAt"] == target["stoppedAt"]

    @pytest.mark.parametrize(
        "extra", [{}, {"optimize": {"inputTargetSizeBytes": "1"}, "rewrite": {}}, {"rewrite": {"partitionId": "1"}}]
    )
    def test_create_rejects_invalid_shape(self, extra):
        result = self.target_request("create", {"collectionName": gen_collection_name(), **extra})
        assert result["code"] != 0, result
        assert "targetId" not in result.get("data", {})
