import time
import random
import pytest
from datetime import datetime, timezone, timedelta

from base.testbase import TestBase
from utils.utils import gen_collection_name
from utils.util_log import test_log as logger


@pytest.mark.L1
class TestTimestamptz(TestBase):
    """
    RESTful e2e coverage for timestamptz field:
    - create collection with default timestamptz
    - describe schema to confirm defaultValue
    - insert rows (one missing timestamptz -> use default)
    - flush + load
    - get entities and validate timestamptz values are preserved/defaulted
    """

    def test_timestamptz_default_value_and_get(self):
        name = gen_collection_name()
        dim = 5
        default_time = "2025-01-01T00:00:00Z"

        # 1. create collection with timestamptz default value and vector index
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "time", "dataType": "Timestamptz", "defaultValue": default_time, "nullable": True},
                    {"fieldName": "color", "dataType": "VarChar", "elementTypeParams": {"max_length": "30"}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ],
            },
            "indexParams": [
                {"fieldName": "vector", "indexName": "vector_index", "metricType": "L2"},
            ],
        }
        logger.info(f"create collection {name} with payload: {payload}")
        rsp = self.collection_client.collection_create(payload)
        assert rsp["code"] == 0
        self.wait_load_completed(name)

        # 2. describe collection and verify defaultValue is returned
        desc = self.collection_client.collection_describe(name)
        assert desc["code"] == 0
        fields = desc.get("data", {}).get("fields", [])
        time_field = next(
            (f for f in fields if f.get("fieldName") == "time" or f.get("name") == "time"),
            None,
        )
        assert time_field is not None, f"timestamptz field not found in describe: {desc}"
        # defaultValue is returned in protobuf-like structure, keep loose check on the string payload
        assert "defaultValue" in time_field

        # 3. insert rows (one row omits timestamptz to trigger default)
        now_utc = datetime.now(timezone.utc)
        one_hour_ago = now_utc - timedelta(hours=1)
        rows = [
            {"id": 1, "time": now_utc.isoformat(), "color": "red_9392", "vector": [random.random() for _ in range(dim)]},
            {"id": 3, "time": one_hour_ago.isoformat(), "color": "pink_9298", "vector": [random.random() for _ in range(dim)]},
            {"id": 4, "color": "green_0004", "vector": [random.random() for _ in range(dim)]},  # default timestamptz
            {"id": 504, "time": one_hour_ago.isoformat(), "color": "blue_0000", "vector": [random.random() for _ in range(dim)]},
        ]
        insert_payload = {"collectionName": name, "data": rows}
        insert_rsp = self.vector_client.vector_insert(insert_payload)
        assert insert_rsp["code"] == 0
        assert insert_rsp["data"]["insertCount"] == len(rows)

        # 4. flush and load collection to make data queryable
        flush_rsp = self.collection_client.flush(name)
        assert flush_rsp["code"] == 0
        load_rsp = self.collection_client.collection_load(collection_name=name)
        assert load_rsp["code"] == 0
        # wait a moment for load state
        time.sleep(2)

        # 5. get entities by id and validate timestamptz values
        get_payload = {
            "collectionName": name,
            "id": [1, 3, 4],
            "outputFields": ["color", "time"],
        }
        get_rsp = self.vector_client.vector_get(get_payload)
        assert get_rsp["code"] == 0
        result = {int(item["id"]): item for item in get_rsp["data"]}
        assert set(result.keys()) == {1, 3, 4}

        def to_dt(ts: str) -> datetime:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))

        # default applied for id=4
        assert to_dt(result[4]["time"]) == to_dt(default_time)
        # provided values preserved (allow small drift if server trims precision)
        assert abs((to_dt(result[1]["time"]) - now_utc).total_seconds()) < 1
        assert abs((to_dt(result[3]["time"]) - one_hour_ago).total_seconds()) < 1
        # colors round-trip
        assert result[1]["color"] == "red_9392"
        assert result[3]["color"] == "pink_9298"
        assert result[4]["color"] == "green_0004"

