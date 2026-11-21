"""Submit a typed source-history object to the real legacy L0 import route."""

import json
import sys
import time
import urllib.request
from pathlib import Path

from acceptance import Fixture
from pymilvus import DataType


def check(f, path):
    schema = f.client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=2)
    f.client.create_collection(f.candidate, schema=schema, num_shards=1, timeout=60)

    def request(action, body):
        endpoint = f"http://{f.endpoint}/v2/vectordb/jobs/import/{action}"
        req = urllib.request.Request(endpoint, json.dumps(body).encode(), {"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.load(resp)

    f.put_config("dataCoord.import.enableL0Import", True)
    try:
        for _ in range(45):
            result = request(
                "create", {"collectionName": f.candidate, "files": [[path]], "options": {"l0_import": "true"}}
            )
            if "l0 import is disabled" not in result.get("message", ""):
                break
            time.sleep(1)
        f.record("import submit", result)
        assert result["code"] == 0, result
        job = result["data"]["jobId"]
        for _ in range(120):
            result = request("describe", {"jobId": job})
            if result.get("data", {}).get("state") in ("Failed", "Completed"):
                break
            time.sleep(1)
        f.record("import terminal", result)
        assert result["data"]["state"] == "Failed" and "typed" in str(result["data"]).lower()
        indexes = f.client.prepare_index_params()
        indexes.add_index("vec", index_type="FLAT", metric_type="L2")
        f.client.create_index(f.candidate, indexes, timeout=90)
        f.client.load_collection(f.candidate, timeout=90)
        rows = f.client.query(
            f.candidate, filter="id >= 0", output_fields=["id"], consistency_level="Strong", timeout=30
        )
        assert rows == []
    finally:
        f.put_config("dataCoord.import.enableL0Import", False)


if __name__ == "__main__":
    check(Fixture(Path(sys.argv[1]).resolve()), sys.argv[2])
