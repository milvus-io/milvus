"""Real-server quota and request-size checks against verify.py's dataset."""
import json
import time
from pathlib import Path

from pymilvus import MilvusClient
from pymilvus.client.prepare import Prepare

from client import MultiSearchClient


def main():
    root = Path("artifacts/multisearch")
    specs = json.loads((root / "results.json").read_text())["collections"]
    admin = MilvusClient(uri="http://localhost:29550", token="root:Milvus")
    client = MultiSearchClient()
    evidence = []

    def request(i):
        s = specs[i]
        return Prepare.search_requests_with_expr(
            collection_name=s["name"], anns_field=s["vector_field"],
            param={"metric_type": s["metric"], "params": {}}, limit=3, data=[s["query"]],
            use_default_consistency=False, consistency_level=0,
        )

    def record(name, detail):
        evidence.append(dict(name=name, passed=True, detail=detail))
        (root / "limits-results.json").write_text(json.dumps(evidence, indent=2))
        print(f"PASS {name}: {detail}", flush=True)

    try:
        oversized = request(0)
        oversized.dsl = "x" * (4 << 20)
        resp = client.multi_search([("oversized", oversized)])
        assert resp.status.code == 1102 and not resp.items
        record("Request byte limit", {"code": resp.status.code, "bytes": oversized.ByteSize()})

        different_db = request(0)
        different_db.db_name = "not_the_envelope_database"
        resp = client.multi_search([("cross-database", different_db)])
        assert resp.status.code == 1100 and not resp.items
        record("Single-database envelope", {"code": resp.status.code})

        admin.alter_collection_properties(specs[0]["name"], {"collection.searchRate.max.vps": "0"})
        end = time.monotonic() + 30
        while True:
            ordinary = client.search(request(0))
            if ordinary.status.code != 0:
                break
            assert time.monotonic() < end, "collection quota did not propagate"
            time.sleep(0.5)
        assert ordinary.status.code in (8, 9), str(ordinary.status)
        batch = client.multi_search([("limited", request(0)), ("unlimited", request(1))])
        assert batch.status.code == 0
        assert batch.items[0].result.status == ordinary.status
        assert batch.items[1].result.status.code == 0
        record("Real collection quota isolation", {"ordinary_code": ordinary.status.code,
               "multisearch_item_code": batch.items[0].result.status.code,
               "other_item_code": batch.items[1].result.status.code,
               "retriable": batch.items[0].result.status.retriable})
    finally:
        admin.alter_collection_properties(specs[0]["name"], {"collection.searchRate.max.vps": "-1"})
        client.close()
        admin.close()


if __name__ == "__main__":
    main()
