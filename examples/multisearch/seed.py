"""Populate only owned ms_v2_* collections on the local test instances."""

import argparse, base64, json
from pathlib import Path
import numpy as np
from pymilvus import MilvusClient, DataType
from pymilvus.client.call_context import CallContext

p = argparse.ArgumentParser()
p.add_argument("--port", type=int, required=True)
p.add_argument("--rows", type=int, default=20000)
p.add_argument("--output", default="artifacts/multisearch-v2/fixture.json")
a = p.parse_args()
c = MilvusClient(uri=f"http://localhost:{a.port}", token="root:Milvus")
specs = []
wire = []
for dbi, db in enumerate(["ms_v2_a", "ms_v2_b"]):
    if db not in c.list_databases():
        c.create_database(db)
    d = MilvusClient(uri=f"http://localhost:{a.port}", token="root:Milvus", db_name=db)
    for j in range(4):
        name = f"ms_v2_{j}"
        dim = [128, 192, 256, 128][j]
        metric = ["L2", "IP", "COSINE", "L2"][j]
        rng = np.random.default_rng(3100 + dbi * 4 + j)
        vectors = rng.normal(size=(a.rows, dim)).astype("float32")
        if not d.has_collection(name):
            schema = d.create_schema(auto_id=False, enable_dynamic_field=False)
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field("vector", DataType.FLOAT_VECTOR, dim=dim)
            schema.add_field("category", DataType.INT64)
            d.create_collection(name, schema=schema, consistency_level="Strong")
            for offset in range(0, a.rows, 1000):
                d.insert(
                    name,
                    [
                        {"id": i, "vector": vectors[i].tolist(), "category": i % 10}
                        for i in range(offset, min(offset + 1000, a.rows))
                    ],
                )
            d.flush(name)
            idx = d.prepare_index_params()
            idx.add_index("vector", index_type="FLAT", metric_type=metric)
            d.create_index(name, idx)
        d.load_collection(name)
        spec = {
            "db_name": db,
            "collection_name": name,
            "data": [vectors[0].tolist()],
            "anns_field": "vector",
            "limit": 5 + j,
            "search_params": {"metric_type": metric, "params": {}},
            "output_fields": ["category"],
            "consistency_level": "Strong",
        }
        specs.append(spec)
        for qi in range(16):
            req = d._get_connection()._prepare_search_request(
                collection_name=name,
                anns_field="vector",
                param=spec["search_params"],
                limit=5 + j,
                data=[vectors[qi].tolist()],
                output_fields=["category"],
                consistency_level="Strong",
                context=CallContext(db_name=db),
            )
            req.db_name = db
            wire.append(base64.b64encode(req.SerializeToString()).decode())
        print("loaded", db, name, dim, metric, a.rows, flush=True)
    d.close()
Path(a.output).write_text(
    json.dumps({"specs": specs, "rows_per_collection": a.rows, "requests": wire}, indent=2)
)
c.close()
