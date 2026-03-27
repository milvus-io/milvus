#!/usr/bin/env python3

import json
import os
import sys
import time
import uuid

from pymilvus import DataType, MilvusClient


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None else int(value)


def hit_value(hit, key: str):
    if isinstance(hit, dict):
        return hit.get(key)
    return getattr(hit, key, None)


def main() -> int:
    uri = os.getenv("MILVUS_URI", "http://127.0.0.1:19530")
    token = os.getenv("MILVUS_TOKEN", "")
    collection_name = os.getenv(
        "MILVUS_RS_SMOKE_COLLECTION",
        f"knowhere_rs_smoke_{int(time.time())}_{uuid.uuid4().hex[:8]}",
    )
    keep_collection = os.getenv("MILVUS_RS_SMOKE_KEEP_COLLECTION", "0") == "1"
    dim = env_int("MILVUS_RS_SMOKE_DIM", 4)
    topk = env_int("MILVUS_RS_SMOKE_TOPK", 2)
    build_m = env_int("MILVUS_RS_SMOKE_HNSW_M", 18)
    ef_construction = env_int("MILVUS_RS_SMOKE_HNSW_EF_CONSTRUCTION", 240)
    search_ef = env_int("MILVUS_RS_SMOKE_HNSW_EF", 64)

    if dim != 4:
        raise SystemExit("stage-1 smoke currently uses a fixed 4D fixture")

    fixture_vectors = [
        [1.0, 0.0, 0.0, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0],
        [0.0, 0.0, 0.0, 1.0],
    ]
    rows = [
        {"id": idx, "tag": f"row-{idx}", "vector": vector}
        for idx, vector in enumerate(fixture_vectors)
    ]
    query = [fixture_vectors[0]]

    client = MilvusClient(uri=uri, token=token)
    try:
        if client.has_collection(collection_name):
            client.drop_collection(collection_name)

        schema = client.create_schema()
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("tag", DataType.VARCHAR, max_length=32)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=dim)
        client.create_collection(collection_name=collection_name, schema=schema)

        insert_result = client.insert(collection_name, rows)

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="vector",
            index_type="HNSW",
            metric_type="COSINE",
            M=build_m,
            efConstruction=ef_construction,
        )
        client.create_index(collection_name, index_params)
        client.load_collection(collection_name)

        results = client.search(
            collection_name,
            query,
            anns_field="vector",
            limit=topk,
            output_fields=["tag"],
            search_params={"metric_type": "COSINE", "params": {"ef": search_ef}},
        )

        if len(results) != 1:
            raise SystemExit(f"expected 1 query result set, got {len(results)}")
        if len(results[0]) != topk:
            raise SystemExit(f"expected topk={topk}, got {len(results[0])}")

        top_hit = results[0][0]
        top_id = hit_value(top_hit, "id")
        if top_id != 0:
            raise SystemExit(f"expected nearest id 0, got {top_id!r}")

        summary = {
            "collection_name": collection_name,
            "index_type": "HNSW",
            "insert_result": insert_result,
            "query_count": len(query),
            "topk": topk,
            "top_hit_id": top_id,
            "top_hit_distance": hit_value(top_hit, "distance"),
            "results": results,
        }
        print(json.dumps(summary, indent=2, sort_keys=True, default=str))
        return 0
    finally:
        try:
            if not keep_collection and client.has_collection(collection_name):
                client.drop_collection(collection_name)
        finally:
            client.close()


if __name__ == "__main__":
    sys.exit(main())
