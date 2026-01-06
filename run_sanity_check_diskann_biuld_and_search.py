from pymilvus import (
    connections, FieldSchema, CollectionSchema, DataType, Collection, utility
)
import random, math
import time
import matplotlib.pyplot as plt
import json
import sys
import os

# ---------- CONFIG ----------
HOST = "127.0.0.1"   # or your Minikube/cluster IP
PORT = "19530"
COLLECTION = "demo_vectors"
METRIC = "L2"

# ---------- CONNECT ----------
connections.connect("default", host=HOST, port=PORT)
print("✅ Connected to Milvus", utility.get_server_version())


results_json = []

for INDEX_TYPE in ["DISKANN"]:
    for DIM in [8, 64, 512]:
        for N in [2000, 100_000]:
            # Clean up old demo if it exists
            if utility.has_collection(COLLECTION):
                utility.drop_collection(COLLECTION)

            # ---------- DEFINE SCHEMA ----------
            id_field = FieldSchema("id", DataType.INT64, is_primary=True, auto_id=False)
            vec_field = FieldSchema("embedding", DataType.FLOAT_VECTOR, dim=DIM)
            schema = CollectionSchema(fields=[id_field, vec_field], description="demo collection")

            coll = Collection(COLLECTION, schema)

            # ---------- INSERT DATA ----------
            def make_vec(dim=DIM):
                return [random.random() for _ in range(dim)]

            ids = list(range(N))
            vectors = [make_vec() for _ in range(N)]
            for i in range(N//1000):
                coll.insert([ids[i:i+1000], vectors[i:i+1000]])
            coll.flush()

            # ---------- CREATE INDEX ----------
            build_start = time.time()
            coll.create_index(
                field_name="embedding",
                index_params={"index_type": INDEX_TYPE, "metric_type": METRIC, "params": {"nlist": 64}},
            )
            coll.load()
            build_end = time.time()
            build_latency = build_end - build_start

            # ---------- SEARCH ----------
            query_vec = [make_vec()]
            search_start = time.time()
            results = coll.search(
                data=query_vec,
                anns_field="embedding",
                param={"metric_type": METRIC, "params": {"nprobe": 8}},
                limit=5,
                output_fields=["id"]
            )
            search_end = time.time()
            search_latency = search_end - search_start

            print(f"INDEX_TYPE={INDEX_TYPE}, D={DIM}, N={N}, build_latency = {build_latency:.4f}, search_latency = {search_latency:.4f}")
            print("\nTop-5 nearest vectors:")
            search_results = []
            for hit in results[0]:
                print(f"  id={hit.id}, distance={hit.distance:.4f}")
                search_results.append({"id": hit.id, "distance": hit.distance})

            results_json.append({
                "INDEX_TYPE": INDEX_TYPE,
                "DIM": DIM,
                "N": N,
                "build_latency": build_latency,
                "search_latency": search_latency,
                "search_results": search_results
            })

            # ---------- OPTIONAL CLEANUP ----------
            coll.release()

# Get suffix from command-line argument (default: "")
suffix = ""
if len(sys.argv) > 1:
    suffix = sys.argv[1]
    if not suffix.startswith("_") and suffix != "":
        suffix = "_" + suffix

results_folder = "milvus_benchmark_results"
os.makedirs(results_folder, exist_ok=True)
results_filename = f"{results_folder}/milvus_benchmark_results{suffix}.json"
with open(results_filename, "w") as f:
    json.dump(results_json, f, indent=2)
