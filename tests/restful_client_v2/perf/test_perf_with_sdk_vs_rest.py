import time
import random
from pymilvus import (
    connections,
    Collection,
    AnnSearchRequest, RRFRanker
)
import requests
import json
import argparse
from loguru import logger
from faker import Faker

fake = Faker()

def main(host="127.0.0.1"):
    connections.connect(
        host=host,
        port=19530,
    )
    collection = Collection(name="test_restful_perf")
    vector_to_search = [
        [random.random() for _ in range(768)] for _ in range(1000)
    ]
    search_params = {"metric_type": "L2", "params": {"ef": 150}}
    nb = 1000
    insert_data = [
        {"id": random.randint(0, 1000),
         "doc_id": random.randint(0, 1000),
         "text_no_index": fake.text(),
         "text": fake.text(),
         "text_emb": [random.random() for _ in range(768)],
         "image_emb": [random.random() for _ in range(768)]}
        for _ in range(nb)]

    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer root:Milvus'
    }

    for op in ["search", "hybrid_search", "query_id", "query_varchar", "insert"]:
        time_list_sdk = []
        time_list_restful = []
        logger.info("start sdk test")
        for i in range(100):
            random_id = random.randint(0, 1000 - 1)
            t0 = time.time()
            logger.info(f"{op}...")
            if op == "search":
                res = collection.search([vector_to_search[random_id]], "text_emb", search_params, 100, output_fields=["*"])
            elif op == "hybrid_search":
                sq1=AnnSearchRequest([vector_to_search[random_id]], "text_emb", search_params, 100)
                sq2=AnnSearchRequest([vector_to_search[random_id]], "image_emb", search_params, 100)
                res = collection.hybrid_search([sq1, sq2], RRFRanker(60), 100, output_fields=["*"])
            elif op == "query_id":
                res = collection.query(expr=f"id in {[x for x in range(100)]}", output_fields=["*"], limit=100)
            elif op == "query_varchar":
                res = collection.query(expr='text like "9999%"', output_fields=["*"], limit=100)
            elif op == "insert":
                insert_collection = Collection(name="test_restful_insert_perf")
                res = insert_collection.insert(data=insert_data)
            else:
                raise Exception(f"unsupported op {op}")
            t1 = time.time()
            tt = t1 - t0
            time_list_sdk.append(tt)
            logger.info(f"{op} cost  {tt:.4f} seconds with response {res}...")

        logger.info("start restful test")
        path = op
        if "query" in op:
            path = "query"

        url = f"http://{host}:19530/v2/vectordb/entities/{path}"
        logger.info(f"{op}...")
        for i in range(100):
            t0 = time.time()
            if op == "search":
                payload = json.dumps({"collectionName": "test_restful_perf",
                                      "outputFields": ["*"],
                                      "annsField": "text_emb",
                                      "data": [random.random() for _ in range(768)],
                                      "limit": 100,
                                      })

                response = requests.request("POST", url, headers=headers, data=payload)
            elif op == "hybrid_search":
                payload = json.dumps({"collectionName": "test_restful_perf",
                                    "search": [
                                        {
                                            "data": [random.random() for _ in range(768)],
                                            "annsField": "text_emb",
                                        },
                                        {
                                            "data": [random.random() for _ in range(768)],
                                            "annsField": "image_emb",
                                        },
                                    ],
                                    "rerank": {
                                        "strategy": "rrf",
                                        "params": {
                                            "k": 60,
                                        }
                                    },
                                    "limit": 100,
                                    "outputFields": ["*"]
                                    })

                response = requests.request("POST", url, headers=headers, data=payload)
            elif op == "query_id":
                payload = json.dumps({"collectionName": "test_restful_perf",
                                      "outputFields": ["*"],
                                      "filter": f"id in {[x for x in range(100)]}",
                                      })
                response = requests.request("POST", url, headers=headers, data=payload)
            elif op == "query_varchar":
                payload = json.dumps({"collectionName": "test_restful_perf",
                                      "outputFields": ["*"],
                                      "filter": 'text like "9999%"',
                                      })
                response = requests.request("POST", url, headers=headers, data=payload)
            elif op == "insert":
                payload = json.dumps({"collectionName": "test_restful_insert_perf",
                                      "data": insert_data})
                response = requests.request("POST", url, headers=headers, data=payload)
            else:
                raise Exception(f"unsupported op {op}")
            assert response.json()["code"] == 200
            t1 = time.time()
            tt = t1 - t0
            time_list_restful.append(tt)
            logger.info(f"{op} cost  {tt:.4f} seconds with response {response.text[:200]}...")

        mean_time_sdk = sum(time_list_sdk) / len(time_list_sdk)
        mean_time_restful = sum(time_list_restful) / len(time_list_restful)
        logger.info(f"[sdk]{op} ave time {mean_time_sdk} , max time {max(time_list_sdk)}, min time {min(time_list_sdk)}")
        logger.info(
            f"[restful]{op} ave time {mean_time_restful} , max time {max(time_list_restful)}, min time {min(time_list_restful)}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="perf test with sdk and restful")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    args = parser.parse_args()
    host = args.host
    main(host=host)
