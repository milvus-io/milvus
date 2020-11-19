import logging
import random
from locust import User, task, between
from locust_task import MilvusTask
from client import MilvusClient

connection_type = "single"
host = "192.168.1.6"
port = 19530
collection_name = "sift_1m_2000000_128_l2_2"
m = MilvusClient(host=host, port=port, collection_name=collection_name)
dim = 128
top_k = 10
nq = 1
X = [[random.random() for i in range(dim)] for i in range(nq)]
search_params = {"nprobe": 32}
vector_query = {"vector": {'float_vector': {
    "topk": top_k,
    "query": X,
    "params": search_params,
    'metric_type': 'L2'}}}
# m.clean_db()


class QueryTask(User):
    wait_time = between(0.001, 0.002)
    if connection_type == "single":
        client = MilvusTask(m=m)
    else:
        client = MilvusTask(host=host, port=port, collection_name=collection_name)
    # client = MilvusClient(collection_name, host, port)

    def preload(self):
        self.client.preload_collection()

    @task(10)
    def query(self):
        self.client.query(vector_query, collection_name=collection_name)