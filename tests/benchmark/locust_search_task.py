import random
from client import MilvusClient
from locust_task import MilvusTask
from locust import User, task, between

connection_type = "single"
host = "172.16.50.9"
port = 19530
collection_name = "sift_1m_2000000_128_l2_2"
m = MilvusClient(host=host, port=port, collection_name=collection_name)
dim = 128
top_k = 5
nq = 1
X = [[random.random() for i in range(dim)] for i in range(nq)]
search_params = {"nprobe": 16}
vector_query = {"vector": {'float_vector': {
    "topk": top_k,
    "query": X,
    "params": search_params,
    'metric_type': 'L2'}}}
# m.clean_db()


class QueryTask(User):
    wait_time = between(0.001, 0.002)

    def preload(self):
        self.client.preload_collection()

    @task(10)
    def query(self):
        if connection_type == "single":
            client = MilvusTask(m=m, connection_type=connection_type)
        elif connection_type == "multi":
            client = MilvusTask(host, port, collection_name, connection_type=connection_type)
        top_k = 10
        search_param = {"nprobe": 16}
        X = [[random.random() for i in range(dim)]]
        vector_query = {"vector": {"float_vector": {
            "topk": top_k,
            "query": X,
            "metric_type": "L2",
            "params": search_param}
        }}
        filter_query = None
        client.query(vector_query, filter_query=filter_query, collection_name=collection_name)