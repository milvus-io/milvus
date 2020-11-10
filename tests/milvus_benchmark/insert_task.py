import pdb
import random, string, logging
from locust import User, task, between
from locust_task import MilvusTask
from client import MilvusClient

connection_type = "single"
host = "192.168.1.6"
port = 19530
collection_name = "sift_128_euclidean"
dim = 128
m = MilvusClient(host=host, port=port, collection_name=collection_name)

if m.exists_collection(collection_name):
    m.drop(name=collection_name)
m.create_collection(collection_name, dim, 32, "l2")


class QueryTask(User):
    wait_time = between(0.001, 0.002)
    print("in query task")
    if connection_type == "single":
        client = MilvusTask(m=m)
    else:
        client = MilvusTask(host=host, port=port, collection_name=collection_name)

    @task(2)
    def insert(self):
        id = random.randint(10000000, 10000000000)
        X = [[random.random() for i in range(dim)] for i in range(1)]
        self.client.insert(X, ids=[id])

    @task(2)
    def delete(self):
        self.client.delete([random.randint(1, 1000000)])

    @task(1)
    def flush(self):
        self.client.flush()
