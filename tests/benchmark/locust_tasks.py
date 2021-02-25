import random
import time
import logging
from locust import TaskSet, task

dim = 128
X = [[random.random() for _ in range(dim)] for _ in range(1)]


class Tasks(TaskSet):

    @task
    def query(self):
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
        self.client.query(vector_query, filter_query=filter_query, log=False)

    @task
    def flush(self):
        self.client.flush(log=False)

    @task
    def get(self):
        self.client.get()

    @task
    def delete(self):
        self.client.delete([random.randint(1, 1000000)], log=False)

    def insert(self):
        ids = [random.randint(1, 10000000)]
        entities = self.client.generate_entities(X, ids)
        self.client.insert(entities, ids, log=False)

    @task
    def insert_rand(self):
        self.client.insert_rand(log=False)
