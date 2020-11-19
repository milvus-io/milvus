import random
import time
import logging
from locust import TaskSet, task

dim = 128


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
        self.client.query(vector_query, filter_query=filter_query)

    @task
    def flush(self):
        self.client.flush()

    @task
    def compact(self):
        self.client.compact()

    @task
    def delete(self):
        self.client.delete([random.randint(1, 1000000)])

    @task
    def insert(self):
        id = random.randint(10000000, 10000000000)
        X = [[random.random() for i in range(dim)] for i in range(1)]
        self.client.insert(X, ids=[id])
