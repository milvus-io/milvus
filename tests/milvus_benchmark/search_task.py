import random
import logging
from locust import User, task, between
from locust_task import MilvusTask
from client import MilvusClient
from milvus import DataType
import utils

connection_type = "single"
host = "192.168.1.112"
port = 19530
collection_name = "sift_10m_100000_128_l2"
dim = 128
segment_row_limit = 10000
m = MilvusClient(host=host, port=port, collection_name=collection_name)
# m.clean_db()
# m.create_collection(dim, segment_row_limit, data_type=DataType.FLOAT_VECTOR, auto_id=True, other_fields=None)
vectors = [[random.random() for _ in range(dim)] for _ in range(1000)]
entities = m.generate_entities(vectors)
ids = [i for i in range(10000000)]

class QueryTask(User):
    wait_time = between(0.001, 0.002)
    if connection_type == "single":
        client = MilvusTask(m=m)
    else:
        client = MilvusTask(host=host, port=port, collection_name=collection_name)

    # def query(self):
    #     top_k = 10
    #     X = [[random.random() for i in range(dim)] for i in range(1)]
    #     search_param = {"nprobe": 16}
    #     self.client.query(X, top_k, search_param)


    @task(1)
    def insert(self):
        self.client.insert(entities)

    # @task(1)
    # def create(self):
    #     collection_name = utils.get_unique_name(prefix="locust")
    #     self.client.create_collection(dim, segment_row_limit, data_type=DataType.FLOAT_VECTOR, auto_id=True, collection_name=collection_name, other_fields=None)

    # @task(1)
    # def delete(self):
    #     delete_ids = random.sample(ids, 100)
    #     logging.error(delete_ids)
    #     self.client.delete(delete_ids)