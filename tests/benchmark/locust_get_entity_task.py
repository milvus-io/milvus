import logging
import random
from locust import User, task, between
from locust_task import MilvusTask
from client import MilvusClient
from milvus import DataType

connection_type = "single"
host = "192.168.1.6"
port = 19530
collection_name = "sift_10m_100000_128_l2"
dim = 128
m = MilvusClient(host=host, port=port, collection_name=collection_name)
# m.clean_db()
# m.create_collection(dim, data_type=DataType.FLOAT_VECTOR, auto_id=True, other_fields=None)
nb = 6000
# vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
# entities = m.generate_entities(vectors)
ids = [i for i in range(nb)]

class GetEntityTask(User):
    wait_time = between(0.001, 0.002)
    if connection_type == "single":
        client = MilvusTask(m=m)
    else:
        client = MilvusTask(host=host, port=port, collection_name=collection_name)

    # def insert(self):
    #     self.client.insert(entities)

    @task(1)
    def get_entity_by_id(self):
        # num = random.randint(100, 200)
        # get_ids = random.sample(ids, num)
        self.client.get_entities([0])
        # logging.getLogger().info(len(get_res))
