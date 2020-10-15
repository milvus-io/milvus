import pdb
import random
from locust import User, task, between
from locust_task import MilvusTask
from utils import *
from constants import *

url = "http://192.168.1.29:19121/"
collection_name = "sift_128_euclidean"
headers = {'Content-Type': "application/json"}

default_query, default_query_vectors = gen_query_vectors(default_float_vec_field_name, default_entities, default_top_k, default_nq)


class HttpTest(User):
    wait_time = between(0, 0.1)
    client = MilvusTask(url)
    # client.clear_db()
    # client.create_collection(collection_name, default_fields)

    # @task
    # def insert(self):
    #     response = self.client.insert(collection_name, default_entities)

    @task
    def search(self):
        response = self.client.search(collection_name, default_query)
        # res = response['result']
        # assert response['num'] == default_nq
        # assert len(res) == default_nq
        # assert len(res[0]) == default_top_k
