import random
from locust import User, task, between
from locust_task import MilvusTask
from utils import *
from constants import *

url = "http://192.168.1.238:19121/"
collection_name = "test"
headers = {'Content-Type': "application/json"}


class HttpTest(User):
    wait_time = between(0, 0.1)
    client = MilvusTask(url)
    # client.clear_db()
    # client.create_collection(collection_name, default_fields)

    @task
    def insert(self):
        response = self.client.insert(collection_name, default_entities)

    # @task
    # def search(self):
    #     response = self.client.search(collection_name, query_expr, fields)
    #     print(response) 
