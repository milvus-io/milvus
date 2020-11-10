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

# if m.exists_collection(collection_name):
#     m.drop(name=collection_name)
# m.create_collection(collection_name, dim, 512, "l2")


class QueryTask(User):
    wait_time = between(0.001, 0.002)
    print("in query task")
    if connection_type == "single":
        client = MilvusTask(m=m)
    else:
        client = MilvusTask(host=host, port=port, collection_name=collection_name)

    ids = [random.randint(0, 1000000000) for i in range(1)]
    X = [[random.random() for i in range(dim)] for i in range(1)]

    # @task(1)
    # def test_partition(self):
    #     tag_name = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))
    #     self.client.create_partition(tag_name)
    #     # self.client.insert(self.X, ids=self.ids, tag=tag_name)
    #     # count = self.count(collection_name)
    #     # logging.info(count)

    @task(2)
    def test_count(self):
        m.count(collection_name)

    @task(1)
    def test_drop(self):
        tags = m.list_partitions()
        tag = random.choice(tags)
        if tag.tag != "_default":
            self.client.drop_partition(tag.tag)
