import time
import random
from locust import Locust, TaskSet, events, task, between
from client import MilvusClient
from . import BasicRunner


dim = 128
top_k = 10
X = [[random.random() for i in range(dim)] for i in range(1)]
search_param = {"nprobe": 16}


class MilvusTask(object):
    def __init__(self, type="single", args):
        self.type = type
        self.m = None
        if type == "single":
            self.m = MilvusClient(host=args["host"], port=args["port"], collection_name=args["collection_name"])
        elif type == "multi":
            self.m = MilvusClient(host=args["m"])

    def query(self, *args, **kwargs):
        name = "milvus_search"
        request_type = "grpc"
        start_time = time.time()
        try:
            # result = self.m.getattr(*args, **kwargs)
            status, result = self.m.query(*args, **kwargs)
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request_failure.fire(request_type=request_type, name=name, response_time=total_time, exception=e, response_length=0)
        else:
            if not status.OK:
                total_time = int((time.time() - start_time) * 1000)
                events.request_failure.fire(request_type=request_type, name=name, response_time=total_time, exception=e, response_length=0)
            else:
                total_time = int((time.time() - start_time) * 1000)
                events.request_success.fire(request_type=request_type, name=name, response_time=total_time, response_length=0)
                # In this example, I've hardcoded response_length=0. If we would want the response length to be
                # reported correctly in the statistics, we would probably need to hook in at a lower level


class MilvusLocust(Locust):
    def __init__(self, *args, **kwargs):
        super(MilvusLocust, self).__init__(*args, **kwargs)
        self.client = MilvusTask(self.host, self.port, self.collection_name)


class Query(MilvusLocust):
    host = "192.168.1.183"
    port = 19530
    collection_name = "sift_128_euclidean"
    # m = MilvusClient(host=host, port=port, collection_name=collection_name)
    wait_time = between(0.001, 0.002)

    class task_set(TaskSet):
        @task
        def query(self):
            self.client.query(X, top_k, search_param)


class LocustRunner(BasicRunner):
    """Only one client, not support M/S mode"""
    def __init__(self, args):
        # Start client with params including client number && last time && hatch rate ...
        pass

    def set_up(self):
        # helm install locust client
        pass

    def tear_down(self):
        # helm uninstall
        pass
