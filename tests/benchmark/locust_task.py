import time
import pdb
import random
import logging
from locust import User, events
from client import MilvusClient


class MilvusTask(object):
    def __init__(self, *args, **kwargs):
        self.request_type = "grpc"
        connection_type = kwargs.get("connection_type")
        if connection_type == "single":
            self.m = kwargs.get("m")
        elif connection_type == "multi":
            host = kwargs.get("host")
            port = kwargs.get("port")
            collection_name = kwargs.get("collection_name")
            self.m = MilvusClient(host=host, port=port, collection_name=collection_name)
        # logging.getLogger().error(id(self.m))

    def __getattr__(self, name):
        func = getattr(self.m, name)

        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                total_time = int((time.time() - start_time) * 1000)
                events.request_success.fire(request_type=self.request_type, name=name, response_time=total_time,
                                            response_length=0)
            except Exception as e:
                total_time = int((time.time() - start_time) * 1000)
                events.request_failure.fire(request_type=self.request_type, name=name, response_time=total_time,
                                            exception=e, response_length=0)

        return wrapper
