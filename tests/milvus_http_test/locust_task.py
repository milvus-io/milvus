import logging
import time
import random
from locust import User, events
from client import MilvusClient

url = 'http://192.168.1.238:19121'


class MilvusTask(object):
    def __init__(self, url):
        self.request_type = "http"
        self.m = MilvusClient(url)
        # logging.getLogger().info(id(self.m))

    def __getattr__(self, name):
        func = getattr(self.m, name)

        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                total_time = int((time.time() - start_time) * 1000)
                if result:
                    events.request_success.fire(request_type=self.request_type, name=name, response_time=total_time,
                                                response_length=0)
                else:
                    events.request_failure.fire(request_type=self.request_type, name=name, response_time=total_time,
                                            exception=e, response_length=0)

            except Exception as e:
                total_time = int((time.time() - start_time) * 1000)
                events.request_failure.fire(request_type=self.request_type, name=name, response_time=total_time,
                                            exception=e, response_length=0)

        return wrapper
