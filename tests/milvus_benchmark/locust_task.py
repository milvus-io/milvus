import time
from locust import events
from client import MilvusClient


class MilvusTask(object):
    """
    """
    def __init__(self, connection_type="single", **kwargs):
        """Generate milvus client for locust.
        
        To make sure we can use the same function name in client as task name in Taskset/User.
        Params: connection_type, single/multi is optional
        other args: host/port/collection_name
        """
        self.request_type = "grpc"
        if connection_type == "single":
            self.m = kwargs.get("m")
        elif connection_type == "multi":
            host = kwargs.get("host")
            port = kwargs.get("port")
            collection_name = kwargs.get("collection_name")
            self.m = MilvusClient(host=host, port=port, collection_name=collection_name)

    def __getattr__(self, name):
        """Register success and failure event with using locust.events.

        Make sure the task function name in locust equals to te name of function in MilvusClient
        """
        func = getattr(self.m, name)

        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                _ = func(*args, **kwargs)
                total_time = int((time.time() - start_time) * 1000)
                events.request_success.fire(request_type=self.request_type, name=name, response_time=total_time,
                                            response_length=0)

            except Exception as e:
                total_time = int((time.time() - start_time) * 1000)
                events.request_failure.fire(request_type=self.request_type, name=name, response_time=total_time,
                                            exception=e, response_length=0)

        return wrapper
