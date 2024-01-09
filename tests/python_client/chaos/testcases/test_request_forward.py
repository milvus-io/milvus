import argparse
import threading
import time
import queue
import random
import numpy as np
from pymilvus import (
    connections,
    FieldSchema, CollectionSchema, DataType,
    Collection
)
from loguru import logger as log
import uuid

enable_mock = False


class Consumer(threading.Thread):
    def __init__(self, q):
        super().__init__()
        self.q = q

    def mock_task(self, data=None):
        log.info(f"mock task {len(data)}")


class InsertWorker(Consumer):
    def __init__(self, q, c_name, host_name):
        super().__init__(q)
        self.q = q
        self.c_name = c_name
        self.host_name = host_name

    def task(self, data=None):
        c_name = self.c_name
        host_name = self.host_name
        collection = Collection(name=c_name, using=host_name)
        res = collection.insert(data)
        log.info(f"{host_name} {c_name} insert data: {len(data[0])} {res}")

    def run(self):
        while True:
            item = self.q.get()
            if item is None:
                log.info(f"host {self.host_name} collection {self.c_name} insert worker consume finished")
                break
            data = item["data"]
            uid = item["uid"]
            if enable_mock:
                self.mock_task(data=data)
            else:
                self.task(data=data)
            log.info(f"host: {self.host_name} collection: {self.c_name} insert worker consume task {uid}")
            self.q.task_done()


class DeleteWorker(Consumer):
    def __init__(self, q, c_name, host_name):
        super().__init__(q)
        self.q = q
        self.c_name = c_name
        self.host_name = host_name

    def task(self, data=None):
        c_name = self.c_name
        host_name = self.host_name
        collection = Collection(name=c_name, using=host_name)
        expr = f'int64 in {data}'
        res = collection.delete(expr=expr)
        log.info(f"host:{host_name} collection: {c_name} delete data: {len(data)} {res}")

    def run(self):
        while True:
            item = self.q.get()
            if item is None:
                log.info(f"host {self.host_name} collection {self.c_name} delete worker consume finished")
                break
            data = item["data"]
            uid = item["uid"]
            if enable_mock:
                self.mock_task(data=data)
            else:
                self.task(data=data)
            log.info(f"host {self.host_name} collection {self.c_name} delete worker consume task {uid}")
            self.q.task_done()


class InsertTaskProducer(threading.Thread):
    def __init__(self, q_list, nb=3000, request_duration=300):
        super().__init__()
        self.q_list = q_list
        self.nb = nb
        self.request_duration = request_duration

    def task(self):
        nb = self.nb
        dim = 128
        vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
        pk_value = []
        for i in range(nb):
            ts = time.time() * 100000
            pk_value.append(int(ts))
            time.sleep(0.001)
        data = [
            pk_value,
            [np.float32(i) for i in range(nb)],
            [str(i) for i in range(nb)],
            vectors
        ]
        uid = uuid.uuid4()
        for q in self.q_list:
            q.put({"uid": uid, "op": "insert", "data": data})
        log.info(f"insert task produce task {uid}")

    def mock_task(self):
        data = [i for i in range(100)]
        uid = uuid.uuid4()
        for q in self.q_list:
            q.put({"uid": uid, "op": "insert", "data": data})
            log.info(f"insert task produce task {uid}")

    def run(self):
        t0 = time.time()
        while time.time() - t0 < self.request_duration:
            if enable_mock:
                self.mock_task()
            else:
                self.task()
            time.sleep(1)
        for q in self.q_list:
            q.put(None)
        log.info("insert task produce finished")


class DeleteTaskProducer(threading.Thread):
    def __init__(self, q_list, c_name, host_name, request_duration=300):
        super().__init__()
        self.q_list = q_list
        self.c_name = c_name
        self.host_name = host_name
        self.request_duration = request_duration

    def task(self):
        collection = Collection(name=self.c_name, using=self.host_name)
        res = collection.query(expr="int64 >= 0", output_fields=["int64"])
        sorted_res = sorted(res, key=lambda k: k['int64'])
        ids = [i["int64"] for i in sorted_res]
        log.info(f"query result {len(ids)}")
        data = ids[:100]
        uid = uuid.uuid4()
        for q in self.q_list:
            q.put({"uid": uid, "op": "delete", "data": data})
        log.info(f"delete task produce task {uid}")

    def mock_task(self):
        data = [i for i in range(100)]
        uid = uuid.uuid4()
        for q in self.q_list:
            q.put({"uid": uid, "op": "delete", "data": data})
        log.info(f"delete task produce task {uid}")

    def run(self):
        t0 = time.time()
        while time.time() - t0 < self.request_duration:
            if enable_mock:
                self.mock_task()
            else:
                self.task()
            time.sleep(1)
        for q in self.q_list:
            q.put(None)
        log.info("delete task produce finished")


def main(host_1, host_2, request_duration=300, c_name=None):
    host_list = [host_1, host_2]
    host_num = len(host_list)
    c_name = "cdc_test_collection" if c_name is None else c_name
    if not enable_mock:
        for i, host in enumerate(host_list):
            connections.connect(f"host_{i}", host, uri=f"http://{host}:19530")
            dim = 128
            default_fields = [
                FieldSchema(name="int64", dtype=DataType.INT64, is_primary=True),
                FieldSchema(name="float", dtype=DataType.FLOAT),
                FieldSchema(name="varchar", dtype=DataType.VARCHAR, max_length=65535),
                FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=dim)
            ]
            default_schema = CollectionSchema(fields=default_fields, description="test collection")
            collection = Collection(name=c_name, schema=default_schema, using=f"host_{i}")
            collection.create_index(field_name="float_vector", index_params={"metric_type": "L2"})
            collection.load()
    host_list = [f"host_{i}" for i in range(host_num)]

    insert_queue_list = [queue.Queue() for _ in range(host_num)]
    delete_queue_list = [queue.Queue() for _ in range(host_num)]
    all_tasks = []
    # produce insert task
    t = InsertTaskProducer(q_list=insert_queue_list, request_duration=request_duration)
    t.start()
    all_tasks.append(t)
    # produce delete task
    t = DeleteTaskProducer(q_list=delete_queue_list, c_name=c_name, host_name=host_list[0], request_duration=request_duration)
    t.start()
    all_tasks.append(t)

    # consume insert task
    for i in range(host_num):
        t = InsertWorker(q=insert_queue_list[i], c_name=c_name, host_name=host_list[i])
        t.start()
        all_tasks.append(t)
    # consume delete task
    for i in range(host_num):
        t = DeleteWorker(q=delete_queue_list[i], c_name=c_name, host_name=host_list[i])
        t.start()
        all_tasks.append(t)
    for t in all_tasks:
        t.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--host_1', type=str, default='127.0.0.1', help='host ip')
    parser.add_argument('--host_2', type=str, default='127.0.0.1', help='host ip')
    parser.add_argument('--request_duration', type=str, default='1', help='request duration')
    parser.add_argument('--c_name', type=str, default='hello_milvus', help='collection name')
    args = parser.parse_args()
    host_1 = args.host_1
    host_2 = args.host_2
    request_duration = int(args.request_duration) * 60  # minutes to seconds
    c_name = args.c_name
    main(host_1=host_1, host_2=host_2, request_duration=request_duration, c_name=c_name)
