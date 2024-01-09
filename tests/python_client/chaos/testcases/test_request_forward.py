# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.


import random
import numpy as np
import time
from loguru import logger as log
from threading import Thread
import threading
import functools
import argparse
from pymilvus import (
    connections, list_collections,
    FieldSchema, CollectionSchema, DataType,
    Collection
)
TIMEOUT = 120


lock = threading.Lock()


def init_collection(c_name="hello_milvus", host_list=None):
    # create collection
    dim = 128
    default_fields = [
        FieldSchema(name="int64", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name="float", dtype=DataType.FLOAT),
        FieldSchema(name="varchar", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=dim)
    ]
    default_schema = CollectionSchema(fields=default_fields, description="test collection")
    for using in host_list:
        collection = Collection(name=c_name, schema=default_schema, using=using)
        collection.create_index(field_name="float_vector", index_params={"metric_type": "L2"})
        collection.load()


def insert_data(c_name="hello_milvus", nb=3000, host_list=None):
    #  insert data
    dim = 128
    vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
    pk_value = []
    for i in range(nb):
        t = time.time() * 100000
        pk_value.append(int(t))
        time.sleep(0.001)
    data = [
        pk_value,
        [np.float32(i) for i in range(nb)],
        [str(i) for i in range(nb)],
        vectors
    ]
    for using in host_list:
        collection = Collection(name=c_name, using=using)
        res = collection.insert(data)
        log.info(f"{using} insert data: {len(data[0])} {res}")
    time.sleep(1)


def delete_data(c_name="hello_milvus", host_list=None):
    # query data
    ids_list = []
    with lock:
        for using in host_list:
            collection = Collection(name=c_name, using=using)
            res = collection.query(
                expr=f"int64 >= 0",
                output_fields=["int64"]
            )
            sorted_res = sorted(res, key=lambda k: k['int64'])
            log.info(f"{using} query result: {len(sorted_res)}")
            ids = [i["int64"] for i in res]
            ids_list.append(ids)
            res = collection.query(expr="", output_fields=["count(*)"])
            log.info(f"{using} count(*) result: {res}")
    ids_list = [sorted(x) for x in ids_list]
    # common ids
    common_ids = functools.reduce(lambda x, y: list(set(x).intersection(set(y))), ids_list)
    # log.info(f"common ids: {common_ids}")
    length = len(common_ids)
    log.info(f"delete data: {length//2}")
    expr = f"int64 in {common_ids[:length//2]}"
    #  delete data
    for using in host_list:
        collection = Collection(name=c_name, using=using)
        res = collection.delete(expr=expr)
        log.info(f"{using} delete data: {res}")
    time.sleep(10)


def hello_milvus(host_1="127.0.0.1", host_2="127.0.0.1", request_duration=300):
    connections.connect("host_1", uri=f"http://{host_1}:19530")
    connections.connect("host_2", uri=f"http://{host_2}:19530")
    log.info(connections)
    host_list = ["host_1", "host_2"]
    c_name = "cdc_test_collection"
    init_collection(host_list=host_list, c_name=c_name)

    def insert_worker():
        t0 = time.time()
        while time.time() - t0 < request_duration:
            insert_data(host_list=host_list, c_name=c_name)

    def delete_worker():
        t0 = time.time()
        while time.time() - t0 < request_duration:
            delete_data(host_list=host_list, c_name=c_name)
    tasks = []
    t1 = Thread(target=insert_worker)
    tasks.append(t1)
    t2 = Thread(target=delete_worker)
    tasks.append(t2)
    for t in tasks:
        t.start()
    for t in tasks:
        t.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='host ip')
    parser.add_argument('--host_1', type=str, default='127.0.0.1', help='host ip')
    parser.add_argument('--host_2', type=str, default='127.0.0.1', help='host ip')
    parser.add_argument('--request_duration', type=str, default='1', help='host ip')
    args = parser.parse_args()
    host_1 = args.host_1
    host_2 = args.host_2
    request_duration = int(args.request_duration) * 60
    hello_milvus(host_1, host_2, request_duration)
