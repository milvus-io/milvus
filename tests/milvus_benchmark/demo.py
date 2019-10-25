import os
import logging
import pdb
import time
import random
from multiprocessing import Process
import numpy as np
from client import MilvusClient

nq = 100000
dimension = 128
run_count = 1
table_name = "sift_10m_1024_128_ip"
insert_vectors = [[random.random() for _ in range(dimension)] for _ in range(nq)]

def do_query(milvus, table_name, top_ks, nqs, nprobe, run_count):
    bi_res = []
    for index, nq in enumerate(nqs):
        tmp_res = []
        for top_k in top_ks:
            avg_query_time = 0.0
            total_query_time = 0.0
            vectors = insert_vectors[0:nq]
            for i in range(run_count):
                start_time = time.time()
                status, query_res = milvus.query(vectors, top_k, nprobe)
                total_query_time = total_query_time + (time.time() - start_time)
                if status.code:
                    print(status.message)
            avg_query_time = round(total_query_time / run_count, 2)
            tmp_res.append(avg_query_time)
        bi_res.append(tmp_res)
    return bi_res

while 1:
    milvus_instance = MilvusClient(table_name, ip="192.168.1.197", port=19530)
    top_ks = random.sample([x for x in range(1, 100)], 4)
    nqs = random.sample([x for x in range(1, 1000)], 3)
    nprobe = random.choice([x for x in range(1, 500)])
    res = do_query(milvus_instance, table_name, top_ks, nqs, nprobe, run_count)
    status, res = milvus_instance.insert(insert_vectors, ids=[x for x in range(len(insert_vectors))])
    if not status.OK():
        logger.error(status.message)

    # status = milvus_instance.drop_index()
    if not status.OK():
        print(status.message)
    index_type = "ivf_sq8"
    status = milvus_instance.create_index(index_type, 16384)
    if not status.OK():
        print(status.message)