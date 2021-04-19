import time
import random
import logging
from client import MilvusClient
import utils


if __name__ == "__main__":
    milvus_instance = MilvusClient()
    milvus_instance.clean_db()
    p_num = 1
    dimension = 128
    insert_xb = 100000
    index_types = ['flat']
    index_param = {"nlist": 2048}
    collection_names = []
    milvus_instances_map = {}
    insert_vectors = [[random.random() for _ in range(dimension)] for _ in range(insert_xb)]

    for i in range(collection_num):
        name = utils.get_unique_name(prefix="collection_")
        print(name)
        collection_names.append(name)
        metric_type = "ip"
        # metric_type = random.choice(["l2", "ip"])
        index_file_size = random.randint(10, 100)
        milvus_instance.create_collection(name, dimension, index_file_size, metric_type)
        milvus_instance = MilvusClient(collection_name=name)
        index_type = random.choice(index_types)
        milvus_instance.create_index(index_type, index_param=index_param)
        insert_vectors = utils.normalize(metric_type, insert_vectors)
        milvus_instance.insert(insert_vectors)
        milvus_instance.flush()
        milvus_instances_map.update({name: milvus_instance})
        print(milvus_instance.describe_index(), milvus_instance.describe(), milvus_instance.count())

    # tasks = ["insert_rand", "delete_rand", "query_rand", "flush", "compact"]
    tasks = ["insert_rand", "query_rand", "flush"]
    i = 1
    while True:
        print("Loop time: %d" % i)
        start_time = time.time()
        while time.time() - start_time < pull_interval_seconds:
            # choose collection
            tmp_collection_name = random.choice(collection_names)
            # choose task from task
            task_name = random.choice(tasks)
            # print(tmp_collection_name, task_name) 
            func = getattr(milvus_instances_map[tmp_collection_name], task_name)
            func()
        print("Restart")
        i = i + 1
