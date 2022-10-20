from pymilvus import connections
import sys
sys.path.append("..")
sys.path.append("../..")
from common.milvus_sys import MilvusSys
from utils import *


def task_1(data_size, host):
    """
    task_1:
        before upgrade: create collection and insert data with flush, load and search
        after upgrade: get collection, load, search, insert data with flush, create index, load, and search
    """
    prefix = "task_1_"
    connections.connect(host=host, port=19530, timeout=60)
    col_list = get_collections(prefix, check=True)
    assert len(col_list) == len(all_index_types)
    load_and_search(prefix)
    create_collections_and_insert_data(prefix, data_size)
    create_index(prefix)
    load_and_search(prefix)


def task_2(data_size, host):
    """
    task_2:
        before upgrade: create collection, insert data and create index, load and search
        after upgrade: get collection, load, search, insert data, create index, load, and search
    """
    prefix = "task_2_"
    connections.connect(host=host, port=19530, timeout=60)
    col_list = get_collections(prefix, check=True)
    assert len(col_list) == len(all_index_types)
    load_and_search(prefix)
    create_collections_and_insert_data(prefix, data_size)
    create_index(prefix)
    load_and_search(prefix)


def task_3(data_size, host):
    """
    task_3:
        before upgrade: create collection, insert data, flush, create index, load with one replicas and search
        after upgrade: get collection, load, search, insert data, create index, release, load with multi replicas, and search
    """
    prefix = "task_3_"
    connections.connect(host=host, port=19530, timeout=60)
    col_list = get_collections(prefix, check=True)
    assert len(col_list) == len(all_index_types)
    load_and_search(prefix)
    create_collections_and_insert_data(prefix, count=data_size)
    create_index(prefix)
    load_and_search(prefix, replicas=NUM_REPLICAS)


def task_4(data_size, host):
    """
    task_4:
        before upgrade: create collection, load, insert data without flush
        after upgrade: get collection, load with multi replicas, search, insert data with flush, load with multi replicas and search
    """
    prefix = "task_4_"
    connections.connect(host=host, port=19530, timeout=60)
    col_list = get_collections(prefix, check=True)
    assert len(col_list) == len(all_index_types)
    load_and_search(prefix, replicas=NUM_REPLICAS)
    create_collections_and_insert_data(prefix, flush=False, count=data_size)
    load_and_search(prefix, replicas=NUM_REPLICAS)


def task_5(data_size, host):
    """
    task_5_:
        before upgrade: create collection and insert data without flush
        after upgrade: get collection, load with multi replicas, search, insert data with flush, load with multi replicas and search
    """
    prefix = "task_5_"
    connections.connect(host=host, port=19530, timeout=60)
    col_list = get_collections(prefix, check=True)
    assert len(col_list) == len(all_index_types)
    load_and_search(prefix, replicas=NUM_REPLICAS)
    create_collections_and_insert_data(prefix, flush=True, count=data_size)
    load_and_search(prefix, replicas=NUM_REPLICAS)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='config for deploy test')
    parser.add_argument('--host', type=str, default="127.0.0.1", help='milvus server ip')
    parser.add_argument('--data_size', type=int, default=3000, help='data size')
    args = parser.parse_args()
    data_size = args.data_size
    host = args.host
    print(f"data size: {data_size}")
    connections.connect(host=host, port=19530, timeout=60)
    ms = MilvusSys()
    task_1(data_size, host)
    task_2(data_size, host)
    if len(ms.query_nodes) >= NUM_REPLICAS:
        task_3(data_size, host)
        task_4(data_size, host)
        task_5(data_size, host)
