from pymilvus import connections
from utils import *


def task_1(data_size):
    """
    task_1:
        before upgrade: create collection and insert data, load and search
        after upgrade: get collection, load, search, create index, load, and search
    """
    prefix = "task_1_"
    connections.connect(host="127.0.0.1", port=19530, timeout=60)
    get_collections(prefix)
    load_and_search(prefix)
    create_collections_and_insert_data(prefix, data_size)
    load_and_search(prefix)


def task_2(data_size):
    """
    task_2:
        before upgrade: create collection, insert data and create index, load and search
        after upgrade: get collection, load, search, insert data, create index, load, and search
    """
    prefix = "task_2_"
    connections.connect(host="127.0.0.1", port=19530, timeout=60)
    get_collections(prefix)
    load_and_search(prefix)
    create_collections_and_insert_data(prefix, data_size)
    create_index(prefix)
    load_and_search(prefix)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='config for deploy test')
    parser.add_argument('--data_size', type=int, default=3000, help='data size')
    args = parser.parse_args()
    data_size = args.data_size
    print(f"data size: {data_size}")
    task_1(data_size)
    task_2(data_size)
