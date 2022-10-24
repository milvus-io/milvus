import h5py
import numpy as np
import time
from pathlib import Path
from pymilvus import (
    connections,
    FieldSchema, CollectionSchema, DataType,
    Collection
)


def read_benchmark_hdf5(file_path):

    f = h5py.File(file_path, 'r')
    train = np.array(f["train"])
    test = np.array(f["test"])
    neighbors = np.array(f["neighbors"])
    f.close()
    return train, test, neighbors


dim = 128
TIMEOUT = 200


def milvus_recall_test(host='127.0.0.1'):
    file_path = f"{str(Path(__file__).absolute().parent.parent.parent)}/assets/ann_hdf5/sift-128-euclidean.hdf5"
    train, test, neighbors = read_benchmark_hdf5(file_path)
    connections.connect(host=host, port="19530")
    default_fields = [
        FieldSchema(name="int64", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name="float", dtype=DataType.FLOAT),
        FieldSchema(name="varchar", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=dim)
    ]
    default_schema = CollectionSchema(
        fields=default_fields, description="test collection")
    collection = Collection(name="sift_128_euclidean", schema=default_schema)
    nb = len(train)
    batch_size = 50000
    epoch = int(nb / batch_size)
    t0 = time.time()
    for i in range(epoch):
        print("epoch:", i)
        start = i * batch_size
        end = (i + 1) * batch_size
        if end > nb:
            end = nb
        data = [
            [i for i in range(start, end)],
            [np.float32(i) for i in range(start, end)],
            [str(i) for i in range(start, end)],
            train[start:end]
        ]
        collection.insert(data)
    t1 = time.time()
    print(f"\nInsert {nb} vectors cost {t1 - t0:.4f} seconds")

    t0 = time.time()
    print(f"\nGet collection entities...")
    print(collection.num_entities)
    t1 = time.time()
    print(f"\nGet collection entities cost {t1 - t0:.4f} seconds")

    # create index
    default_index = {"index_type": "IVF_SQ8",
                     "metric_type": "L2", "params": {"nlist": 64}}
    print(f"\nCreate index...")
    t0 = time.time()
    collection.create_index(field_name="float_vector",
                            index_params=default_index)
    t1 = time.time()
    print(f"\nCreate index cost {t1 - t0:.4f} seconds")

    # load collection
    replica_number = 1
    print(f"\nload collection...")
    t0 = time.time()
    collection.load(replica_number=replica_number)
    t1 = time.time()
    print(f"\nload collection cost {t1 - t0:.4f} seconds")

    # search
    topK = 100
    nq = 10000
    search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
    t0 = time.time()
    print(f"\nSearch...")
    # define output_fields of search result
    res = collection.search(
        test[:nq], "float_vector", search_params, topK, output_fields=["int64"], timeout=TIMEOUT
    )
    t1 = time.time()
    print(f"search cost  {t1 - t0:.4f} seconds")
    result_ids = []
    for hits in res:
        result_id = []
        for hit in hits:
            result_id.append(hit.entity.get("int64"))
        result_ids.append(result_id)

    # calculate recall
    true_ids = neighbors[:nq, :topK]
    sum_radio = 0.0
    for index, item in enumerate(result_ids):
        # tmp = set(item).intersection(set(flat_id_list[index]))
        assert len(item) == len(true_ids[index])
        tmp = set(true_ids[index]).intersection(set(item))
        sum_radio = sum_radio + len(tmp) / len(item)
    recall = round(sum_radio / len(result_ids), 3)
    assert recall >= 0.95
    print(f"recall={recall}")

    # query
    expr = "int64 in [2,4,6,8]"
    output_fields = ["int64", "float"]
    res = collection.query(expr, output_fields, timeout=TIMEOUT)
    sorted_res = sorted(res, key=lambda k: k['int64'])
    for r in sorted_res:
        print(r)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='config for recall test')
    parser.add_argument('--host', type=str,
                        default="127.0.0.1", help='milvus server ip')
    args = parser.parse_args()
    host = args.host
    milvus_recall_test(host)
