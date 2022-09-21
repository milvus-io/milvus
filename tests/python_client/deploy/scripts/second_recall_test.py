import h5py
import numpy as np
import time
from pathlib import Path
from pymilvus import connections, Collection


def read_benchmark_hdf5(file_path):

    f = h5py.File(file_path, 'r')
    train = np.array(f["train"])
    test = np.array(f["test"])
    neighbors = np.array(f["neighbors"])
    f.close()
    return train, test, neighbors


dim = 128
TIMEOUT = 200


def search_test(host="127.0.0.1"):
    file_path = f"{str(Path(__file__).absolute().parent.parent.parent)}/assets/ann_hdf5/sift-128-euclidean.hdf5"
    train, test, neighbors = read_benchmark_hdf5(file_path)
    connections.connect(host=host, port="19530")
    collection = Collection(name="sift_128_euclidean")
    nq = 10000
    topK = 100
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
    true_ids = neighbors[:nq,:topK]
    sum_radio = 0.0
    for index, item in enumerate(result_ids):
        # tmp = set(item).intersection(set(flat_id_list[index]))
        assert len(item) == len(true_ids[index]), f"get {len(item)} but expect {len(true_ids[index])}"
        tmp = set(true_ids[index]).intersection(set(item))
        sum_radio = sum_radio + len(tmp) / len(item)
    recall = round(sum_radio / len(result_ids), 3)
    assert recall >= 0.95, f"recall is {recall}, less than 0.95"
    print(f"recall={recall}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='config for recall test')
    parser.add_argument('--host', type=str, default="127.0.0.1", help='milvus server ip')
    args = parser.parse_args()
    host = args.host
    search_test(host)
