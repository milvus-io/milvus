import h5py
import numpy as np
import time
from loguru import logger
from pathlib import Path
from pymilvus import connections, Collection


all_index_types = ["IVF_FLAT", "IVF_SQ8", "IVF_PQ", "HNSW", "ANNOY"]


def read_benchmark_hdf5(file_path):

    f = h5py.File(file_path, 'r')
    train = np.array(f["train"])
    test = np.array(f["test"])
    neighbors = np.array(f["neighbors"])
    f.close()
    return train, test, neighbors


def gen_search_param(index_type, metric_type="L2"):
    search_params = []
    if index_type in ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ"]:
        for nprobe in [10]:
            ivf_search_params = {"metric_type": metric_type, "params": {"nprobe": nprobe}}
            search_params.append(ivf_search_params)
    elif index_type in ["BIN_FLAT", "BIN_IVF_FLAT"]:
        for nprobe in [10]:
            bin_search_params = {"metric_type": "HAMMING", "params": {"nprobe": nprobe}}
            search_params.append(bin_search_params)
    elif index_type in ["HNSW"]:
        for ef in [50]:
            hnsw_search_param = {"metric_type": metric_type, "params": {"ef": ef}}
            search_params.append(hnsw_search_param)
    elif index_type == "ANNOY":
        for search_k in [1000]:
            annoy_search_param = {"metric_type": metric_type, "params": {"search_k": search_k}}
            search_params.append(annoy_search_param)
    else:
        logger.info("Invalid index_type.")
        raise Exception("Invalid index_type.")
    return search_params[0]


dim = 128
TIMEOUT = 200


def search_test(host="127.0.0.1", index_type="HNSW"):
    logger.info(f"recall test for index type {index_type}")
    file_path = f"{str(Path(__file__).absolute().parent.parent.parent)}/assets/ann_hdf5/sift-128-euclidean.hdf5"
    train, test, neighbors = read_benchmark_hdf5(file_path)
    connections.connect(host=host, port="19530")
    collection = Collection(name=f"sift_128_euclidean_{index_type}")
    nq = 10000
    topK = 100
    search_params = gen_search_param(index_type)
    for i in range(3):
        t0 = time.time()
        logger.info(f"Search...")
        # define output_fields of search result
        res = collection.search(
            test[:nq], "float_vector", search_params, topK, output_fields=["int64"], timeout=TIMEOUT
        )
        t1 = time.time()
        logger.info(f"search cost  {t1 - t0:.4f} seconds")
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
            assert len(item) == len(true_ids[index]), f"get {len(item)} but expect {len(true_ids[index])}"
            tmp = set(true_ids[index]).intersection(set(item))
            sum_radio = sum_radio + len(tmp) / len(item)
        recall = round(sum_radio / len(result_ids), 3)
        logger.info(f"recall={recall}")
        if index_type in ["IVF_PQ", "ANNOY"]:
            assert recall >= 0.6, f"recall={recall} < 0.6"
        else:
            assert 0.95 <= recall < 1.0, f"recall is {recall}, less than 0.95, greater than or equal to 1.0"

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
    logger.info(f"recall={recall}")


if __name__ == "__main__":
    import argparse
    import threading
    parser = argparse.ArgumentParser(description='config for recall test')
    parser.add_argument('--host', type=str, default="127.0.0.1", help='milvus server ip')
    args = parser.parse_args()
    host = args.host
    for index_type in all_index_types:
        search_test(host, index_type)
