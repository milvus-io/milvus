import h5py
import numpy as np
import time
from loguru import logger
import copy
from pathlib import Path
import pymilvus
from pymilvus import (
    connections,
    FieldSchema, CollectionSchema, DataType,
    Collection
)

pymilvus_version = pymilvus.__version__


all_index_types = ["IVF_FLAT", "IVF_SQ8", "IVF_PQ", "HNSW", "ANNOY"]
default_index_params = [{"nlist": 128}, {"nlist": 128}, {"nlist": 128, "m": 16, "nbits": 8},
                        {"M": 48, "efConstruction": 100}, {"n_trees": 50}]
index_params_map = dict(zip(all_index_types, default_index_params))


def gen_index_params(index_type, metric_type="L2"):
    default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": metric_type}
    index = copy.deepcopy(default_index)
    index["index_type"] = index_type
    index["params"] = index_params_map[index_type]
    if index_type in ["BIN_FLAT", "BIN_IVF_FLAT"]:
        index["metric_type"] = "HAMMING"
    return index


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


def read_benchmark_hdf5(file_path):

    f = h5py.File(file_path, 'r')
    train = np.array(f["train"])
    test = np.array(f["test"])
    neighbors = np.array(f["neighbors"])
    f.close()
    return train, test, neighbors


dim = 128
TIMEOUT = 200


def milvus_recall_test(host='127.0.0.1', index_type="HNSW"):
    logger.info(f"recall test for index type {index_type}")
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

    name = f"sift_128_euclidean_{index_type}"
    logger.info(f"Create collection {name}")
    collection = Collection(name=name, schema=default_schema)
    nb = len(train)
    batch_size = 50000
    epoch = int(nb / batch_size)
    t0 = time.time()
    for i in range(epoch):
        logger.info(f"epoch: {i}")
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
    logger.info(f"Insert {nb} vectors cost {t1 - t0:.4f} seconds")

    t0 = time.time()
    logger.info(f"Get collection entities...")
    if pymilvus_version >= "2.2.0":
        collection.flush()
    else:
        collection.num_entities
    logger.info(collection.num_entities)
    t1 = time.time()
    logger.info(f"Get collection entities cost {t1 - t0:.4f} seconds")

    # create index
    default_index = gen_index_params(index_type)
    logger.info(f"Create index...")
    t0 = time.time()
    collection.create_index(field_name="float_vector",
                            index_params=default_index)
    t1 = time.time()
    logger.info(f"Create index cost {t1 - t0:.4f} seconds")

    # load collection
    replica_number = 1
    logger.info(f"load collection...")
    t0 = time.time()
    collection.load(replica_number=replica_number)
    t1 = time.time()
    logger.info(f"load collection cost {t1 - t0:.4f} seconds")

    # search
    topK = 100
    nq = 10000
    current_search_params = gen_search_param(index_type)

    # define output_fields of search result
    for i in range(3):
        t0 = time.time()
        logger.info(f"Search...")
        res = collection.search(
            test[:nq], "float_vector", current_search_params, topK, output_fields=["int64"], timeout=TIMEOUT
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
        logger.info(f"Calculate recall...")
        for index, item in enumerate(result_ids):
            # tmp = set(item).intersection(set(flat_id_list[index]))
            assert len(item) == len(true_ids[index])
            tmp = set(true_ids[index]).intersection(set(item))
            sum_radio = sum_radio + len(tmp) / len(item)
        recall = round(sum_radio / len(result_ids), 3)
        logger.info(f"recall={recall}")
        if index_type in ["IVF_PQ", "ANNOY"]:
            assert recall >= 0.6, f"recall={recall} < 0.6"
        else:
            assert 0.95 <= recall < 1.0, f"recall is {recall}, less than 0.95, greater than or equal to 1.0"
    # query
    expr = "int64 in [2,4,6,8]"
    output_fields = ["int64", "float"]
    res = collection.query(expr, output_fields, timeout=TIMEOUT)
    sorted_res = sorted(res, key=lambda k: k['int64'])
    for r in sorted_res:
        logger.info(r)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='config for recall test')
    parser.add_argument('--host', type=str,
                        default="127.0.0.1", help='milvus server ip')
    args = parser.parse_args()
    host = args.host
    for index_type in all_index_types:
        milvus_recall_test(host, index_type)

