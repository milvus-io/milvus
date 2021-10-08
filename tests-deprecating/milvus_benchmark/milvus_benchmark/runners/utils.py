import os
import pdb
import logging
import numpy as np
import sklearn.preprocessing
import h5py
import random
from itertools import product

from pymilvus import DataType
from milvus_benchmark import config

logger = logging.getLogger("milvus_benchmark.runners.utils")

DELETE_INTERVAL_TIME = 2

VECTORS_PER_FILE = 1000000
SIFT_VECTORS_PER_FILE = 100000
BINARY_VECTORS_PER_FILE = 2000000

MAX_NQ = 10001
FILE_PREFIX = "binary_"

WARM_TOP_K = 1
WARM_NQ = 1
DEFAULT_DIM = 512
DEFAULT_METRIC_TYPE = "L2"

RANDOM_SRC_DATA_DIR = config.RAW_DATA_DIR + 'random/'
SIFT_SRC_DATA_DIR = config.RAW_DATA_DIR + 'sift1b/'  # has organized the corresponding query.npy file
DEEP_SRC_DATA_DIR = config.RAW_DATA_DIR + 'deep1b/'
JACCARD_SRC_DATA_DIR = config.RAW_DATA_DIR + 'jaccard/'  # has organized the corresponding query.npy file
HAMMING_SRC_DATA_DIR = config.RAW_DATA_DIR + 'hamming/'  # The current data set is not organized on the NAS
STRUCTURE_SRC_DATA_DIR = config.RAW_DATA_DIR + 'structure/'  # The current data set is not organized on the NAS
BINARY_SRC_DATA_DIR = config.RAW_DATA_DIR + 'binary/'  # has organized the corresponding query.npy file
SIFT_SRC_GROUNDTRUTH_DATA_DIR = SIFT_SRC_DATA_DIR + 'gnd'

DEFAULT_F_FIELD_NAME = 'float_vector'
DEFAULT_B_FIELD_NAME = 'binary_vector'
DEFAULT_INT_FIELD_NAME = 'int64'
DEFAULT_FLOAT_FIELD_NAME = 'float'
DEFAULT_DOUBLE_FIELD_NAME = "double"

GROUNDTRUTH_MAP = {
    "1000000": "idx_1M.ivecs",
    "2000000": "idx_2M.ivecs",
    "5000000": "idx_5M.ivecs",
    "10000000": "idx_10M.ivecs",
    "20000000": "idx_20M.ivecs",
    "50000000": "idx_50M.ivecs",
    "100000000": "idx_100M.ivecs",
    "200000000": "idx_200M.ivecs",
    "500000000": "idx_500M.ivecs",
    "1000000000": "idx_1000M.ivecs",
}

METRIC_MAP = {
    "l2": "L2",
    "ip": "IP",
    "jaccard": "JACCARD",
    "hamming": "HAMMING",
    "sub": "SUBSTRUCTURE",
    "super": "SUPERSTRUCTURE"
}


def get_len_vectors_per_file(data_type, dimension):
    if data_type == "random":
        if dimension == 512:
            vectors_per_file = VECTORS_PER_FILE
        elif dimension == 4096:
            vectors_per_file = 100000
        elif dimension == 16384:
            vectors_per_file = 10000
    elif data_type == "sift":
        vectors_per_file = SIFT_VECTORS_PER_FILE
    elif data_type in ["binary"]:
        vectors_per_file = BINARY_VECTORS_PER_FILE
    elif data_type == "local":
        vectors_per_file = SIFT_VECTORS_PER_FILE
    else:
        raise Exception("data_type: %s not supported" % data_type)
    return vectors_per_file


def get_vectors_from_binary(nq, dimension, data_type):
    # use the first file, nq should be less than VECTORS_PER_FILE
    if nq > MAX_NQ:
        raise Exception("Over size nq")
    if data_type == "local":
        return generate_vectors(nq, dimension)
    elif data_type == "random":
        file_name = RANDOM_SRC_DATA_DIR + 'query_%d.npy' % dimension
    elif data_type == "sift":
        file_name = SIFT_SRC_DATA_DIR + 'query.npy'
    elif data_type == "deep":
        file_name = DEEP_SRC_DATA_DIR + 'query.npy'
    elif data_type == "binary":
        file_name = BINARY_SRC_DATA_DIR + 'query.npy'
    data = np.load(file_name)
    vectors = data[0:nq].tolist()
    return vectors


def generate_vectors(nb, dim):
    return [[random.random() for _ in range(dim)] for _ in range(nb)]


def generate_values(data_type, vectors, ids):
    values = None
    if data_type in [DataType.INT32, DataType.INT64]:
        values = ids
    elif data_type in [DataType.FLOAT, DataType.DOUBLE]:
        values = [(i + 0.0) for i in ids]
    elif data_type in [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR]:
        values = vectors
    return values


def generate_entities(info, vectors, ids=None):
    entities = []
    for field in info["fields"]:
        # if field["name"] == "_id":
        #     continue
        field_type = field["type"]
        entities.append(
            {"name": field["name"], "type": field_type, "values": generate_values(field_type, vectors, ids)})
    return entities


def metric_type_trans(metric_type):
    if metric_type in METRIC_MAP.keys():
        return METRIC_MAP[metric_type]
    else:
        raise Exception("metric_type: %s not in METRIC_MAP" % metric_type)


def get_dataset(hdf5_file_path):
    if not os.path.exists(hdf5_file_path):
        raise Exception("%s not existed" % hdf5_file_path)
    dataset = h5py.File(hdf5_file_path)
    return dataset


def get_default_field_name(data_type=DataType.FLOAT_VECTOR):
    if data_type == DataType.FLOAT_VECTOR:
        field_name = DEFAULT_F_FIELD_NAME
    elif data_type == DataType.BINARY_VECTOR:
        field_name = DEFAULT_B_FIELD_NAME
    elif data_type == DataType.INT64:
        field_name = DEFAULT_INT_FIELD_NAME
    elif data_type == DataType.FLOAT:
        field_name = DEFAULT_FLOAT_FIELD_NAME
    else:
        logger.error(data_type)
        raise Exception("Not supported data type")
    return field_name


def get_vector_type(data_type):
    vector_type = ''
    if data_type in ["random", "sift", "deep", "glove", "local"]:
        vector_type = DataType.FLOAT_VECTOR
    elif data_type in ["binary"]:
        vector_type = DataType.BINARY_VECTOR
    else:
        raise Exception("Data type: %s not defined" % data_type)
    return vector_type


def get_vector_type_from_metric(metric_type):
    vector_type = ''
    if metric_type in ["hamming", "jaccard"]:
        vector_type = DataType.BINARY_VECTOR
    else:
        vector_type = DataType.FLOAT_VECTOR
    return vector_type


def normalize(metric_type, X):
    if metric_type == "ip":
        logger.info("Set normalize for metric_type: %s" % metric_type)
        X = sklearn.preprocessing.normalize(X, axis=1, norm='l2')
        X = X.astype(np.float32)
    elif metric_type == "l2":
        X = X.astype(np.float32)
    elif metric_type in ["jaccard", "hamming", "sub", "super"]:
        tmp = []
        for item in X:
            new_vector = bytes(np.packbits(item, axis=-1).tolist())
            tmp.append(new_vector)
        X = tmp
    return X


def generate_combinations(args):
    if isinstance(args, list):
        args = [el if isinstance(el, list) else [el] for el in args]
        return [list(x) for x in product(*args)]
    elif isinstance(args, dict):
        flat = []
        for k, v in args.items():
            if isinstance(v, list):
                flat.append([(k, el) for el in v])
            else:
                flat.append([(k, v)])
        return [dict(x) for x in product(*flat)]
    else:
        raise TypeError("No args handling exists for %s" % type(args).__name__)


def gen_file_name(idx, dimension, data_type):
    s = "%05d" % idx
    fname = FILE_PREFIX + str(dimension) + "d_" + s + ".npy"
    if data_type == "random":
        fname = RANDOM_SRC_DATA_DIR + fname
    elif data_type == "sift":
        fname = SIFT_SRC_DATA_DIR + fname
    elif data_type == "deep":
        fname = DEEP_SRC_DATA_DIR + fname
    elif data_type == "jaccard":
        fname = JACCARD_SRC_DATA_DIR + fname
    elif data_type == "hamming":
        fname = HAMMING_SRC_DATA_DIR + fname
    elif data_type == "sub" or data_type == "super":
        fname = STRUCTURE_SRC_DATA_DIR + fname
    return fname


def get_recall_value(true_ids, result_ids):
    """
    Use the intersection length
    """
    sum_radio = 0.0
    for index, item in enumerate(result_ids):
        # tmp = set(item).intersection(set(flat_id_list[index]))
        tmp = set(true_ids[index]).intersection(set(item))
        sum_radio = sum_radio + len(tmp) / len(item)
        # logger.debug(sum_radio)
    return round(sum_radio / len(result_ids), 3)


def get_ground_truth_ids(collection_size):
    fname = GROUNDTRUTH_MAP[str(collection_size)]
    fname = SIFT_SRC_GROUNDTRUTH_DATA_DIR + "/" + fname
    a = np.fromfile(fname, dtype='int32')
    d = a[0]
    true_ids = a.reshape(-1, d + 1)[:, 1:].copy()
    return true_ids


def normalize(metric_type, X):
    if metric_type == "ip":
        logger.info("Set normalize for metric_type: %s" % metric_type)
        X = sklearn.preprocessing.normalize(X, axis=1, norm='l2')
        X = X.astype(np.float32)
    elif metric_type == "l2":
        X = X.astype(np.float32)
    elif metric_type in ["jaccard", "hamming", "sub", "super"]:
        tmp = []
        for item in X:
            new_vector = bytes(np.packbits(item, axis=-1).tolist())
            tmp.append(new_vector)
        X = tmp
    return X