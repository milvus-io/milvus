# STL imports
import random
import string
import struct
import sys
import logging
import time, datetime
import copy
import numpy as np
from milvus import Milvus, IndexType, MetricType

port = 19530
epsilon = 0.000001

all_index_types = [
    IndexType.FLAT,
    IndexType.IVFLAT,
    IndexType.IVF_SQ8,
    IndexType.IVF_SQ8H,
    IndexType.IVF_PQ,
    IndexType.HNSW,
    IndexType.RNSG,
    IndexType.ANNOY
]


def get_milvus(host, port, uri=None, handler=None):
    if handler is None:
        handler = "GRPC"
    if uri is not None:
        milvus = Milvus(uri=uri, handler=handler)
    else:
        milvus = Milvus(host=host, port=port, handler=handler)
    return milvus


def gen_inaccuracy(num):
    return num / 255.0


def gen_vectors(num, dim):
    return [[random.random() for _ in range(dim)] for _ in range(num)]


def gen_binary_vectors(num, dim):
    raw_vectors = []
    binary_vectors = []
    for i in range(num):
        raw_vector = [random.randint(0, 1) for i in range(dim)]
        raw_vectors.append(raw_vector)
        binary_vectors.append(bytes(np.packbits(raw_vector, axis=-1).tolist()))
    return raw_vectors, binary_vectors


def jaccard(x, y):
    x = np.asarray(x, np.bool)
    y = np.asarray(y, np.bool)
    return 1 - np.double(np.bitwise_and(x, y).sum()) / np.double(np.bitwise_or(x, y).sum())


def hamming(x, y):
    x = np.asarray(x, np.bool)
    y = np.asarray(y, np.bool)
    return np.bitwise_xor(x, y).sum()


def tanimoto(x, y):
    x = np.asarray(x, np.bool)
    y = np.asarray(y, np.bool)
    return -np.log2(np.double(np.bitwise_and(x, y).sum()) / np.double(np.bitwise_or(x, y).sum()))


def substructure(x, y):
    x = np.asarray(x, np.bool)
    y = np.asarray(y, np.bool)
    return 1 - np.double(np.bitwise_and(x, y).sum()) / np.count_nonzero(y)


def superstructure(x, y):
    x = np.asarray(x, np.bool)
    y = np.asarray(y, np.bool)
    return 1 - np.double(np.bitwise_and(x, y).sum()) / np.count_nonzero(x)


def gen_binary_sub_vectors(vectors, length):
    raw_vectors = []
    binary_vectors = []
    dim = len(vectors[0])
    for i in range(length):
        raw_vector = [0 for i in range(dim)]
        vector = vectors[i]
        for index, j in enumerate(vector):
            if j == 1:
                raw_vector[index] = 1
        raw_vectors.append(raw_vector)
        binary_vectors.append(bytes(np.packbits(raw_vector, axis=-1).tolist()))
    return raw_vectors, binary_vectors


def gen_binary_super_vectors(vectors, length):
    raw_vectors = []
    binary_vectors = []
    dim = len(vectors[0])
    for i in range(length):
        cnt_1 = np.count_nonzero(vectors[i])
        raw_vector = [1 for i in range(dim)] 
        raw_vectors.append(raw_vector)
        binary_vectors.append(bytes(np.packbits(raw_vector, axis=-1).tolist()))
    return raw_vectors, binary_vectors
    

def gen_single_vector(dim):
    return [[random.random() for _ in range(dim)]]


def gen_vector(nb, d, seed=np.random.RandomState(1234)):
    xb = seed.rand(nb, d).astype("float32")
    return xb.tolist()


def gen_unique_str(str_value=None):
    prefix = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))
    return "test_" + prefix if str_value is None else str_value + "_" + prefix


def gen_long_str(num):
    string = ''
    for _ in range(num):
        char = random.choice('tomorrow')
        string += char


def gen_invalid_ips():
    ips = [
            # "255.0.0.0",
            # "255.255.0.0",
            # "255.255.255.0",
            # "255.255.255.255",
            "127.0.0",
            # "123.0.0.2",
            "12-s",
            " ",
            "12 s",
            "BB。A",
            " siede ",
            "(mn)",
            "中文",
            "a".join("a" for _ in range(256))
    ]
    return ips


def gen_invalid_ports():
    ports = [
            # empty
            " ",
            -1,
            # too big port
            100000,
            # not correct port
            39540,
            "BB。A",
            " siede ",
            "(mn)",
            "中文"
    ]
    return ports


def gen_invalid_uris():
    ip = None
    uris = [
            " ",
            "中文",
            # invalid protocol
            # "tc://%s:%s" % (ip, port),
            # "tcp%s:%s" % (ip, port),

            # # invalid port
            # "tcp://%s:100000" % ip,
            # "tcp://%s: " % ip,
            # "tcp://%s:19540" % ip,
            # "tcp://%s:-1" % ip,
            # "tcp://%s:string" % ip,

            # invalid ip
            "tcp:// :19530",
            # "tcp://123.0.0.1:%s" % port,
            "tcp://127.0.0:19530",
            # "tcp://255.0.0.0:%s" % port,
            # "tcp://255.255.0.0:%s" % port,
            # "tcp://255.255.255.0:%s" % port,
            # "tcp://255.255.255.255:%s" % port,
            "tcp://\n:19530",
    ]
    return uris


def gen_invalid_collection_names():
    collection_names = [
            "12-s",
            " ",
            # "",
            # None,
            "12 s",
            "BB。A",
            "c|c",
            " siede ",
            "(mn)",
            "pip+",
            "=c",
            "中文",
            "a".join("a" for i in range(256))
    ]
    return collection_names


def gen_invalid_top_ks():
    top_ks = [
            0,
            -1,
            None,
            [1,2,3],
            (1,2),
            {"a": 1},
            " ",
            "",
            "String",
            "12-s",
            "BB。A",
            " siede ",
            "(mn)",
            "pip+",
            "=c",
            "中文",
            "a".join("a" for i in range(256))
    ]
    return top_ks


def gen_invalid_dims():
    dims = [
            0,
            -1,
            100001,
            1000000000000001,
            None,
            False,
            [1,2,3],
            (1,2),
            {"a": 1},
            " ",
            "",
            "String",
            "12-s",
            "BB。A",
            " siede ",
            "(mn)",
            "pip+",
            "=c",
            "中文",
            "a".join("a" for i in range(256))
    ]
    return dims


def gen_invalid_file_sizes():
    file_sizes = [
            0,
            -1,
            1000000000000001,
            None,
            False,
            [1,2,3],
            (1,2),
            {"a": 1},
            " ",
            "",
            "String",
            "12-s",
            "BB。A",
            " siede ",
            "(mn)",
            "pip+",
            "=c",
            "中文",
            "a".join("a" for i in range(256))
    ]
    return file_sizes


def gen_invalid_index_types():
    invalid_types = [
            0,
            -1,
            100,
            1000000000000001,
            # None,
            False,
            [1,2,3],
            (1,2),
            {"a": 1},
            " ",
            "",
            "String",
            "12-s",
            "BB。A",
            " siede ",
            "(mn)",
            "pip+",
            "=c",
            "中文",
            "a".join("a" for i in range(256))
    ]
    return invalid_types


def gen_invalid_params():
    params = [
            9999999999,
            -1,
            # None,
            [1,2,3],
            (1,2),
            {"a": 1},
            " ",
            "",
            "String",
            "12-s",
            "BB。A",
            " siede ",
            "(mn)",
            "pip+",
            "=c",
            "中文"
    ]
    return params


def gen_invalid_nprobes():
    nprobes = [
            0,
            -1,
            1000000000000001,
            None,
            [1,2,3],
            (1,2),
            {"a": 1},
            " ",
            "",
            "String",
            "12-s",
            "BB。A",
            " siede ",
            "(mn)",
            "pip+",
            "=c",
            "中文"
    ]
    return nprobes


def gen_invalid_metric_types():
    metric_types = [
            0,
            -1,
            1000000000000001,
            # None,
            [1,2,3],
            (1,2),
            {"a": 1},
            " ",
            "",
            "String",
            "12-s",
            "BB。A",
            " siede ",
            "(mn)",
            "pip+",
            "=c",
            "中文"    
    ]
    return metric_types


def gen_invalid_vectors():
    invalid_vectors = [
            "1*2",
            [],
            [1],
            [1,2],
            [" "],
            ['a'],
            [None],
            None,
            (1,2),
            {"a": 1},
            " ",
            "",
            "String",
            "12-s",
            "BB。A",
            " siede ",
            "(mn)",
            "pip+",
            "=c",
            "中文",
            "a".join("a" for i in range(256))
    ]
    return invalid_vectors


def gen_invalid_vector_ids():
    invalid_vector_ids = [
            1.0,
            -1.0,
            None,
            # int 64
            10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,
            " ",
            "",
            "String",
            "BB。A",
            " siede ",
            "(mn)",
            "=c",
            "中文",
    ]
    return invalid_vector_ids


def gen_invalid_cache_config():
    invalid_configs = [
            0,
            -1,
            9223372036854775808,
            [1,2,3],
            (1,2),
            {"a": 1},
            " ",
            "",
            "String",
            "12-s",
            "BB。A",
            " siede ",
            "(mn)",
            "pip+",
            "=c",
            "中文",
            "'123'",
            "さようなら"
    ]
    return invalid_configs


def gen_invalid_engine_config():
    invalid_configs = [
            -1,
            [1,2,3],
            (1,2),
            {"a": 1},
            " ",
            "",
            "String",
            "12-s",
            "BB。A",
            " siede ",
            "(mn)",
            "pip+",
            "=c",
            "中文",
            "'123'",
    ]
    return invalid_configs


def gen_invaild_search_params():
    invalid_search_key = 100
    search_params = []
    for index_type in all_index_types:
        if index_type == IndexType.FLAT:
            continue
        search_params.append({"index_type": index_type, "search_param": {"invalid_key": invalid_search_key}})
        if index_type in [IndexType.IVFLAT, IndexType.IVF_SQ8, IndexType.IVF_SQ8H, IndexType.IVF_PQ]:
            for nprobe in gen_invalid_params():
                ivf_search_params = {"index_type": index_type, "search_param": {"nprobe": nprobe}}
                search_params.append(ivf_search_params)
        elif index_type == IndexType.HNSW:
            for ef in gen_invalid_params():
                hnsw_search_param = {"index_type": index_type, "search_param": {"ef": ef}}
                search_params.append(hnsw_search_param)
        elif index_type == IndexType.RNSG:
            for search_length in gen_invalid_params():
                nsg_search_param = {"index_type": index_type, "search_param": {"search_length": search_length}}
                search_params.append(nsg_search_param)
            search_params.append({"index_type": index_type, "search_param": {"invalid_key": 100}})
        elif index_type == IndexType.ANNOY:
            for search_k in gen_invalid_params():
                if isinstance(search_k, int):
                    continue
                annoy_search_param = {"index_type": index_type, "search_param": {"search_k": search_k}}
                search_params.append(annoy_search_param)
    return search_params


def gen_invalid_index():
    index_params = []
    for index_type in gen_invalid_index_types():
        index_param = {"index_type": index_type, "index_param": {"nlist": 1024}}
        index_params.append(index_param)
    for nlist in gen_invalid_params():
        index_param = {"index_type": IndexType.IVFLAT, "index_param": {"nlist": nlist}}
        index_params.append(index_param)
    for M in gen_invalid_params():
        index_param = {"index_type": IndexType.HNSW, "index_param": {"M": M, "efConstruction": 100}}
        index_params.append(index_param)
    for efConstruction in gen_invalid_params():
        index_param = {"index_type": IndexType.HNSW, "index_param": {"M": 16, "efConstruction": efConstruction}}
        index_params.append(index_param)
    for search_length in gen_invalid_params():
        index_param = {"index_type": IndexType.RNSG,
                       "index_param": {"search_length": search_length, "out_degree": 40, "candidate_pool_size": 50,
                                       "knng": 100}}
        index_params.append(index_param)
    for out_degree in gen_invalid_params():
        index_param = {"index_type": IndexType.RNSG,
                       "index_param": {"search_length": 100, "out_degree": out_degree, "candidate_pool_size": 50,
                                       "knng": 100}}
        index_params.append(index_param)
    for candidate_pool_size in gen_invalid_params():
        index_param = {"index_type": IndexType.RNSG, "index_param": {"search_length": 100, "out_degree": 40,
                                                                     "candidate_pool_size": candidate_pool_size,
                                                                     "knng": 100}}
        index_params.append(index_param)
    index_params.append({"index_type": IndexType.IVF_FLAT, "index_param": {"invalid_key": 1024}})
    index_params.append({"index_type": IndexType.HNSW, "index_param": {"invalid_key": 16, "efConstruction": 100}})
    index_params.append({"index_type": IndexType.RNSG,
                         "index_param": {"invalid_key": 100, "out_degree": 40, "candidate_pool_size": 300,
                                         "knng": 100}})
    for invalid_n_trees in gen_invalid_params():
        index_params.append({"index_type": IndexType.ANNOY, "index_param": {"n_trees": invalid_n_trees}})

    return index_params


def gen_index():
    nlists = [1, 1024, 16384]
    pq_ms = [128, 64, 32, 16, 8, 4]
    Ms = [5, 24, 48]
    efConstructions = [100, 300, 500]
    search_lengths = [10, 100, 300]
    out_degrees = [5, 40, 300]
    candidate_pool_sizes = [50, 100, 300]
    knngs = [5, 100, 300]

    index_params = []
    for index_type in all_index_types:
        if index_type == IndexType.FLAT:
            index_params.append({"index_type": index_type, "index_param": {"nlist": 1024}})
        elif index_type in [IndexType.IVFLAT, IndexType.IVF_SQ8, IndexType.IVF_SQ8H]:
            ivf_params = [{"index_type": index_type, "index_param": {"nlist": nlist}} \
                          for nlist in nlists]
            index_params.extend(ivf_params)
        elif index_type == IndexType.IVF_PQ:
            ivf_pq_params = [{"index_type": index_type, "index_param": {"nlist": nlist, "m": m}} \
                        for nlist in nlists \
                        for m in pq_ms]
            index_params.extend(ivf_pq_params)
        elif index_type == IndexType.HNSW:
            hnsw_params = [{"index_type": index_type, "index_param": {"M": M, "efConstruction": efConstruction}} \
                           for M in Ms \
                           for efConstruction in efConstructions]
            index_params.extend(hnsw_params)
        elif index_type == IndexType.RNSG:
            nsg_params = [{"index_type": index_type,
                           "index_param": {"search_length": search_length, "out_degree": out_degree,
                                           "candidate_pool_size": candidate_pool_size, "knng": knng}} \
                          for search_length in search_lengths \
                          for out_degree in out_degrees \
                          for candidate_pool_size in candidate_pool_sizes \
                          for knng in knngs]
            index_params.extend(nsg_params)

    return index_params


def gen_simple_index():
    params = [
        {"nlist": 1024},
        {"nlist": 1024},
        {"nlist": 1024},
        {"nlist": 1024},
        {"nlist": 1024, "m": 16},
        {"M": 48, "efConstruction": 500},
        {"search_length": 50, "out_degree": 40, "candidate_pool_size": 100, "knng": 50},
        {"n_trees": 4}
    ]
    index_params = []
    for i in range(len(all_index_types)):
        index_params.append({"index_type": all_index_types[i], "index_param": params[i]})
    return index_params


def get_search_param(index_type):
    if index_type in [IndexType.FLAT, IndexType.IVFLAT, IndexType.IVF_SQ8, IndexType.IVF_SQ8H, IndexType.IVF_PQ]:
        return {"nprobe": 32}
    elif index_type == IndexType.HNSW:
        return {"ef": 64}
    elif index_type == IndexType.RNSG:
        return {"search_length": 100}
    elif index_type == IndexType.ANNOY:
        return {"search_k": 100}

    else:
        logging.getLogger().info("Invalid index_type.")


def assert_has_collection(conn, collection_name):
    status, ok = conn.has_collection(collection_name)
    return status.OK() and ok


def assert_equal_vector(v1, v2):
    if len(v1) != len(v2):
        assert False
    for i in range(len(v1)):
        assert abs(v1[i] - v2[i]) < epsilon
