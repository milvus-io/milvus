import os
import sys
import random
import pdb
import string
import struct
import logging
import time, datetime
import copy
import numpy as np
from sklearn import preprocessing
from milvus import Milvus, IndexType, MetricType, DataType

port = 19530
epsilon = 0.000001
default_flush_interval = 1
big_flush_interval = 1000
dimension = 128
segment_size = 10


all_index_types = [
    "FLAT",
    "IVFFLAT",
    "IVFSQ8",
    "IVFSQ8H",
    "IVFPQ",
    "HNSW",
    "RNSG",
    "ANNOY"
]


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


def get_milvus(host, port, uri=None, handler=None, **kwargs):
    if handler is None:
        handler = "GRPC"
    try_connect = kwargs.get("try_connect", True)
    if uri is not None:
        milvus = Milvus(uri=uri, handler=handler, try_connect=try_connect)
    else:
        milvus = Milvus(host=host, port=port, handler=handler, try_connect=try_connect)
    return milvus


def disable_flush(connect):
    connect.set_config("storage", "auto_flush_interval", big_flush_interval)


def enable_flush(connect):
    # reset auto_flush_interval=1
    connect.set_config("storage", "auto_flush_interval", default_flush_interval)
    config_value = connect.get_config("storage", "auto_flush_interval")
    assert config_value == str(default_flush_interval)


def gen_inaccuracy(num):
    return num / 255.0


def gen_vectors(num, dim, is_normal=False):
    vectors = [[random.random() for _ in range(dim)] for _ in range(num)]
    vectors = preprocessing.normalize(vectors, axis=1, norm='l2')
    return vectors.tolist()


# def gen_vectors(num, dim, seed=np.random.RandomState(1234), is_normal=False):
#     xb = seed.rand(num, dim).astype("float32")
#     xb = preprocessing.normalize(xb, axis=1, norm='l2')
#     return xb.tolist()


def gen_binary_vectors(num, dim):
    raw_vectors = []
    binary_vectors = []
    for i in range(num):
        raw_vector = [random.randint(0, 1) for i in range(dim)]
        raw_vectors.append(raw_vector)
        binary_vectors.append(bytes(np.packbits(raw_vector, axis=-1).tolist()))
    return raw_vectors, binary_vectors


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


def gen_int_attr(row_num):
    return [random.randint(0, 255) for _ in range(row_num)]


def gen_float_attr(row_num):
    return [random.uniform(0, 255) for _ in range(row_num)]


def gen_unique_str(str_value=None):
    prefix = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))
    return "test_" + prefix if str_value is None else str_value + "_" + prefix


def gen_single_filter_fields():
    fields = []
    for data_type in DataType:
        if data_type in [DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64, DataType.FLOAT, DataType.DOUBLE]:
            fields.append({"field": data_type.name, "type": data_type})
    return fields


def gen_single_vector_fields():
    fields = []
    for metric_type in ['HAMMING', 'IP', 'JACCARD', 'L2', 'SUBSTRUCTURE', 'SUPERSTRUCTURE', 'TANIMOTO']:
        for data_type in [DataType.VECTOR, DataType.BINARY_VECTOR]:
            if metric_type in ["L2", "IP"] and data_type == DataType.BINARY_VECTOR:
                continue
            field = {"field": data_type.name, "type": data_type, "params": {"metric_type": metric_type, "dimension": dimension}}
            fields.append(field)
    return fields


def gen_default_fields():
    default_fields = {
        "fields": [
            {"field": "int8", "type": DataType.INT8},
            {"field": "int64", "type": DataType.INT64},
            {"field": "float", "type": DataType.FLOAT},
            {"field": "vector", "type": DataType.VECTOR, "params": {"metric_type": "L2", "dimension": dimension}}
        ],
        "segment_size": segment_size
    }
    return default_fields


def gen_entities(nb, is_normal=False):
    vectors = gen_vectors(nb, dimension, is_normal)
    entities = [
        {"field": "int8", "type": DataType.INT8, "values": [1 for i in range(nb)]},
        {"field": "int64", "type": DataType.INT64, "values": [2 for i in range(nb)]},
        {"field": "float", "type": DataType.FLOAT, "values": [3.0 for i in range(nb)]},
        {"field": "vector", "type": DataType.VECTOR, "values": vectors}
    ]
    return entities


def gen_binary_entities(nb):
    raw_vectors, vectors = gen_binary_vectors(nb, dimension)
    entities = [
        {"field": "int8", "type": DataType.INT8, "values": [1 for i in range(nb)]},
        {"field": "int64", "type": DataType.INT64, "values": [2 for i in range(nb)]},
        {"field": "float", "type": DataType.FLOAT, "values": [3.0 for i in range(nb)]},
        {"field": "binary_vector", "type": DataType.BINARY_VECTOR, "values": vectors}
    ]
    return raw_vectors, entities


def add_field(entities):
    field = {
        "field": gen_unique_str(), 
        "type": DataType.INT8, 
        "values": [1 for i in range(nb)]
    }
    entities.append(field)
    return entities


def add_vector_field(entities, is_normal=False):
    vectors = gen_vectors(nb, dimension, is_normal)
    field = {
        "field": gen_unique_str(), 
        "type": DataType.VECTOR, 
        "values": vectors
    }
    entities.append(field)
    return entities


def update_fields_metric_type(fields, metric_type):
    if metric_type in ["L2", "IP"]:
        fields["fields"][-1]["type"] = DataType.VECTOR
    else:
        fields["fields"][-1]["type"] = DataType.BINARY_VECTOR
    fields["fields"][-1]["params"]["metric_type"] = metric_type
    return fields


def remove_field(entities):
    del entities[0]
    return entities


def remove_vector_field(entities):
    del entities[-1]
    return entities


def update_field_name(entities, old_name, new_name):
    for item in entities:
        if item["field"] == old_name:
            item["field"] = new_name
    return entities


def update_field_type(entities, old_name, new_name):
    for item in entities:
        if item["type"] == old_name:
            item["type"] = new_name
    return entities


def update_field_value(entities, old_type, new_value):
    for item in entities:
        if item["type"] == old_type:
            for i in item["values"]:
                item["values"][i] = new_value
    return entities


def add_vector_field(nb, dimension):
    field_name = gen_unique_str()
    field = {
        "field": field_name,
        "type": DataType.VECTOR,
        "values": gen_vectors(nb, dimension)
    }
    return field_name
        

def gen_segment_sizes():
    sizes = [
            1,
            2,
            1024,
            4096
    ]
    return sizes


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


def gen_invalid_strs():
    strings = [
            1,
            [1],
            None,
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
    return strings


def gen_invalid_field_types():
    field_types = [
            1,
            "=c",
            0,
            None,
            "",
            "a".join("a" for i in range(256))
    ]
    return field_types


def gen_invalid_metric_types():
    metric_types = [
            1,
            "=c",
            0,
            None,
            "",
            "a".join("a" for i in range(256))
    ]
    return metric_types


def gen_invalid_ints():
    top_ks = [
            1.0,
            None,
            "stringg",
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


def gen_invaild_search_params():
    invalid_search_key = 100
    search_params = []
    for index_type in all_index_types:
        if index_type == "FLAT":
            continue
        search_params.append({"index_type": index_type, "search_param": {"invalid_key": invalid_search_key}})
        if index_type in ["IVFFLAT", "IVFSQ8", "IVFSQ8H", "IVFPQ"]:
            for nprobe in gen_invalid_params():
                ivf_search_params = {"index_type": index_type, "search_param": {"nprobe": nprobe}}
                search_params.append(ivf_search_params)
        elif index_type == "HNSW":
            for ef in gen_invalid_params():
                hnsw_search_param = {"index_type": index_type, "search_param": {"ef": ef}}
                search_params.append(hnsw_search_param)
        elif index_type == "RNSG":
            for search_length in gen_invalid_params():
                nsg_search_param = {"index_type": index_type, "search_param": {"search_length": search_length}}
                search_params.append(nsg_search_param)
            search_params.append({"index_type": index_type, "search_param": {"invalid_key": 100}})
        elif index_type == "ANNOY":
            for search_k in gen_invalid_params():
                if isinstance(search_k, int):
                    continue
                annoy_search_param = {"index_type": index_type, "search_param": {"search_k": search_k}}
                search_params.append(annoy_search_param)
    return search_params


def gen_invalid_index():
    index_params = []
    for index_type in gen_invalid_index_types():
        index_param = {"index_type": index_type, "params": {"nlist": 1024}}
        index_params.append(index_param)
    for nlist in gen_invalid_params():
        index_param = {"index_type": "IVFFLAT", "params": {"nlist": nlist}}
        index_params.append(index_param)
    for M in gen_invalid_params():
        index_param = {"index_type": "HNSW", "params": {"M": M, "efConstruction": 100}}
        index_params.append(index_param)
    for efConstruction in gen_invalid_params():
        index_param = {"index_type": "HNSW", "params": {"M": 16, "efConstruction": efConstruction}}
        index_params.append(index_param)
    for search_length in gen_invalid_params():
        index_param = {"index_type": "RNSG",
                       "params": {"search_length": search_length, "out_degree": 40, "candidate_pool_size": 50,
                                       "knng": 100}}
        index_params.append(index_param)
    for out_degree in gen_invalid_params():
        index_param = {"index_type": "RNSG",
                       "params": {"search_length": 100, "out_degree": out_degree, "candidate_pool_size": 50,
                                       "knng": 100}}
        index_params.append(index_param)
    for candidate_pool_size in gen_invalid_params():
        index_param = {"index_type": "RNSG", "params": {"search_length": 100, "out_degree": 40,
                                                                     "candidate_pool_size": candidate_pool_size,
                                                                     "knng": 100}}
        index_params.append(index_param)
    index_params.append({"index_type": "IVFFLAT", "params": {"invalid_key": 1024}})
    index_params.append({"index_type": "HNSW", "params": {"invalid_key": 16, "efConstruction": 100}})
    index_params.append({"index_type": "RNSG",
                         "params": {"invalid_key": 100, "out_degree": 40, "candidate_pool_size": 300,
                                         "knng": 100}})
    for invalid_n_trees in gen_invalid_params():
        index_params.append({"index_type": "ANNOY", "params": {"n_trees": invalid_n_trees}})

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
        if index_type == "FLAT":
            index_params.append({"index_type": index_type, "index_param": {"nlist": 1024}})
        elif index_type in ["IVFFLAT", "IVFSQ8", "IVFSQ8H"]:
            ivf_params = [{"index_type": index_type, "index_param": {"nlist": nlist}} \
                          for nlist in nlists]
            index_params.extend(ivf_params)
        elif index_type == "IVFPQ":
            IVFPQ_params = [{"index_type": index_type, "index_param": {"nlist": nlist, "m": m}} \
                        for nlist in nlists \
                        for m in pq_ms]
            index_params.extend(IVFPQ_params)
        elif index_type == "HNSW":
            hnsw_params = [{"index_type": index_type, "index_param": {"M": M, "efConstruction": efConstruction}} \
                           for M in Ms \
                           for efConstruction in efConstructions]
            index_params.extend(hnsw_params)
        elif index_type == "RNSG":
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
        index_params.append({"index_type": all_index_types[i], "params": params[i]})
    return index_params


def get_search_param(index_type):
    if index_type in ["FLAT", "IVFFLAT", "IVFSQ8", "IVFSQ8H", "IVFPQ"]:
        return {"nprobe": 32}
    elif index_type == "HNSW":
        return {"ef": 64}
    elif index_type == "RNSG":
        return {"search_length": 100}
    elif index_type == "ANNOY":
        return {"search_k": 100}

    else:
        logging.getLogger().info("Invalid index_type.")


def assert_equal_vector(v1, v2):
    if len(v1) != len(v2):
        assert False
    for i in range(len(v1)):
        assert abs(v1[i] - v2[i]) < epsilon


def restart_server(helm_release_name):
    res = True
    timeout = 120
    from kubernetes import client, config
    client.rest.logger.setLevel(logging.WARNING)

    namespace = "milvus"
    # service_name = "%s.%s.svc.cluster.local" % (helm_release_name, namespace)
    config.load_kube_config()
    v1 = client.CoreV1Api()
    pod_name = None
    # config_map_names = v1.list_namespaced_config_map(namespace, pretty='true')
    # body = {"replicas": 0}
    pods = v1.list_namespaced_pod(namespace)
    for i in pods.items:
        if i.metadata.name.find(helm_release_name) != -1 and i.metadata.name.find("mysql") == -1:
            pod_name = i.metadata.name
            break
            # v1.patch_namespaced_config_map(config_map_name, namespace, body, pretty='true')
    # status_res = v1.read_namespaced_service_status(helm_release_name, namespace, pretty='true')
    # print(status_res)
    if pod_name is not None:
        try:
            v1.delete_namespaced_pod(pod_name, namespace)
        except Exception as e:
            logging.error(str(e))
            logging.error("Exception when calling CoreV1Api->delete_namespaced_pod")
            res = False
            return res
        time.sleep(5)
        # check if restart successfully
        pods = v1.list_namespaced_pod(namespace)
        for i in pods.items:
            pod_name_tmp = i.metadata.name
            if pod_name_tmp.find(helm_release_name) != -1:
                logging.debug(pod_name_tmp)
                start_time = time.time()
                while time.time() - start_time > timeout:
                    status_res = v1.read_namespaced_pod_status(pod_name_tmp, namespace, pretty='true')
                    if status_res.status.phase == "Running":
                        break
                    time.sleep(1)
                if time.time() - start_time > timeout:
                    logging.error("Restart pod: %s timeout" % pod_name_tmp)
                    res = False
                    return res
    else:
        logging.error("Pod: %s not found" % helm_release_name)
        res = False
    return res
