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


def get_milvus(handler=None):
    if handler is None:
        handler = "GRPC"
    return Milvus(handler=handler)


def gen_inaccuracy(num):
    return num/255.0


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


def gen_single_vector(dim):
    return [[random.random() for _ in range(dim)]]


def gen_vector(nb, d, seed=np.random.RandomState(1234)):
    xb = seed.rand(nb, d).astype("float32")
    return xb.tolist()


def gen_unique_str(str_value=None):
    prefix = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))
    return "test_"+prefix if str_value is None else str_value+"_"+prefix


def get_current_day():
    return time.strftime('%Y-%m-%d', time.localtime())


def get_last_day(day):
    tmp = datetime.datetime.now()-datetime.timedelta(days=day)
    return tmp.strftime('%Y-%m-%d')


def get_next_day(day):
    tmp = datetime.datetime.now()+datetime.timedelta(days=day)
    return tmp.strftime('%Y-%m-%d')


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
            "\n",
            "\t",
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
            "\n",
            "\t",
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


def gen_invalid_table_names():
    table_names = [
            "12-s",
            "12/s",
            " ",
            # "",
            # None,
            "12 s",
            "BB。A",
            "c|c",
            " siede ",
            "(mn)",
            "#12s",
            "pip+",
            "=c",
            "\n",
            "\t",
            "中文",
            "a".join("a" for i in range(256))
    ]
    return table_names


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
            "#12s",
            "pip+",
            "=c",
            "\n",
            "\t",
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
            "#12s",
            "pip+",
            "=c",
            "\n",
            "\t",
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
            "#12s",
            "pip+",
            "=c",
            "\n",
            "\t",
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
            "#12s",
            "pip+",
            "=c",
            "\n",
            "\t",
            "中文",
            "a".join("a" for i in range(256))
    ]
    return invalid_types


def gen_invalid_nlists():
    nlists = [
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
            "#12s",
            "pip+",
            "=c",
            "\n",
            "\t",
            "中文"
    ]
    return nlists


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
            "#12s",
            "pip+",
            "=c",
            "\n",
            "\t",
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
            "#12s",
            "pip+",
            "=c",
            "\n",
            "\t",
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
            "#12s",
            "pip+",
            "=c",
            "\n",
            "\t",
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
            "#12s",
            "=c",
            "\n",
            "中文",
    ]
    return invalid_vector_ids



def gen_invalid_query_ranges():
    query_ranges = [
            [(get_last_day(1), "")],
            [(get_current_day(), "")],
            [(get_next_day(1), "")],
            [(get_current_day(), get_last_day(1))],
            [(get_next_day(1), get_last_day(1))],
            [(get_next_day(1), get_current_day())],
            [(0, get_next_day(1))],
            [(-1, get_next_day(1))],
            [(1, get_next_day(1))],
            [(100001, get_next_day(1))],
            [(1000000000000001, get_next_day(1))],
            [(None, get_next_day(1))],
            [([1,2,3], get_next_day(1))],
            [((1,2), get_next_day(1))],
            [({"a": 1}, get_next_day(1))],
            [(" ", get_next_day(1))],
            [("", get_next_day(1))],
            [("String", get_next_day(1))],
            [("12-s", get_next_day(1))],
            [("BB。A", get_next_day(1))],
            [(" siede ", get_next_day(1))],
            [("(mn)", get_next_day(1))],
            [("#12s", get_next_day(1))],
            [("pip+", get_next_day(1))],
            [("=c", get_next_day(1))],
            [("\n", get_next_day(1))],
            [("\t", get_next_day(1))],
            [("中文", get_next_day(1))],
            [("a".join("a" for i in range(256)), get_next_day(1))]
    ]
    return query_ranges


def gen_invalid_index_params():
    index_params = []
    for index_type in gen_invalid_index_types():
        index_param = {"index_type": index_type, "nlist": 16384}
        index_params.append(index_param)
    for nlist in gen_invalid_nlists():
        index_param = {"index_type": IndexType.IVFLAT, "nlist": nlist}
        index_params.append(index_param)
    return index_params


def gen_index_params():
    index_params = []
    index_types = [IndexType.FLAT, IndexType.IVFLAT, IndexType.IVF_SQ8, IndexType.IVF_SQ8H, IndexType.IVF_PQ]
    nlists = [1, 16384, 50000]

    def gen_params(index_types, nlists):
        return [ {"index_type": index_type, "nlist": nlist} \
            for index_type in index_types \
                for nlist in nlists]

    return gen_params(index_types, nlists)


def gen_simple_index_params():
    index_params = []
    index_types = [IndexType.FLAT, IndexType.IVFLAT, IndexType.IVF_SQ8, IndexType.IVF_SQ8H, IndexType.IVF_PQ]
    nlists = [1024]

    def gen_params(index_types, nlists):
        return [ {"index_type": index_type, "nlist": nlist} \
            for index_type in index_types \
                for nlist in nlists]

    return gen_params(index_types, nlists)


def assert_has_table(conn, table_name):
    status, ok = conn.has_table(table_name)
    return status.OK() and ok