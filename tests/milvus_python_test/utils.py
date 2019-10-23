# STL imports
import random
import string
import struct
import sys
import time, datetime
import copy
import numpy as np
from utils import *
from milvus import Milvus, IndexType, MetricType


def gen_inaccuracy(num):
    return num/255.0

def gen_vectors(num, dim):
    return [[random.random() for _ in range(dim)] for _ in range(num)]


def gen_single_vector(dim):
    return [[random.random() for _ in range(dim)]]


def gen_vector(nb, d, seed=np.random.RandomState(1234)):
    xb = seed.rand(nb, d).astype("float32")
    return xb.tolist()


def gen_unique_str(str=None):
    prefix = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))
    return prefix if str is None else str + "_" + prefix


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
            "255.0.0.0",
            "255.255.0.0",
            "255.255.255.0",
            "255.255.255.255",
            "127.0.0",
            "123.0.0.2",
            "12-s",
            " ",
            "12 s",
            "BB。A",
            " siede ",
            "(mn)",
            "\n",
            "\t",
            "中文",
            "a".join("a" for i in range(256))
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
    port = 19530

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
            "tcp:// :%s" % port,
            "tcp://123.0.0.1:%s" % port,
            "tcp://127.0.0:%s" % port,
            "tcp://255.0.0.0:%s" % port,
            "tcp://255.255.0.0:%s" % port,
            "tcp://255.255.255.0:%s" % port,
            "tcp://255.255.255.255:%s" % port,
            "tcp://\n:%s" % port,

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
    index_types = [IndexType.FLAT, IndexType.IVFLAT, IndexType.IVF_SQ8, IndexType.IVF_SQ8H]
    nlists = [1, 16384, 50000]

    def gen_params(index_types, nlists):
        return [ {"index_type": index_type, "nlist": nlist} \
            for index_type in index_types \
                for nlist in nlists]

    return gen_params(index_types, nlists)

def gen_simple_index_params():
    index_params = []
    index_types = [IndexType.FLAT, IndexType.IVFLAT, IndexType.IVF_SQ8, IndexType.IVF_SQ8H]
    nlists = [16384]

    def gen_params(index_types, nlists):
        return [ {"index_type": index_type, "nlist": nlist} \
            for index_type in index_types \
                for nlist in nlists]

    return gen_params(index_types, nlists)


def assert_has_table(conn, table_name):
    status, ok = conn.has_table(table_name)
    return status.OK() and ok
    

if __name__ == "__main__":
    import numpy

    dim = 128
    nq = 10000
    table = "test"


    file_name = '/poc/yuncong/ann_1000m/query.npy'
    data = np.load(file_name)
    vectors = data[0:nq].tolist()
    # print(vectors)

    connect = Milvus()
    # connect.connect(host="192.168.1.27")
    # print(connect.show_tables())
    # print(connect.get_table_row_count(table))
    # sys.exit()
    connect.connect(host="127.0.0.1")
    connect.delete_table(table)
    # sys.exit()
    # time.sleep(2)
    print(connect.get_table_row_count(table))
    param = {'table_name': table,
         'dimension': dim,
         'metric_type': MetricType.L2,
         'index_file_size': 10}
    status = connect.create_table(param)
    print(status)
    print(connect.get_table_row_count(table))
    # add vectors
    for i in range(10):
        status, ids = connect.add_vectors(table, vectors)
        print(status)
        print(ids[0])
    # print(ids[0])
    index_params = {"index_type": IndexType.IVFLAT, "nlist": 16384}
    status = connect.create_index(table, index_params)
    print(status)
    # sys.exit()
    query_vec = [vectors[0]]
    # print(numpy.inner(numpy.array(query_vec[0]), numpy.array(query_vec[0])))
    top_k = 12
    nprobe = 1
    for i in range(2):
        result = connect.search_vectors(table, top_k, nprobe, query_vec)
        print(result)
    sys.exit()


    table = gen_unique_str("test_add_vector_with_multiprocessing")
    uri = "tcp://%s:%s" % (args["ip"], args["port"])
    param = {'table_name': table,
             'dimension': dim,
             'index_file_size': index_file_size}
    # create table
    milvus = Milvus()
    milvus.connect(uri=uri)
    milvus.create_table(param)
    vector = gen_single_vector(dim)

    process_num = 4
    loop_num = 10
    processes = []
    # with dependent connection
    def add(milvus):
        i = 0
        while i < loop_num:
            status, ids = milvus.add_vectors(table, vector)
            i = i + 1
    for i in range(process_num):
        milvus = Milvus()
        milvus.connect(uri=uri)
        p = Process(target=add, args=(milvus,))
        processes.append(p)
        p.start()
        time.sleep(0.2)
    for p in processes:
        p.join()
    time.sleep(3)
    status, count = milvus.get_table_row_count(table)
    assert count == process_num * loop_num