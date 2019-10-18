import pdb
import random
import logging
import json
import sys
import time, datetime
from multiprocessing import Process
from milvus import Milvus, IndexType, MetricType

logger = logging.getLogger("milvus_benchmark.client")

SERVER_HOST_DEFAULT = "127.0.0.1"
SERVER_PORT_DEFAULT = 19530


def time_wrapper(func):
    """
    This decorator prints the execution time for the decorated function.
    """
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logger.info("Milvus {} run in {}s".format(func.__name__, round(end - start, 2)))
        return result
    return wrapper


class MilvusClient(object):
    def __init__(self, table_name=None, ip=None, port=None):
        self._milvus = Milvus()
        self._table_name = table_name
        try:
            if not ip:
                self._milvus.connect(
                    host = SERVER_HOST_DEFAULT,
                    port = SERVER_PORT_DEFAULT)
            else:
                self._milvus.connect(
                    host = ip,
                    port = port)
        except Exception as e:
            raise e

    def __str__(self):
        return 'Milvus table %s' % self._table_name

    def check_status(self, status):
        if not status.OK():
            logger.error(status.message)
            raise Exception("Status not ok")

    def create_table(self, table_name, dimension, index_file_size, metric_type):
        if not self._table_name:
            self._table_name = table_name
        if metric_type == "l2":
            metric_type = MetricType.L2
        elif metric_type == "ip":
            metric_type = MetricType.IP
        else:
            logger.error("Not supported metric_type: %s" % metric_type)
        create_param = {'table_name': table_name,
                 'dimension': dimension,
                 'index_file_size': index_file_size, 
                 "metric_type": metric_type}
        status = self._milvus.create_table(create_param)
        self.check_status(status)

    @time_wrapper
    def insert(self, X, ids=None):
        status, result = self._milvus.add_vectors(self._table_name, X, ids)
        self.check_status(status)
        return status, result

    @time_wrapper
    def create_index(self, index_type, nlist):
        if index_type == "flat":
            index_type = IndexType.FLAT
        elif index_type == "ivf_flat":
            index_type = IndexType.IVFLAT
        elif index_type == "ivf_sq8":
            index_type = IndexType.IVF_SQ8
        elif index_type == "mix_nsg":
            index_type = IndexType.MIX_NSG
        elif index_type == "ivf_sq8h":
            index_type = IndexType.IVF_SQ8H
        index_params = {
            "index_type": index_type,
            "nlist": nlist,
        }
        logger.info("Building index start, table_name: %s, index_params: %s" % (self._table_name, json.dumps(index_params)))
        status = self._milvus.create_index(self._table_name, index=index_params, timeout=6*3600)
        self.check_status(status)

    def describe_index(self):
        return self._milvus.describe_index(self._table_name)

    def drop_index(self):
        logger.info("Drop index: %s" % self._table_name)
        return self._milvus.drop_index(self._table_name)

    @time_wrapper
    def query(self, X, top_k, nprobe):
        status, result = self._milvus.search_vectors(self._table_name, top_k, nprobe, X)
        self.check_status(status)
        return status, result

    def count(self):
        return self._milvus.get_table_row_count(self._table_name)[1]

    def delete(self, timeout=60):
        logger.info("Start delete table: %s" % self._table_name)
        self._milvus.delete_table(self._table_name)
        i = 0
        while i < timeout:
            if self.count():
                time.sleep(1)
                i = i + 1
                continue
            else:
                break
        if i < timeout:
            logger.error("Delete table timeout")

    def describe(self):
        return self._milvus.describe_table(self._table_name)

    def exists_table(self):
        return self._milvus.has_table(self._table_name)

    @time_wrapper
    def preload_table(self):
        return self._milvus.preload_table(self._table_name, timeout=3000)


def fit(table_name, X):
    milvus = Milvus()
    milvus.connect(host = SERVER_HOST_DEFAULT, port = SERVER_PORT_DEFAULT) 
    start = time.time()
    status, ids = milvus.add_vectors(table_name, X)
    end = time.time()
    logger(status, round(end - start, 2))


def fit_concurrent(table_name, process_num, vectors):
    processes = []

    for i in range(process_num):
        p = Process(target=fit, args=(table_name, vectors, ))
        processes.append(p)
        p.start()
    for p in processes:             
        p.join()
    

if __name__ == "__main__":

    # table_name = "sift_2m_20_128_l2"
    table_name = "test_tset1"
    m = MilvusClient(table_name)
    # m.create_table(table_name, 128, 50, "l2")

    print(m.describe())
    # print(m.count())
    # print(m.describe_index())
    insert_vectors = [[random.random() for _ in range(128)] for _ in range(10000)]
    for i in range(5):
        m.insert(insert_vectors)
    print(m.create_index("ivf_sq8h", 16384))
    X = [insert_vectors[0]]
    top_k = 10
    nprobe = 10
    print(m.query(X, top_k, nprobe))

    # # # print(m.drop_index())
    # # print(m.describe_index())
    # # sys.exit()
    # # # insert_vectors = [[random.random() for _ in range(128)] for _ in range(100000)]
    # # # for i in range(100):
    # # #     m.insert(insert_vectors)
    # # # time.sleep(5)
    # # # print(m.describe_index())
    # # # print(m.drop_index())
    # # m.create_index("ivf_sq8h", 16384)
    # print(m.count())
    # print(m.describe_index())



    # sys.exit()
    # print(m.create_index("ivf_sq8h", 16384))
    # print(m.count())
    # print(m.describe_index())
    import numpy as np

    def mmap_fvecs(fname):
        x = np.memmap(fname, dtype='int32', mode='r')
        d = x[0]
        return x.view('float32').reshape(-1, d + 1)[:, 1:]

    print(mmap_fvecs("/poc/deep1b/deep1B_queries.fvecs"))
    # SIFT_SRC_QUERY_DATA_DIR = '/poc/yuncong/ann_1000m'
    # file_name = SIFT_SRC_QUERY_DATA_DIR+'/'+'query.npy'
    # data = numpy.load(file_name)
    # query_vectors = data[0:2].tolist()
    # print(len(query_vectors))
    # results = m.query(query_vectors, 10, 10)
    # result_ids = []
    # for result in results[1]:
    #     tmp = []
    #     for item in result:
    #         tmp.append(item.id)
    #     result_ids.append(tmp)
    # print(result_ids[0][:10])
    # # gt
    # file_name = SIFT_SRC_QUERY_DATA_DIR+"/gnd/"+"idx_1M.ivecs"
    # a = numpy.fromfile(file_name, dtype='int32')
    # d = a[0]
    # true_ids = a.reshape(-1, d + 1)[:, 1:].copy()
    # print(true_ids[:3, :2])

    # print(len(true_ids[0]))
    # import numpy as np
    # import sklearn.preprocessing

    # def mmap_fvecs(fname):
    #     x = np.memmap(fname, dtype='int32', mode='r')
    #     d = x[0]
    #     return x.view('float32').reshape(-1, d + 1)[:, 1:]

    # data = mmap_fvecs("/poc/deep1b/deep1B_queries.fvecs")
    # print(data[0], len(data[0]), len(data))

    # total_size = 10000
    # # total_size = 1000000000
    # file_size = 1000
    # # file_size = 100000
    # file_num = total_size // file_size
    # for i in range(file_num):
    #     fname = "/test/milvus/raw_data/deep1b/binary_96_%05d" % i
    #     print(fname, i*file_size, (i+1)*file_size)
    #     single_data = data[i*file_size : (i+1)*file_size]
    #     single_data = sklearn.preprocessing.normalize(single_data, axis=1, norm='l2')
    #     np.save(fname, single_data)
