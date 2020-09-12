import sys
import pdb
import random
import logging
import json
import time, datetime
from multiprocessing import Process
from milvus import Milvus, IndexType, MetricType

logger = logging.getLogger("milvus_benchmark.client")

SERVER_HOST_DEFAULT = "127.0.0.1"
# SERVER_HOST_DEFAULT = "192.168.1.130"
SERVER_PORT_DEFAULT = 19530
INDEX_MAP = {
    "flat": IndexType.FLAT,
    "ivf_flat": IndexType.IVFLAT,
    "ivf_sq8": IndexType.IVF_SQ8,
    "nsg": IndexType.RNSG,
    "ivf_sq8h": IndexType.IVF_SQ8H,
    "ivf_pq": IndexType.IVF_PQ,
    "hnsw": IndexType.HNSW,
    "annoy": IndexType.ANNOY
}
epsilon = 0.1

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
    def __init__(self, collection_name=None, ip=None, port=None, timeout=60):
        self._collection_name = collection_name
        try:
            i = 1
            start_time = time.time()
            if not ip:
                self._milvus = Milvus(
                    host = SERVER_HOST_DEFAULT,
                    port = SERVER_PORT_DEFAULT)
            else:
                # retry connect for remote server
                while time.time() < start_time + timeout:
                    try:
                        self._milvus = Milvus(
                            host = ip,
                            port = port)
                        if self._milvus.server_status():
                            logger.debug("Try connect times: %d, %s" % (i, round(time.time() - start_time, 2)))
                            break
                    except Exception as e:
                        logger.debug("Milvus connect failed")
                        i = i + 1

        except Exception as e:
            raise e

    def __str__(self):
        return 'Milvus collection %s' % self._collection_name

    def check_status(self, status):
        if not status.OK():
            logger.error(status.message)
            raise Exception("Status not ok")

    def check_result_ids(self, result):
        for index, item in enumerate(result):
            if item[0].distance >= epsilon:
                logger.error(index)
                logger.error(item[0].distance)
                raise Exception("Distance wrong")

    def create_collection(self, collection_name, dimension, index_file_size, metric_type):
        if not self._collection_name:
            self._collection_name = collection_name
        if metric_type == "l2":
            metric_type = MetricType.L2
        elif metric_type == "ip":
            metric_type = MetricType.IP
        elif metric_type == "jaccard":
            metric_type = MetricType.JACCARD
        elif metric_type == "hamming":
            metric_type = MetricType.HAMMING
        elif metric_type == "sub":
            metric_type = MetricType.SUBSTRUCTURE
        elif metric_type == "super":
            metric_type = MetricType.SUPERSTRUCTURE
        else:
            logger.error("Not supported metric_type: %s" % metric_type)
        create_param = {'collection_name': collection_name,
                 'dimension': dimension,
                 'index_file_size': index_file_size, 
                 "metric_type": metric_type}
        status = self._milvus.create_collection(create_param)
        self.check_status(status)

    @time_wrapper
    def insert(self, X, ids=None):
        status, result = self._milvus.add_vectors(self._collection_name, X, ids)
        self.check_status(status)
        return status, result

    @time_wrapper
    def delete_vectors(self, ids):
        status = self._milvus.delete_by_id(self._collection_name, ids)
        self.check_status(status)

    @time_wrapper
    def flush(self):
        status = self._milvus.flush([self._collection_name])
        self.check_status(status)

    @time_wrapper
    def compact(self):
        status = self._milvus.compact(self._collection_name)
        self.check_status(status)

    @time_wrapper
    def create_index(self, index_type, index_param=None):
        index_type = INDEX_MAP[index_type]
        logger.info("Building index start, collection_name: %s, index_type: %s" % (self._collection_name, index_type))
        if index_param:
            logger.info(index_param)
        status = self._milvus.create_index(self._collection_name, index_type, index_param)
        self.check_status(status)

    def describe_index(self):
        status, result = self._milvus.describe_index(self._collection_name)
        self.check_status(status)
        index_type = None
        for k, v in INDEX_MAP.items():
            if result._index_type == v:
                index_type = k
                break
        return {"index_type": index_type, "index_param": result._params}

    def drop_index(self):
        logger.info("Drop index: %s" % self._collection_name)
        return self._milvus.drop_index(self._collection_name)

    @time_wrapper
    def query(self, X, top_k, search_param=None):
        status, result = self._milvus.search_vectors(self._collection_name, top_k, query_records=X, params=search_param)
        self.check_status(status)
        return result

    @time_wrapper
    def query_ids(self, top_k, ids, search_param=None):
        status, result = self._milvus.search_by_ids(self._collection_name, ids, top_k, params=search_param)
        self.check_result_ids(result)
        return result

    def count(self):
        return self._milvus.count_collection(self._collection_name)[1]

    def delete(self, timeout=120):
        timeout = int(timeout)
        logger.info("Start delete collection: %s" % self._collection_name)
        self._milvus.drop_collection(self._collection_name)
        i = 0
        while i < timeout:
            if self.count():
                time.sleep(1)
                i = i + 1
                continue
            else:
                break
        if i >= timeout:
            logger.error("Delete collection timeout")

    def describe(self):
        return self._milvus.describe_collection(self._collection_name)

    def show_collections(self):
        return self._milvus.show_collections()

    def exists_collection(self, collection_name=None):
        if collection_name is None:
            collection_name = self._collection_name
        status, res = self._milvus.has_collection(collection_name)
        # self.check_status(status)
        return res

    @time_wrapper
    def preload_collection(self):
        status = self._milvus.preload_collection(self._collection_name, timeout=3000)
        self.check_status(status)
        return status

    def get_server_version(self):
        status, res = self._milvus.server_version()
        return res

    def get_server_mode(self):
        return self.cmd("mode")

    def get_server_commit(self):
        return self.cmd("build_commit_id")

    def get_server_config(self):
        return json.loads(self.cmd("get_config *"))

    def get_mem_info(self):
        result = json.loads(self.cmd("get_system_info"))
        result_human = {
            # unit: Gb
            "memory_used": round(int(result["memory_used"]) / (1024*1024*1024), 2)
        }
        return result_human

    def cmd(self, command):
        status, res = self._milvus._cmd(command)
        logger.info("Server command: %s, result: %s" % (command, res))
        self.check_status(status)
        return res


def fit(collection_name, X):
    milvus = Milvus()
    milvus.connect(host = SERVER_HOST_DEFAULT, port = SERVER_PORT_DEFAULT) 
    start = time.time()
    status, ids = milvus.add_vectors(collection_name, X)
    end = time.time()
    logger(status, round(end - start, 2))


def fit_concurrent(collection_name, process_num, vectors):
    processes = []

    for i in range(process_num):
        p = Process(target=fit, args=(collection_name, vectors, ))
        processes.append(p)
        p.start()
    for p in processes:             
        p.join()
    

if __name__ == "__main__":
    import numpy
    import sklearn.preprocessing

    # collection_name = "tset_test"
    # # collection_name = "test_tset1"
    # m = MilvusClient(collection_name)
    # m.delete()
    # time.sleep(2)
    # m.create_collection(collection_name, 128, 20, "ip")

    # print(m.describe())
    # print(m.count())
    # print(m.describe_index())
    # # sys.exit()
    # tmp = [[random.random() for _ in range(128)] for _ in range(20000)]
    # tmp1 = sklearn.preprocessing.normalize(tmp, axis=1, norm='l2')
    # print(tmp1[0][0])
    # tmp = [[random.random() for _ in range(128)] for _ in range(20000)]
    # tmp /= numpy.linalg.norm(tmp)
    # print(tmp[0][0])

    # sum_1 = 0
    # sum_2 = 0
    # for item in tmp:
    #     for i in item:
    #         sum_2 = sum_2 + i * i
    #     break
    # for item in tmp1:
    #     for i in item:
    #         sum_1 = sum_1 + i * i
    #     break
    # print(sum_1, sum_2)
    # insert_vectors = tmp.tolist()
    # # print(insert_vectors)
    # for i in range(2):
    #     m.insert(insert_vectors)

    # time.sleep(5)
    # print(m.create_index("ivf_flat", 16384))
    # X = [insert_vectors[0], insert_vectors[1], insert_vectors[2]]
    # top_k = 5
    # nprobe = 1
    # print(m.query(X, top_k, nprobe))

    # # print(m.drop_index())
    # print(m.describe_index())
    # sys.exit()
    # # insert_vectors = [[random.random() for _ in range(128)] for _ in range(100000)]
    # # for i in range(100):
    # #     m.insert(insert_vectors)
    # # time.sleep(5)
    # # print(m.describe_index())
    # # print(m.drop_index())
    # m.create_index("ivf_sq8h", 16384)
    # print(m.count())
    # print(m.describe_index())



    # sys.exit()
    # print(m.create_index("ivf_sq8h", 16384))
    # print(m.count())
    # print(m.describe_index())
    import numpy as np

    # def mmap_fvecs(fname):
    #     x = np.memmap(fname, dtype='int32', mode='r')
    #     d = x[0]
    #     return x.view('float32').reshape(-1, d + 1)[:, 1:]

    # print(mmap_fvecs("/poc/deep1b/deep1B_queries.fvecs"))
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
    # data = sklearn.preprocessing.normalize(data, axis=1, norm='l2')
    # np.save("/test/milvus/deep1b/query.npy", data)
    dimension = 4096
    insert_xb = 10000
    insert_vectors = [[random.random() for _ in range(dimension)] for _ in range(insert_xb)]
    data = sklearn.preprocessing.normalize(insert_vectors, axis=1, norm='l2')
    np.save("/test/milvus/raw_data/random/query_%d.npy" % dimension, data)
    sys.exit()

    total_size = 100000000
    # total_size = 1000000000
    file_size = 100000
    # file_size = 100000
    dimension = 4096
    file_num = total_size // file_size
    for i in range(file_num):
        print(i)
        # fname = "/test/milvus/raw_data/deep1b/binary_96_%05d" % i
        fname = "/test/milvus/raw_data/random/binary_%dd_%05d" % (dimension, i)
        # print(fname, i*file_size, (i+1)*file_size)
        # single_data = data[i*file_size : (i+1)*file_size]
        single_data = [[random.random() for _ in range(dimension)] for _ in range(file_size)]
        single_data = sklearn.preprocessing.normalize(single_data, axis=1, norm='l2')
        np.save(fname, single_data)
