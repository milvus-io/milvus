import sys
import pdb
import random
import logging
import json
import time, datetime
from multiprocessing import Process
from milvus import Milvus, IndexType, MetricType
import utils

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

METRIC_MAP = {
    "l2": MetricType.L2,
    "ip": MetricType.IP,
    "jaccard": MetricType.JACCARD,
    "hamming": MetricType.HAMMING,
    "sub": MetricType.SUBSTRUCTURE,
    "super": MetricType.SUPERSTRUCTURE
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
    def __init__(self, collection_name=None, host=None, port=None, timeout=60):
        self._collection_name = collection_name
        try:
            start_time = time.time()
            if not host:
                host = SERVER_HOST_DEFAULT
            if not port:
                port = SERVER_PORT_DEFAULT
            logger.debug(host)
            logger.debug(port)
            # retry connect for remote server
            i = 0
            while time.time() < start_time + timeout:
                try:
                    self._milvus = Milvus(
                        host = host,
                        port = port, 
                        try_connect = False, 
                        pre_ping = False)
                    if self._milvus.server_status():
                        logger.debug("Try connect times: %d, %s" % (i, round(time.time() - start_time, 2)))
                        break
                except Exception as e:
                    logger.debug("Milvus connect failed: %d times" % i)
                    i = i + 1

            if time.time() > start_time + timeout:
                raise Exception("Server connect timeout")

        except Exception as e:
            raise e
        self._metric_type = None
        if self._collection_name and self.exists_collection():
            self._metric_type = self._metric_type_to_str(self.describe()[1].metric_type)
            self._dimension = self.describe()[1].dimension

    def __str__(self):
        return 'Milvus collection %s' % self._collection_name

    def _metric_type_to_str(self, metric_type):
        for key, value in METRIC_MAP.items():
            if value == metric_type:
                return key
        raise Exception("metric_type: %s mapping not found" % metric_type)

    def set_collection(self, name):
        self._collection_name = name

    def check_status(self, status):
        if not status.OK():
            logger.error(self._collection_name)
            logger.error(status.message)
            logger.error(self._milvus.server_status())
            logger.error(self.count())
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
        if metric_type not in METRIC_MAP.keys():
            raise Exception("Not supported metric_type: %s" % metric_type)
        metric_type = METRIC_MAP[metric_type]
        create_param = {'collection_name': collection_name,
                 'dimension': dimension,
                 'index_file_size': index_file_size, 
                 "metric_type": metric_type}
        status = self._milvus.create_collection(create_param)
        self.check_status(status)

    def create_partition(self, tag_name):
        status = self._milvus.create_partition(self._collection_name, tag_name)
        self.check_status(status)

    def drop_partition(self, tag_name):
        status = self._milvus.drop_partition(self._collection_name, tag_name)
        self.check_status(status)

    def list_partitions(self):
        status, tags = self._milvus.list_partitions(self._collection_name)
        return tags

    @time_wrapper
    def insert(self, X, ids=None, collection_name=None):
        if collection_name is None:
            collection_name = self._collection_name
        status, result = self._milvus.insert(collection_name, X, ids)
        self.check_status(status)
        return status, result

    def insert_rand(self):
        insert_xb = random.randint(1, 100)
        X = [[random.random() for _ in range(self._dimension)] for _ in range(insert_xb)]
        X = utils.normalize(self._metric_type, X)
        count_before = self.count()
        status, ids = self.insert(X)
        self.check_status(status)
        self.flush()
        assert count_before + insert_xb == self.count()

    def get_rand_ids(self, length):
        while True:
            status, stats = self._milvus.get_collection_stats(self._collection_name)
            self.check_status(status)
            segments = stats["partitions"][0]["segments"]
            # random choice one segment
            segment = random.choice(segments)
            status, segment_ids = self._milvus.list_id_in_segment(self._collection_name, segment["name"])
            if not status.OK():
                logger.error(status.message)
                continue
            if len(segment_ids):
                break
        if length >= len(segment_ids):
            logger.debug("Reset length: %d" % len(segment_ids))
            return segment_ids
        return random.sample(segment_ids, length)

    def get_rand_ids_each_segment(self, length):
        res = []
        status, stats = self._milvus.get_collection_stats(self._collection_name)
        self.check_status(status)
        segments = stats["partitions"][0]["segments"]
        segments_num = len(segments)
        # random choice from each segment
        for segment in segments:
            status, segment_ids = self._milvus.list_id_in_segment(self._collection_name, segment["name"])
            self.check_status(status)
            res.extend(segment_ids[:length])
        return segments_num, res

    def get_rand_entities(self, length):
        ids = self.get_rand_ids(length)
        status, get_res = self._milvus.get_entity_by_id(self._collection_name, ids)
        self.check_status(status)
        return ids, get_res

    @time_wrapper
    def get_entities(self, get_ids):
        status, get_res = self._milvus.get_entity_by_id(self._collection_name, get_ids)
        self.check_status(status)
        return get_res

    @time_wrapper
    def delete(self, ids, collection_name=None):
        if collection_name is None:
            collection_name = self._collection_name
        status = self._milvus.delete_entity_by_id(collection_name, ids)
        self.check_status(status)

    def delete_rand(self):
        delete_id_length = random.randint(1, 100)
        count_before = self.count()
        logger.info("%s: length to delete: %d" % (self._collection_name, delete_id_length))
        delete_ids = self.get_rand_ids(delete_id_length)
        self.delete(delete_ids)
        self.flush()
        logger.info("%s: count after delete: %d" % (self._collection_name, self.count()))
        status, get_res = self._milvus.get_entity_by_id(self._collection_name, delete_ids)
        self.check_status(status)
        for item in get_res:
            assert not item
        assert count_before - len(delete_ids) == self.count()

    @time_wrapper
    def flush(self, collection_name=None):
        if collection_name is None:
            collection_name = self._collection_name
        status = self._milvus.flush([collection_name])
        self.check_status(status)

    @time_wrapper
    def compact(self, collection_name=None):
        if collection_name is None:
            collection_name = self._collection_name
        status = self._milvus.compact(collection_name)
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
        status, result = self._milvus.get_index_info(self._collection_name)
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

    def query(self, X, top_k, search_param=None, collection_name=None):
        if collection_name is None:
            collection_name = self._collection_name
        status, result = self._milvus.search(collection_name, top_k, query_records=X, params=search_param)
        self.check_status(status)
        return result

    def query_rand(self):
        top_k = random.randint(1, 100)
        nq = random.randint(1, 100)
        nprobe = random.randint(1, 100)
        search_param = {"nprobe": nprobe}
        ids, X = self.get_rand_entities(nq)
        logger.info("%s, Search nq: %d, top_k: %d, nprobe: %d" % (self._collection_name, nq, top_k, nprobe))
        status, search_res = self._milvus.search(self._collection_name, top_k, query_records=X, params=search_param)
        self.check_status(status)
        # for i, item in enumerate(search_res):
        #     if item[0].id != ids[i]:
        #         logger.warning("The index of search result: %d" % i)
        #         raise Exception("Query failed")

    # @time_wrapper
    # def query_ids(self, top_k, ids, search_param=None):
    #     status, result = self._milvus.search_by_id(self._collection_name, ids, top_k, params=search_param)
    #     self.check_result_ids(result)
    #     return result

    def count(self, name=None):
        if name is None:
            name = self._collection_name
        logger.debug(self._milvus.count_entities(name))
        row_count = self._milvus.count_entities(name)[1]
        if not row_count:
            row_count = 0
        logger.debug("Row count: %d in collection: <%s>" % (row_count, name))
        return row_count

    def drop(self, timeout=120, name=None):
        timeout = int(timeout)
        if name is None:
            name = self._collection_name
        logger.info("Start delete collection: %s" % name)
        status = self._milvus.drop_collection(name)
        self.check_status(status)
        i = 0
        while i < timeout:
            if self.count(name=name):
                time.sleep(1)
                i = i + 1
                continue
            else:
                break
        if i >= timeout:
            logger.error("Delete collection timeout")

    def describe(self):
        # logger.info(self._milvus.get_collection_info(self._collection_name))
        return self._milvus.get_collection_info(self._collection_name)

    def show_collections(self):
        return self._milvus.list_collections()

    def exists_collection(self, collection_name=None):
        if collection_name is None:
            collection_name = self._collection_name
        status, res = self._milvus.has_collection(collection_name)
        # self.check_status(status)
        return res

    def clean_db(self):
        collection_names = self.show_collections()[1]
        for name in collection_names:
            logger.debug(name)
            self.drop(name=name)

    @time_wrapper
    def preload_collection(self):
        status = self._milvus.load_collection(self._collection_name, timeout=3000)
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
