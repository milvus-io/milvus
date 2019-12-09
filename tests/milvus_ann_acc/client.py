import pdb
import random
import logging
import json
import time, datetime
from multiprocessing import Process
import numpy
import sklearn.preprocessing
from milvus import Milvus, IndexType, MetricType

logger = logging.getLogger("milvus_acc.client")

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
    def __init__(self, table_name=None, host=None, port=None):
        self._milvus = Milvus()
        self._table_name = table_name
        try:
            if not host:
                self._milvus.connect(
                    host = SERVER_HOST_DEFAULT,
                    port = SERVER_PORT_DEFAULT)
            else:
                self._milvus.connect(
                    host = host,
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
        self._metric_type = metric_type
        create_param = {'table_name': table_name,
                 'dimension': dimension,
                 'index_file_size': index_file_size, 
                 "metric_type": metric_type}
        status = self._milvus.create_table(create_param)
        self.check_status(status)

    @time_wrapper
    def insert(self, X, ids):
        if self._metric_type == MetricType.IP:
            logger.info("Set normalize for metric_type: Inner Product")
            X = sklearn.preprocessing.normalize(X, axis=1, norm='l2')
        X = X.astype(numpy.float32)
        status, result = self._milvus.add_vectors(self._table_name, X.tolist(), ids=ids)
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
        elif index_type == "ivf_sq8h":
            index_type = IndexType.IVF_SQ8H
        elif index_type == "nsg":
            index_type = IndexType.NSG
        elif index_type == "ivf_pq":
            index_type = IndexType.IVF_PQ
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
        if self._metric_type == MetricType.IP:
            logger.info("Set normalize for metric_type: Inner Product")
            X = sklearn.preprocessing.normalize(X, axis=1, norm='l2')
        X = X.astype(numpy.float32)
        status, results = self._milvus.search_vectors(self._table_name, top_k, nprobe, X.tolist())
        self.check_status(status)
        ids = []
        for result in results:
            tmp_ids = []
            for item in result:
                tmp_ids.append(item.id)
            ids.append(tmp_ids)
        return ids

    def count(self):
        return self._milvus.get_table_row_count(self._table_name)[1]

    def delete(self, table_name):
        logger.info("Start delete table: %s" % table_name)
        return self._milvus.delete_table(table_name)

    def describe(self):
        return self._milvus.describe_table(self._table_name)

    def exists_table(self, table_name):
        return self._milvus.has_table(table_name)

    def get_server_version(self):
        status, res = self._milvus.server_version()
        self.check_status(status)
        return res

    @time_wrapper
    def preload_table(self):
        return self._milvus.preload_table(self._table_name)
