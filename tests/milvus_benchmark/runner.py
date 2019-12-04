import os
import logging
import pdb
import time
import random
from multiprocessing import Process
import numpy as np
from client import MilvusClient
import utils
import parser

logger = logging.getLogger("milvus_benchmark.runner")

SERVER_HOST_DEFAULT = "127.0.0.1"
SERVER_PORT_DEFAULT = 19530
VECTORS_PER_FILE = 1000000
SIFT_VECTORS_PER_FILE = 100000
MAX_NQ = 10001
FILE_PREFIX = "binary_"

# FOLDER_NAME = 'ann_1000m/source_data'
SRC_BINARY_DATA_DIR = '/poc/yuncong/yunfeng/random_data'
SRC_BINARY_DATA_DIR_high = '/test/milvus/raw_data/random'
SIFT_SRC_DATA_DIR = '/poc/yuncong/ann_1000m/'
SIFT_SRC_BINARY_DATA_DIR = SIFT_SRC_DATA_DIR + 'source_data'
SIFT_SRC_GROUNDTRUTH_DATA_DIR = SIFT_SRC_DATA_DIR + 'gnd'

WARM_TOP_K = 1
WARM_NQ = 1
DEFAULT_DIM = 512


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


def gen_file_name(idx, table_dimension, data_type):
    s = "%05d" % idx
    fname = FILE_PREFIX + str(table_dimension) + "d_" + s + ".npy"
    if data_type == "random":
        if table_dimension == 512:
            fname = SRC_BINARY_DATA_DIR+'/'+fname
        elif table_dimension >= 4096:
            fname = SRC_BINARY_DATA_DIR_high+'/'+fname
    elif data_type == "sift":
        fname = SIFT_SRC_BINARY_DATA_DIR+'/'+fname
    return fname


def get_vectors_from_binary(nq, dimension, data_type):
    # use the first file, nq should be less than VECTORS_PER_FILE
    if nq > MAX_NQ:
        raise Exception("Over size nq")
    if data_type == "random":
        file_name = gen_file_name(0, dimension, data_type)
    elif data_type == "sift":
        file_name = SIFT_SRC_DATA_DIR+'/'+'query.npy'
    data = np.load(file_name)
    vectors = data[0:nq].tolist()
    return vectors


class Runner(object):
    def __init__(self):
        pass

    def do_insert(self, milvus, table_name, data_type, dimension, size, ni):
        '''
        @params:
            mivlus: server connect instance
            dimension: table dimensionn
            # index_file_size: size trigger file merge
            size: row count of vectors to be insert
            ni: row count of vectors to be insert each time
            # store_id: if store the ids returned by call add_vectors or not
        @return:
            total_time: total time for all insert operation
            qps: vectors added per second
            ni_time: avarage insert operation time
        '''
        bi_res = {}
        total_time = 0.0
        qps = 0.0
        ni_time = 0.0
        if data_type == "random":
            if dimension == 512:
                vectors_per_file = VECTORS_PER_FILE
            elif dimension == 4096:
                vectors_per_file = 100000
            elif dimension == 16384:
                vectors_per_file = 10000
        elif data_type == "sift":
            vectors_per_file = SIFT_VECTORS_PER_FILE
        if size % vectors_per_file or ni > vectors_per_file:
            raise Exception("Not invalid table size or ni")
        file_num = size // vectors_per_file
        for i in range(file_num):
            file_name = gen_file_name(i, dimension, data_type)
            logger.info("Load npy file: %s start" % file_name)
            data = np.load(file_name)
            logger.info("Load npy file: %s end" % file_name)
            loops = vectors_per_file // ni
            for j in range(loops):
                vectors = data[j*ni:(j+1)*ni].tolist()
                if vectors:
                    ni_start_time = time.time()
                    # start insert vectors
                    start_id = i * vectors_per_file + j * ni
                    end_id = start_id + len(vectors)
                    logger.info("Start id: %s, end id: %s" % (start_id, end_id))
                    ids = [k for k in range(start_id, end_id)]
                    status, ids = milvus.insert(vectors, ids=ids)
                    ni_end_time = time.time()
                    total_time = total_time + ni_end_time - ni_start_time

        qps = round(size / total_time, 2)
        ni_time = round(total_time / (loops * file_num), 2)
        bi_res["total_time"] = round(total_time, 2)
        bi_res["qps"] = qps
        bi_res["ni_time"] = ni_time
        return bi_res

    def do_query(self, milvus, table_name, top_ks, nqs, nprobe, run_count):
        (data_type, table_size, index_file_size, dimension, metric_type) = parser.table_parser(table_name)
        base_query_vectors = get_vectors_from_binary(MAX_NQ, dimension, data_type)

        bi_res = []
        for index, nq in enumerate(nqs):
            tmp_res = []
            vectors = base_query_vectors[0:nq]
            for top_k in top_ks:
                avg_query_time = 0.0
                total_query_time = 0.0
                logger.info("Start query, query params: top-k: {}, nq: {}, actually length of vectors: {}".format(top_k, nq, len(vectors)))
                for i in range(run_count):
                    logger.info("Start run query, run %d of %s" % (i+1, run_count))
                    start_time = time.time()
                    status, query_res = milvus.query(vectors, top_k, nprobe)
                    total_query_time = total_query_time + (time.time() - start_time)
                    if status.code:
                        logger.error("Query failed with message: %s" % status.message)
                avg_query_time = round(total_query_time / run_count, 2)
                logger.info("Avarage query time: %.2f" % avg_query_time)
                tmp_res.append(avg_query_time)
            bi_res.append(tmp_res)
        return bi_res

    def do_query_ids(self, milvus, table_name, top_k, nq, nprobe):
        (data_type, table_size, index_file_size, dimension, metric_type) = parser.table_parser(table_name)
        base_query_vectors = get_vectors_from_binary(MAX_NQ, dimension, data_type)
        vectors = base_query_vectors[0:nq]
        logger.info("Start query, query params: top-k: {}, nq: {}, actually length of vectors: {}".format(top_k, nq, len(vectors)))
        status, query_res = milvus.query(vectors, top_k, nprobe)
        if not status.OK():
            msg = "Query failed with message: %s" % status.message
            raise Exception(msg)
        result_ids = []
        result_distances = []
        for result in query_res:
            tmp = []
            tmp_distance = []
            for item in result:
                tmp.append(item.id)
                tmp_distance.append(item.distance)
            result_ids.append(tmp)
            result_distances.append(tmp_distance)
        return result_ids, result_distances

    def do_query_acc(self, milvus, table_name, top_k, nq, nprobe, id_store_name):
        (data_type, table_size, index_file_size, dimension, metric_type) = parser.table_parser(table_name)
        base_query_vectors = get_vectors_from_binary(MAX_NQ, dimension, data_type)
        vectors = base_query_vectors[0:nq]
        logger.info("Start query, query params: top-k: {}, nq: {}, actually length of vectors: {}".format(top_k, nq, len(vectors)))
        status, query_res = milvus.query(vectors, top_k, nprobe)
        if not status.OK():
            msg = "Query failed with message: %s" % status.message
            raise Exception(msg)
        # if file existed, cover it
        if os.path.isfile(id_store_name):
            os.remove(id_store_name)
        with open(id_store_name, 'a+') as fd:
            for nq_item in query_res:
                for item in nq_item:
                    fd.write(str(item.id)+'\t')
                fd.write('\n')

    # compute and print accuracy
    def compute_accuracy(self, flat_file_name, index_file_name):
        flat_id_list = []; index_id_list = []
        logger.info("Loading flat id file: %s" % flat_file_name)
        with open(flat_file_name, 'r') as flat_id_fd:
            for line in flat_id_fd:
                tmp_list = line.strip("\n").strip().split("\t")
                flat_id_list.append(tmp_list)
        logger.info("Loading index id file: %s" % index_file_name)
        with open(index_file_name) as index_id_fd:
            for line in index_id_fd:
                tmp_list = line.strip("\n").strip().split("\t")
                index_id_list.append(tmp_list)
        if len(flat_id_list) != len(index_id_list):
            raise Exception("Flat index result length: <flat: %s, index: %s> not match, Acc compute exiting ..." % (len(flat_id_list), len(index_id_list)))
        # get the accuracy
        return self.get_recall_value(flat_id_list, index_id_list)

    def get_recall_value(self, flat_id_list, index_id_list):
        """
        Use the intersection length
        """
        sum_radio = 0.0
        for index, item in enumerate(index_id_list):
            tmp = set(item).intersection(set(flat_id_list[index]))
            sum_radio = sum_radio + len(tmp) / len(item)
        return round(sum_radio / len(index_id_list), 3)

    """
    Implementation based on:
        https://github.com/facebookresearch/faiss/blob/master/benchs/datasets.py
    """
    def get_groundtruth_ids(self, table_size):
        fname = GROUNDTRUTH_MAP[str(table_size)]
        fname = SIFT_SRC_GROUNDTRUTH_DATA_DIR + "/" + fname
        a = np.fromfile(fname, dtype='int32')
        d = a[0]
        true_ids = a.reshape(-1, d + 1)[:, 1:].copy()
        return true_ids