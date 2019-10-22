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
from runner import Runner

logger = logging.getLogger("milvus_benchmark.local_runner")


class LocalRunner(Runner):
    """run local mode"""
    def __init__(self, ip, port):
        super(LocalRunner, self).__init__()
        self.ip = ip
        self.port = port

    def run(self, definition, run_type=None):
        if run_type == "performance":
            for op_type, op_value in definition.items():
                run_count = op_value["run_count"]
                run_params = op_value["params"]

                if op_type == "insert":
                    for index, param in enumerate(run_params):
                        table_name = param["table_name"]
                        # random_1m_100_512
                        (data_type, table_size, index_file_size, dimension, metric_type) = parser.table_parser(table_name)
                        milvus = MilvusClient(table_name, ip=self.ip, port=self.port)
                        # Check has table or not
                        if milvus.exists_table():
                            milvus.delete()
                            time.sleep(10)
                        milvus.create_table(table_name, dimension, index_file_size, metric_type)
                        res = self.do_insert(milvus, table_name, data_type, dimension, table_size, param["ni_per"])
                        logger.info(res)

                elif op_type == "query":
                    for index, param in enumerate(run_params):
                        logger.info("Definition param: %s" % str(param))
                        table_name = param["dataset"]
                        (data_type, table_size, index_file_size, dimension, metric_type) = parser.table_parser(table_name)

                        milvus = MilvusClient(table_name, ip=self.ip, port=self.port)
                        # parse index info
                        index_types = param["index.index_types"]
                        nlists = param["index.nlists"]
                        # parse top-k, nq, nprobe
                        top_ks, nqs, nprobes = parser.search_params_parser(param)

                        for index_type in index_types:
                            for nlist in nlists:
                                milvus.create_index(index_type, nlist)
                                # preload index
                                milvus.preload_table()
                                # Run query test
                                for nprobe in nprobes:
                                    logger.info("index_type: %s, nlist: %s, metric_type: %s, nprobe: %s" % (index_type, nlist, metric_type, nprobe))
                                    res = self.do_query(milvus, table_name, top_ks, nqs, nprobe, run_count)
                                    headers = [param["dataset"]]
                                    headers.extend([str(top_k) for top_k in top_ks])
                                    utils.print_table(headers, nqs, res)

        elif run_type == "stability":
            for op_type, op_value in definition.items():
                if op_type != "query":
                    logger.warning("invalid operation: %s in accuracy test, only support query operation" % op_type)
                    break
                run_count = op_value["run_count"]
                run_params = op_value["params"]
                nq = 10000

                for index, param in enumerate(run_params):
                    logger.info("Definition param: %s" % str(param))
                    table_name = param["dataset"]
                    (data_type, table_size, index_file_size, dimension, metric_type) = parser.table_parser(table_name)
                    
                    # set default test time
                    if "during_time" not in param:
                        during_time = 100 # seconds
                    else:
                        during_time = int(param["during_time"]) * 60
                    # set default query process num
                    if "query_process_num" not in param:
                        query_process_num = 10
                    else:
                        query_process_num = int(param["query_process_num"])
                    milvus = MilvusClient(table_name)
                    # Check has table or not
                    if not milvus.exists_table():
                        logger.warning("Table %s not existed, continue exec next params ..." % table_name)
                        continue

                    start_time = time.time()
                    insert_vectors = [[random.random() for _ in range(dimension)] for _ in range(nq)]
                    while time.time() < start_time + during_time:
                        processes = []
                        # # do query
                        # for i in range(query_process_num):
                        #     milvus_instance = MilvusClient(table_name)
                        #     top_k = random.choice([x for x in range(1, 100)])
                        #     nq = random.choice([x for x in range(1, 1000)])
                        #     nprobe = random.choice([x for x in range(1, 500)])
                        #     logger.info(nprobe)
                        #     p = Process(target=self.do_query, args=(milvus_instance, table_name, [top_k], [nq], 64, run_count, ))
                        #     processes.append(p)
                        #     p.start()
                        #     time.sleep(0.1)
                        # for p in processes:
                        #     p.join()
                        milvus_instance = MilvusClient(table_name)
                        top_ks = random.sample([x for x in range(1, 100)], 4)
                        nqs = random.sample([x for x in range(1, 1000)], 3)
                        nprobe = random.choice([x for x in range(1, 500)])
                        res = self.do_query(milvus, table_name, top_ks, nqs, nprobe, run_count)
                        # milvus_instance = MilvusClient(table_name)
                        status, res = milvus_instance.insert(insert_vectors, ids=[x for x in range(len(insert_vectors))])
                        if not status.OK():
                            logger.error(status.message)
                        if (time.time() - start_time) % 300 == 0:
                            status = milvus_instance.drop_index()
                            if not status.OK():
                                logger.error(status.message)
                            index_type = random.choice(["flat", "ivf_flat", "ivf_sq8"])
                            status = milvus_instance.create_index(index_type, 16384)
                            if not status.OK():
                                logger.error(status.message)
