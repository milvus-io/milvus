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
                        logger.info(milvus.describe())
                        logger.info(milvus.describe_index())
                        logger.info(milvus.count())
                        logger.info(milvus.show_tables())
                        # parse index info
                        index_types = param["index.index_types"]
                        nlists = param["index.nlists"]
                        # parse top-k, nq, nprobe
                        top_ks, nqs, nprobes = parser.search_params_parser(param)
                        # milvus.drop_index()

                        for index_type in index_types:
                            for nlist in nlists:
                                # milvus.create_index(index_type, nlist)
                                # preload index
                                logger.info("Start preloading table")
                                milvus.preload_table()
                                logger.info("End preloading table")
                                # Run query test
                                logger.info("Start warm up query")
                                res = self.do_query(milvus, table_name, [1], [1], 1, 2)
                                logger.info("End warm up query")
                                for nprobe in nprobes:
                                    logger.info("index_type: %s, nlist: %s, metric_type: %s, nprobe: %s" % (index_type, nlist, metric_type, nprobe))
                                    res = self.do_query(milvus, table_name, top_ks, nqs, nprobe, run_count)
                                    headers = ["nq/topk"]
                                    headers.extend([str(top_k) for top_k in top_ks])
                                    utils.print_table(headers, nqs, res)

        elif run_type == "accuracy":
            for op_type, op_value in definition.items():
                if op_type != "query":
                    logger.warning("invalid operation: %s in accuracy test, only support query operation" % op_type)
                    break
                run_count = op_value["run_count"]
                run_params = op_value["params"]
                for index, param in enumerate(run_params):
                    logger.info("Definition param: %s" % str(param))
                    table_name = param["dataset"]
                    sift_acc = False
                    if "sift_acc" in param:
                        sift_acc = param["sift_acc"]
                    (data_type, table_size, index_file_size, dimension, metric_type) = parser.table_parser(table_name)

                    milvus = MilvusClient(table_name, ip=self.ip, port=self.port)
                    logger.debug(milvus.show_tables())
                    # Check has table or not
                    if not milvus.exists_table():
                        logger.warning("Table %s not existed, continue exec next params ..." % table_name)
                        continue

                    # parse index info
                    index_types = param["index.index_types"]
                    nlists = param["index.nlists"]
                    # parse top-k, nq, nprobe
                    top_ks, nqs, nprobes = parser.search_params_parser(param)

                    if sift_acc is True:
                        # preload groundtruth data
                        true_ids_all = self.get_groundtruth_ids(table_size)

                    acc_dict = {}
                    for index_type in index_types:
                        for nlist in nlists:
                            result = milvus.describe_index()
                            logger.info(result)
                            # milvus.drop_index()
                            milvus.create_index(index_type, nlist)
                            # preload index
                            milvus.preload_table()
                            # Run query test
                            for nprobe in nprobes:
                                logger.info("index_type: %s, nlist: %s, metric_type: %s, nprobe: %s" % (index_type, nlist, metric_type, nprobe))
                                for top_k in top_ks:
                                    for nq in nqs:
                                        result_ids = []
                                        id_prefix = "%s_index_%s_nlist_%s_metric_type_%s_nprobe_%s_top_k_%s_nq_%s" % \
                                                    (table_name, index_type, nlist, metric_type, nprobe, top_k, nq)
                                        if sift_acc is False:
                                            self.do_query_acc(milvus, table_name, top_k, nq, nprobe, id_prefix)
                                            if index_type != "flat":
                                                # Compute accuracy
                                                base_name = "%s_index_flat_nlist_%s_metric_type_%s_nprobe_%s_top_k_%s_nq_%s" % \
                                                    (table_name, nlist, metric_type, nprobe, top_k, nq)
                                                avg_acc = self.compute_accuracy(base_name, id_prefix)
                                                logger.info("Query: <%s> accuracy: %s" % (id_prefix, avg_acc))
                                        else:
                                            result_ids, result_distances = self.do_query_ids(milvus, table_name, top_k, nq, nprobe)
                                            debug_file_ids = "0.5.3_result_ids"
                                            debug_file_distances = "0.5.3_result_distances"
                                            with open(debug_file_ids, "w+") as fd:
                                                total = 0
                                                for index, item in enumerate(result_ids):
                                                    true_item = true_ids_all[:nq, :top_k].tolist()[index]
                                                    tmp = set(item).intersection(set(true_item))
                                                    total = total + len(tmp)
                                                    fd.write("query: N-%d, intersection: %d, total: %d\n" % (index, len(tmp), total))
                                                    fd.write("%s\n" % str(item))
                                                    fd.write("%s\n" % str(true_item))
                                            acc_value = self.get_recall_value(true_ids_all[:nq, :top_k].tolist(), result_ids)
                                            logger.info("Query: <%s> accuracy: %s" % (id_prefix, acc_value))
                    # # print accuracy table
                    # headers = [table_name]
                    # headers.extend([str(top_k) for top_k in top_ks])
                    # utils.print_table(headers, nqs, res)

        elif run_type == "stability":
            for op_type, op_value in definition.items():
                if op_type != "query":
                    logger.warning("invalid operation: %s in accuracy test, only support query operation" % op_type)
                    break
                run_count = op_value["run_count"]
                run_params = op_value["params"]
                nq = 100000

                for index, param in enumerate(run_params):
                    logger.info("Definition param: %s" % str(param))
                    table_name = param["dataset"]
                    index_type = param["index_type"]
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
                    milvus = MilvusClient(table_name, ip=self.ip, port=self.port)
                    logger.debug(milvus.show_tables())
                    logger.debug(milvus.describe_index())
                    logger.debug(milvus.count())
                    # Check has table or not
                    if not milvus.exists_table():
                        logger.warning("Table %s not existed, continue exec next params ..." % table_name)
                        continue

                    start_time = time.time()
                    insert_vectors = [[random.random() for _ in range(dimension)] for _ in range(nq)]
                    i = 0
                    while time.time() < start_time + during_time:
                        # processes = []
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
                        i = i + 1
                        milvus_instance = MilvusClient(table_name, ip=self.ip, port=self.port)
                        top_ks = random.sample([x for x in range(1, 100)], 1)
                        nqs = random.sample([x for x in range(1, 200)], 2)
                        nprobe = random.choice([x for x in range(1, 100)])
                        res = self.do_query(milvus_instance, table_name, top_ks, nqs, nprobe, run_count)
                        # milvus_instance = MilvusClient(table_name)
                        status, res = milvus_instance.insert(insert_vectors, ids=[x for x in range(len(insert_vectors))])
                        if not status.OK():
                            logger.error(status.message)
                        logger.debug(milvus.count())
                        res = self.do_query(milvus_instance, table_name, top_ks, nqs, nprobe, run_count)
                        # status = milvus_instance.create_index(index_type, 16384)
