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

logger = logging.getLogger("milvus_benchmark.docker")


class DockerRunner(Runner):
    """run docker mode"""
    def __init__(self, image):
        super(DockerRunner, self).__init__()
        self.image = image
        
    def run(self, definition, run_type=None):
        if run_type == "performance":
            for op_type, op_value in definition.items():
                # run docker mode
                run_count = op_value["run_count"]
                run_params = op_value["params"]
                container = None
                
                if op_type == "insert":
                    if not run_params:
                        logger.debug("No run params")
                        continue
                    for index, param in enumerate(run_params):
                        logger.info("Definition param: %s" % str(param))
                        collection_name = param["collection_name"]
                        volume_name = param["db_path_prefix"]
                        print(collection_name)
                        (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
                        for k, v in param.items():
                            if k.startswith("server."):
                                # Update server config
                                utils.modify_config(k, v, type="server", db_slave=None)
                        container = utils.run_server(self.image, test_type="remote", volume_name=volume_name, db_slave=None)
                        time.sleep(2)
                        milvus = MilvusClient(collection_name)
                        # Check has collection or not
                        if milvus.exists_collection():
                            milvus.delete()
                            time.sleep(10)
                        milvus.create_collection(collection_name, dimension, index_file_size, metric_type)
                        # debug
                        # milvus.create_index("ivf_sq8", 16384)
                        res = self.do_insert(milvus, collection_name, data_type, dimension, collection_size, param["ni_per"])
                        logger.info(res)
                        # wait for file merge
                        time.sleep(collection_size * dimension / 5000000)
                        # Clear up
                        utils.remove_container(container)

                elif op_type == "query":
                    for index, param in enumerate(run_params):
                        logger.info("Definition param: %s" % str(param))
                        collection_name = param["dataset"]
                        volume_name = param["db_path_prefix"]
                        (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
                        for k, v in param.items():
                            if k.startswith("server."):                   
                                utils.modify_config(k, v, type="server")
                        container = utils.run_server(self.image, test_type="remote", volume_name=volume_name, db_slave=None)
                        time.sleep(2)
                        milvus = MilvusClient(collection_name)
                        logger.debug(milvus.show_collections())
                        # Check has collection or not
                        if not milvus.exists_collection():
                            logger.warning("Table %s not existed, continue exec next params ..." % collection_name)
                            continue
                        # parse index info
                        index_types = param["index.index_types"]
                        nlists = param["index.nlists"]
                        # parse top-k, nq, nprobe
                        top_ks, nqs, nprobes = parser.search_params_parser(param)
                        for index_type in index_types:
                            for nlist in nlists:
                                result = milvus.describe_index()
                                logger.info(result)
                                # milvus.drop_index()
                                # milvus.create_index(index_type, nlist)
                                result = milvus.describe_index()
                                logger.info(result)
                                logger.info(milvus.count())
                                # preload index
                                milvus.preload_collection()
                                logger.info("Start warm up query")
                                res = self.do_query(milvus, collection_name, [1], [1], 1, 1)
                                logger.info("End warm up query")
                                # Run query test
                                for nprobe in nprobes:
                                    logger.info("index_type: %s, nlist: %s, metric_type: %s, nprobe: %s" % (index_type, nlist, metric_type, nprobe))
                                    res = self.do_query(milvus, collection_name, top_ks, nqs, nprobe, run_count)
                                    headers = ["Nq/Top-k"]
                                    headers.extend([str(top_k) for top_k in top_ks])
                                    utils.print_collection(headers, nqs, res)
                        utils.remove_container(container)

        elif run_type == "insert_performance":
            for op_type, op_value in definition.items():
                # run docker mode
                run_count = op_value["run_count"]
                run_params = op_value["params"]
                container = None
                if not run_params:
                    logger.debug("No run params")
                    continue
                for index, param in enumerate(run_params):
                    logger.info("Definition param: %s" % str(param))
                    collection_name = param["collection_name"]
                    volume_name = param["db_path_prefix"]
                    print(collection_name)
                    (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
                    for k, v in param.items():
                        if k.startswith("server."):
                            # Update server config
                            utils.modify_config(k, v, type="server", db_slave=None)
                    container = utils.run_server(self.image, test_type="remote", volume_name=volume_name, db_slave=None)
                    time.sleep(2)
                    milvus = MilvusClient(collection_name)
                    # Check has collection or not
                    if milvus.exists_collection():
                        milvus.delete()
                        time.sleep(10)
                    milvus.create_collection(collection_name, dimension, index_file_size, metric_type)
                    # debug
                    # milvus.create_index("ivf_sq8", 16384)
                    res = self.do_insert(milvus, collection_name, data_type, dimension, collection_size, param["ni_per"])
                    logger.info(res)
                    # wait for file merge
                    time.sleep(collection_size * dimension / 5000000)
                    # Clear up
                    utils.remove_container(container)

        elif run_type == "search_performance":
            for op_type, op_value in definition.items():
                # run docker mode
                run_count = op_value["run_count"]
                run_params = op_value["params"]
                container = None
                for index, param in enumerate(run_params):
                    logger.info("Definition param: %s" % str(param))
                    collection_name = param["dataset"]
                    volume_name = param["db_path_prefix"]
                    (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
                    for k, v in param.items():
                        if k.startswith("server."):                   
                            utils.modify_config(k, v, type="server")
                    container = utils.run_server(self.image, test_type="remote", volume_name=volume_name, db_slave=None)
                    time.sleep(2)
                    milvus = MilvusClient(collection_name)
                    logger.debug(milvus.show_collections())
                    # Check has collection or not
                    if not milvus.exists_collection():
                        logger.warning("Table %s not existed, continue exec next params ..." % collection_name)
                        continue
                    # parse index info
                    index_types = param["index.index_types"]
                    nlists = param["index.nlists"]
                    # parse top-k, nq, nprobe
                    top_ks, nqs, nprobes = parser.search_params_parser(param)
                    for index_type in index_types:
                        for nlist in nlists:
                            result = milvus.describe_index()
                            logger.info(result)
                            # milvus.drop_index()
                            # milvus.create_index(index_type, nlist)
                            result = milvus.describe_index()
                            logger.info(result)
                            logger.info(milvus.count())
                            # preload index
                            milvus.preload_collection()
                            logger.info("Start warm up query")
                            res = self.do_query(milvus, collection_name, [1], [1], 1, 1)
                            logger.info("End warm up query")
                            # Run query test
                            for nprobe in nprobes:
                                logger.info("index_type: %s, nlist: %s, metric_type: %s, nprobe: %s" % (index_type, nlist, metric_type, nprobe))
                                res = self.do_query(milvus, collection_name, top_ks, nqs, nprobe, run_count)
                                headers = ["Nq/Top-k"]
                                headers.extend([str(top_k) for top_k in top_ks])
                                utils.print_collection(headers, nqs, res)
                    utils.remove_container(container)

        elif run_type == "accuracy":
            """
            {
                "dataset": "random_50m_1024_512", 
                "index.index_types": ["flat", ivf_flat", "ivf_sq8"],
                "index.nlists": [16384],
                "nprobes": [1, 32, 128], 
                "nqs": [100],
                "top_ks": [1, 64], 
                "server.use_blas_threshold": 1100, 
                "server.cpu_cache_capacity": 256
            }
            """
            for op_type, op_value in definition.items():
                if op_type != "query":
                    logger.warning("invalid operation: %s in accuracy test, only support query operation" % op_type)
                    break
                run_count = op_value["run_count"]
                run_params = op_value["params"]
                container = None

                for index, param in enumerate(run_params):
                    logger.info("Definition param: %s" % str(param))
                    collection_name = param["dataset"]
                    sift_acc = False
                    if "sift_acc" in param:
                        sift_acc = param["sift_acc"]
                    (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
                    for k, v in param.items():
                        if k.startswith("server."):                   
                            utils.modify_config(k, v, type="server")
                    volume_name = param["db_path_prefix"]
                    container = utils.run_server(self.image, test_type="remote", volume_name=volume_name, db_slave=None)
                    time.sleep(2)
                    milvus = MilvusClient(collection_name)
                    # Check has collection or not
                    if not milvus.exists_collection():
                        logger.warning("Table %s not existed, continue exec next params ..." % collection_name)
                        continue

                    # parse index info
                    index_types = param["index.index_types"]
                    nlists = param["index.nlists"]
                    # parse top-k, nq, nprobe
                    top_ks, nqs, nprobes = parser.search_params_parser(param)
                    if sift_acc is True:
                        # preload groundtruth data
                        true_ids_all = self.get_groundtruth_ids(collection_size)
                    acc_dict = {}
                    for index_type in index_types:
                        for nlist in nlists:
                            result = milvus.describe_index()
                            logger.info(result)
                            milvus.create_index(index_type, nlist)
                            # preload index
                            milvus.preload_collection()
                            # Run query test
                            for nprobe in nprobes:
                                logger.info("index_type: %s, nlist: %s, metric_type: %s, nprobe: %s" % (index_type, nlist, metric_type, nprobe))
                                for top_k in top_ks:
                                    for nq in nqs:
                                        result_ids = []
                                        id_prefix = "%s_index_%s_nlist_%s_metric_type_%s_nprobe_%s_top_k_%s_nq_%s" % \
                                                    (collection_name, index_type, nlist, metric_type, nprobe, top_k, nq)
                                        if sift_acc is False:
                                            self.do_query_acc(milvus, collection_name, top_k, nq, nprobe, id_prefix)
                                            if index_type != "flat":
                                                # Compute accuracy
                                                base_name = "%s_index_flat_nlist_%s_metric_type_%s_nprobe_%s_top_k_%s_nq_%s" % \
                                                    (collection_name, nlist, metric_type, nprobe, top_k, nq)
                                                avg_acc = self.compute_accuracy(base_name, id_prefix)
                                                logger.info("Query: <%s> accuracy: %s" % (id_prefix, avg_acc))
                                        else:
                                            result_ids, result_distances = self.do_query_ids(milvus, collection_name, top_k, nq, nprobe)
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
                    # # print accuracy collection
                    # headers = [collection_name]
                    # headers.extend([str(top_k) for top_k in top_ks])
                    # utils.print_collection(headers, nqs, res)

                    # remove container, and run next definition
                    logger.info("remove container, and run next definition")
                    utils.remove_container(container)

        elif run_type == "stability":
            for op_type, op_value in definition.items():
                if op_type != "query":
                    logger.warning("invalid operation: %s in accuracy test, only support query operation" % op_type)
                    break
                run_count = op_value["run_count"]
                run_params = op_value["params"]
                container = None
                for index, param in enumerate(run_params):
                    logger.info("Definition param: %s" % str(param))
                    collection_name = param["dataset"]
                    index_type = param["index_type"]
                    volume_name = param["db_path_prefix"]
                    (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
                    
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

                    for k, v in param.items():
                        if k.startswith("server."):                   
                            utils.modify_config(k, v, type="server")

                    container = utils.run_server(self.image, test_type="remote", volume_name=volume_name, db_slave=None)
                    time.sleep(2)
                    milvus = MilvusClient(collection_name)
                    # Check has collection or not
                    if not milvus.exists_collection():
                        logger.warning("Table %s not existed, continue exec next params ..." % collection_name)
                        continue

                    start_time = time.time()
                    insert_vectors = [[random.random() for _ in range(dimension)] for _ in range(10000)]
                    i = 0
                    while time.time() < start_time + during_time:
                        i = i + 1
                        processes = []
                        # do query
                        # for i in range(query_process_num):
                        #     milvus_instance = MilvusClient(collection_name)
                        #     top_k = random.choice([x for x in range(1, 100)])
                        #     nq = random.choice([x for x in range(1, 100)])
                        #     nprobe = random.choice([x for x in range(1, 1000)])
                        #     # logger.info("index_type: %s, nlist: %s, metric_type: %s, nprobe: %s" % (index_type, nlist, metric_type, nprobe))
                        #     p = Process(target=self.do_query, args=(milvus_instance, collection_name, [top_k], [nq], [nprobe], run_count, ))
                        #     processes.append(p)
                        #     p.start()
                        #     time.sleep(0.1)
                        # for p in processes:
                        #     p.join()
                        milvus_instance = MilvusClient(collection_name)
                        top_ks = random.sample([x for x in range(1, 100)], 3)
                        nqs = random.sample([x for x in range(1, 1000)], 3)
                        nprobe = random.choice([x for x in range(1, 500)])
                        res = self.do_query(milvus, collection_name, top_ks, nqs, nprobe, run_count)
                        if i % 10 == 0:
                            status, res = milvus_instance.insert(insert_vectors, ids=[x for x in range(len(insert_vectors))])
                            if not status.OK():
                                logger.error(status)
                            # status = milvus_instance.drop_index()
                            # if not status.OK():
                            #     logger.error(status)
                            # index_type = random.choice(["flat", "ivf_flat", "ivf_sq8"])
                            milvus_instance.create_index(index_type, 16384)
                            result = milvus.describe_index()
                            logger.info(result)
                            # milvus_instance.create_index("ivf_sq8", 16384)
                    utils.remove_container(container)

        else:
            logger.warning("Run type: %s not supported" % run_type)

