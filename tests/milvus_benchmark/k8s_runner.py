import os
import logging
import pdb
import time
import re
import random
import traceback
import json
from multiprocessing import Process
import numpy as np
from yaml import full_load, dump
from client import MilvusClient
import parser
from runner import Runner
from milvus_metrics.api import report
from milvus_metrics.models import Env, Hardware, Server, Metric
import utils

logger = logging.getLogger("milvus_benchmark.k8s_runner")
namespace = "milvus"
DELETE_INTERVAL_TIME = 5
# INSERT_INTERVAL = 100000
INSERT_INTERVAL = 50000
timestamp = int(time.time())
default_path = "/var/lib/milvus"


class K8sRunner(Runner):
    """run docker mode"""
    def __init__(self):
        super(K8sRunner, self).__init__()
        self.name = utils.get_unique_name()
        self.host = None
        self.ip = None
        self.hostname = None
        self.env_value = None
        
    def init_env(self, server_config, server_host, image_type, image_tag):
        self.hostname = server_host
        # update values
        helm_path = os.path.join(os.getcwd(), "../milvus-helm")
        values_file_path = helm_path+"/values.yaml"
        if not os.path.exists(values_file_path):
            raise Exception("File %s not existed" % values_file_path)
        utils.update_values(values_file_path, server_host, server_config)
        try:
            logger.debug("Start install server")
            self.host, self.ip = utils.helm_install_server(helm_path, image_tag, image_type, self.name, namespace)
        except Exception as e:
            logger.error("Helm install server failed: %s" % str(e))
            logger.error(traceback.format_exc())
            self.clean_up()
            return False
        # for debugging
        # self.host = "192.168.1.101"
        if not self.host:
            logger.error("Helm install server failed")
            self.clean_up()
            return False
        return True

    def clean_up(self):
        logger.debug(self.name)
        utils.helm_del_server(self.name, namespace)

    def report_wrapper(self, milvus_instance, env_value, hostname, collection_info, index_info, search_params):
        metric = Metric()
        metric.set_run_id(timestamp)
        metric.env = Env(env_value)
        metric.env.OMP_NUM_THREADS = 0
        metric.hardware = Hardware(name=hostname)
        server_version = milvus_instance.get_server_version()
        server_mode = milvus_instance.get_server_mode()
        commit = milvus_instance.get_server_commit()
        metric.server = Server(version=server_version, mode=server_mode, build_commit=commit)
        metric.collection = collection_info
        metric.index = index_info
        metric.search = search_params
        return metric

    def run(self, run_type, collection):
        logger.debug(run_type)
        logger.debug(collection)
        collection_name = collection["collection_name"]
        milvus_instance = MilvusClient(collection_name=collection_name, ip=self.ip)
        self.env_value = milvus_instance.get_server_config()

        # ugly implemention
        self.env_value.pop("logs")

        if run_type == "insert_performance":
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            ni_per = collection["ni_per"]
            build_index = collection["build_index"]
            if milvus_instance.exists_collection():
                milvus_instance.delete()
                time.sleep(10)
            index_info = {}
            search_params = {}
            milvus_instance.create_collection(collection_name, dimension, index_file_size, metric_type)
            if build_index is True:
                index_type = collection["index_type"]
                index_param = collection["index_param"]
                index_info = {
                    "index_type": index_type,
                    "index_param": index_param
                }
                milvus_instance.create_index(index_type, index_param)
                logger.debug(milvus_instance.describe_index())
            res = self.do_insert(milvus_instance, collection_name, data_type, dimension, collection_size, ni_per)
            logger.info(res)
            milvus_instance.flush()
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info, search_params)
            metric.metrics = {
                "type": run_type,
                "value": {
                    "total_time": res["total_time"],
                    "qps": res["qps"],
                    "ni_time": res["ni_time"]
                } 
            }
            report(metric)
            if build_index is True:
                logger.debug("Start build index for last file")
                milvus_instance.create_index(index_type, index_param)
                logger.debug(milvus_instance.describe_index())

        if run_type == "insert_flush_performance":
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            ni_per = collection["ni_per"]
            if milvus_instance.exists_collection():
                milvus_instance.delete()
                time.sleep(10)
            index_info = {}
            search_params = {}
            milvus_instance.create_collection(collection_name, dimension, index_file_size, metric_type)
            res = self.do_insert(milvus_instance, collection_name, data_type, dimension, collection_size, ni_per)
            logger.info(res)
            logger.debug(milvus_instance.count())
            start_time = time.time()
            milvus_instance.flush()
            end_time = time.time()
            logger.debug(milvus_instance.count())
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info, search_params)
            metric.metrics = {
                "type": run_type,
                "value": {
                    "flush_time": round(end_time - start_time, 1)
                }
            }
            report(metric)

        elif run_type == "build_performance":
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            index_type = collection["index_type"]
            index_param = collection["index_param"]
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            index_info = {
                "index_type": index_type,
                "index_param": index_param
            }
            if not milvus_instance.exists_collection():
                logger.error("Table name: %s not existed" % collection_name)
                return
            search_params = {}
            start_time = time.time()
            # drop index
            logger.debug("Drop index")
            milvus_instance.drop_index()
            start_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            milvus_instance.create_index(index_type, index_param)
            logger.debug(milvus_instance.describe_index())
            logger.debug(milvus_instance.count())
            end_time = time.time()
            end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info, search_params)
            metric.metrics = {
                "type": "build_performance",
                "value": {
                    "build_time": round(end_time - start_time, 1),
                    "start_mem_usage": start_mem_usage,
                    "end_mem_usage": end_mem_usage,
                    "diff_mem": end_mem_usage - start_mem_usage
                } 
            }
            report(metric)

        elif run_type == "delete_performance":
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            ni_per = collection["ni_per"]
            search_params = {}
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            if not milvus_instance.exists_collection():
                logger.error("Table name: %s not existed" % collection_name)
                return
            length = milvus_instance.count()
            logger.info(length)
            index_info = milvus_instance.describe_index()
            logger.info(index_info)
            ids = [i for i in range(length)]
            loops = int(length / ni_per)
            milvus_instance.preload_collection()
            start_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            start_time = time.time()
            for i in range(loops):
                delete_ids = ids[i*ni_per : i*ni_per+ni_per]
                logger.debug("Delete %d - %d" % (delete_ids[0], delete_ids[-1]))
                milvus_instance.delete_vectors(delete_ids)
                milvus_instance.flush()
                logger.debug("Table row counts: %d" % milvus_instance.count())
            logger.debug("Table row counts: %d" % milvus_instance.count())
            milvus_instance.flush()
            end_time = time.time()
            end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            logger.debug("Table row counts: %d" % milvus_instance.count())
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info, search_params)
            metric.metrics = {
                "type": "delete_performance",
                "value": {
                    "delete_time": round(end_time - start_time, 1),
                    "start_mem_usage": start_mem_usage,
                    "end_mem_usage": end_mem_usage,
                    "diff_mem": end_mem_usage - start_mem_usage
                }
            }
            report(metric)

        elif run_type == "search_performance":
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            run_count = collection["run_count"]
            top_ks = collection["top_ks"]
            nqs = collection["nqs"]
            search_params = collection["search_params"]
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            # fro debugging
            # time.sleep(3600)
            if not milvus_instance.exists_collection():
                logger.error("Table name: %s not existed" % collection_name)
                return

            logger.info(milvus_instance.count())
            index_info = milvus_instance.describe_index()
            logger.info(index_info)
            milvus_instance.preload_collection()
            logger.info("Start warm up query")
            res = self.do_query(milvus_instance, collection_name, [1], [1], 2, search_param=search_params[0])
            logger.info("End warm up query")
            for search_param in search_params:
                logger.info("Search param: %s" % json.dumps(search_param))
                res = self.do_query(milvus_instance, collection_name, top_ks, nqs, run_count, search_param)
                headers = ["Nq/Top-k"]
                headers.extend([str(top_k) for top_k in top_ks])
                logger.info("Search param: %s" % json.dumps(search_param))
                utils.print_table(headers, nqs, res)
                for index_nq, nq in enumerate(nqs):
                    for index_top_k, top_k in enumerate(top_ks):
                        search_param_group = {
                            "nq": nq,
                            "topk": top_k,
                            "search_param": search_param
                        }
                        search_time = res[index_nq][index_top_k]
                        metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info, search_param_group)
                        metric.metrics = {
                            "type": "search_performance",
                            "value": {
                                "search_time": search_time
                            } 
                        }
                        report(metric)

        elif run_type == "search_ids_stability":
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            search_params = collection["search_params"]
            during_time = collection["during_time"]
            ids_length = collection["ids_length"]
            ids = collection["ids"]
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            if not milvus_instance.exists_collection():
                logger.error("Table name: %s not existed" % collection_name)
                return
            logger.info(milvus_instance.count())
            index_info = milvus_instance.describe_index()
            logger.info(index_info)
            g_top_k = int(collection["top_ks"].split("-")[1])
            l_top_k = int(collection["top_ks"].split("-")[0])
            g_id = int(ids.split("-")[1])
            l_id = int(ids.split("-")[0])
            g_id_length = int(ids_length.split("-")[1])
            l_id_length = int(ids_length.split("-")[0])

            milvus_instance.preload_collection()
            start_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            logger.debug(start_mem_usage)
            start_time = time.time()
            while time.time() < start_time + during_time * 60:
                search_param = {}
                top_k = random.randint(l_top_k, g_top_k)
                ids_num = random.randint(l_id_length, g_id_length)
                ids_param = [random.randint(l_id_length, g_id_length) for _ in range(ids_num)]
                for k, v in search_params.items():
                    search_param[k] = random.randint(int(v.split("-")[0]), int(v.split("-")[1]))
                logger.debug("Query top-k: %d, ids_num: %d, param: %s" % (top_k, ids_num, json.dumps(search_param)))
                result = milvus_instance.query_ids(top_k, ids_param, search_param=search_param)
            end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info, {})
            metric.metrics = {
                "type": "search_ids_stability",
                "value": {
                    "during_time": during_time,
                    "start_mem_usage": start_mem_usage,
                    "end_mem_usage": end_mem_usage,
                    "diff_mem": end_mem_usage - start_mem_usage
                }
            }
            report(metric)

        # for sift/deep datasets
        # TODO: enable
        elif run_type == "accuracy":
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            search_params = collection["search_params"]
            # mapping to search param list
            search_params = self.generate_combinations(search_params)

            top_ks = collection["top_ks"]
            nqs = collection["nqs"]
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            if not milvus_instance.exists_collection():
                logger.error("Table name: %s not existed" % collection_name)
                return
            logger.info(milvus_instance.count())
            index_info = milvus_instance.describe_index()
            logger.info(index_info)
            milvus_instance.preload_collection()
            true_ids_all = self.get_groundtruth_ids(collection_size)
            for search_param in search_params:
                for top_k in top_ks:
                    for nq in nqs:
                        total = 0
                        search_param_group = {
                            "nq": nq,
                            "topk": top_k,
                            "search_param": search_param
                        }
                        logger.info("Query params: %s" % json.dumps(search_param_group))
                        result_ids, result_distances = self.do_query_ids(milvus_instance, collection_name, top_k, nq, search_param=search_param)
                        acc_value = self.get_recall_value(true_ids_all[:nq, :top_k].tolist(), result_ids)
                        logger.info("Query accuracy: %s" % acc_value)
                        metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info, search_param_group)
                        metric.metrics = {
                            "type": "accuracy",
                            "value": {
                                "acc": acc_value
                            } 
                        }
                        report(metric)

        elif run_type == "ann_accuracy":
            hdf5_source_file = collection["source_file"]
            collection_name = collection["collection_name"]
            index_file_sizes = collection["index_file_sizes"]
            index_types = collection["index_types"]
            index_params = collection["index_params"]
            top_ks = collection["top_ks"]
            nqs = collection["nqs"]
            search_params = collection["search_params"]
            # mapping to search param list
            search_params = self.generate_combinations(search_params)
            # mapping to index param list
            index_params = self.generate_combinations(index_params)

            data_type, dimension, metric_type = parser.parse_ann_collection_name(collection_name)
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            dataset = utils.get_dataset(hdf5_source_file)
            if milvus_instance.exists_collection(collection_name):
                logger.info("Re-create collection: %s" % collection_name)
                milvus_instance.delete()
                time.sleep(DELETE_INTERVAL_TIME)
            true_ids = np.array(dataset["neighbors"])
            for index_file_size in index_file_sizes:
                milvus_instance.create_collection(collection_name, dimension, index_file_size, metric_type)
                logger.info(milvus_instance.describe())
                insert_vectors = self.normalize(metric_type, np.array(dataset["train"]))
                # Insert batch once
                # milvus_instance.insert(insert_vectors)
                loops = len(insert_vectors) // INSERT_INTERVAL + 1
                for i in range(loops):
                    start = i*INSERT_INTERVAL
                    end = min((i+1)*INSERT_INTERVAL, len(insert_vectors))
                    tmp_vectors = insert_vectors[start:end]
                    if start < end:
                        if not isinstance(tmp_vectors, list):
                            milvus_instance.insert(tmp_vectors.tolist(), ids=[i for i in range(start, end)])
                        else:
                            milvus_instance.insert(tmp_vectors, ids=[i for i in range(start, end)])
                milvus_instance.flush()
                logger.info("Table: %s, row count: %s" % (collection_name, milvus_instance.count()))
                if milvus_instance.count() != len(insert_vectors):
                    logger.error("Table row count is not equal to insert vectors")
                    return
                for index_type in index_types:
                    for index_param in index_params:
                        logger.debug("Building index with param: %s" % json.dumps(index_param))
                        milvus_instance.create_index(index_type, index_param=index_param)
                        logger.info(milvus_instance.describe_index())
                        logger.info("Start preload collection: %s" % collection_name)
                        milvus_instance.preload_collection()
                        index_info = {
                            "index_type": index_type,
                            "index_param": index_param
                        }
                        logger.debug(index_info)
                        for search_param in search_params:
                            for nq in nqs:
                                query_vectors = self.normalize(metric_type, np.array(dataset["test"][:nq]))
                                for top_k in top_ks:
                                    search_param_group = {
                                        "nq": len(query_vectors),
                                        "topk": top_k,
                                        "search_param": search_param 
                                    }
                                    logger.debug(search_param_group)
                                    if not isinstance(query_vectors, list):
                                        result = milvus_instance.query(query_vectors.tolist(), top_k, search_param=search_param)
                                    else:
                                        result = milvus_instance.query(query_vectors, top_k, search_param=search_param)
                                    result_ids = result.id_array
                                    acc_value = self.get_recall_value(true_ids[:nq, :top_k].tolist(), result_ids)
                                    logger.info("Query ann_accuracy: %s" % acc_value)
                                    metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info, search_param_group)
                                    metric.metrics = {
                                        "type": "ann_accuracy",
                                        "value": {
                                            "acc": acc_value
                                        } 
                                    }
                                    report(metric)

        elif run_type == "search_stability":
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            search_params = collection["search_params"]
            during_time = collection["during_time"]
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            if not milvus_instance.exists_collection():
                logger.error("Table name: %s not existed" % collection_name)
                return
            logger.info(milvus_instance.count())
            index_info = milvus_instance.describe_index()
            logger.info(index_info)
            g_top_k = int(collection["top_ks"].split("-")[1])
            g_nq = int(collection["nqs"].split("-")[1])
            l_top_k = int(collection["top_ks"].split("-")[0])
            l_nq = int(collection["nqs"].split("-")[0])
            milvus_instance.preload_collection()
            start_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            logger.debug(start_mem_usage)
            start_row_count = milvus_instance.count()
            logger.debug(milvus_instance.describe_index())
            logger.info(start_row_count)
            start_time = time.time()
            while time.time() < start_time + during_time * 60:
                search_param = {}
                top_k = random.randint(l_top_k, g_top_k)
                nq = random.randint(l_nq, g_nq)
                for k, v in search_params.items():
                    search_param[k] = random.randint(int(v.split("-")[0]), int(v.split("-")[1]))
                query_vectors = [[random.random() for _ in range(dimension)] for _ in range(nq)]
                logger.debug("Query nq: %d, top-k: %d, param: %s" % (nq, top_k, json.dumps(search_param)))
                result = milvus_instance.query(query_vectors, top_k, search_param=search_param)
            end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info, {})
            metric.metrics = {
                "type": "search_stability",
                "value": {
                    "during_time": during_time,
                    "start_mem_usage": start_mem_usage,
                    "end_mem_usage": end_mem_usage,
                    "diff_mem": end_mem_usage - start_mem_usage
                } 
            }
            report(metric)

        elif run_type == "stability":
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            search_params = collection["search_params"]
            insert_xb = collection["insert_xb"]
            insert_interval = collection["insert_interval"]
            delete_xb = collection["delete_xb"]
            during_time = collection["during_time"]
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name
            }
            if not milvus_instance.exists_collection():
                logger.error("Table name: %s not existed" % collection_name)
                return
            logger.info(milvus_instance.count())
            index_info = milvus_instance.describe_index()
            logger.info(index_info)
            g_top_k = int(collection["top_ks"].split("-")[1])
            g_nq = int(collection["nqs"].split("-")[1])
            l_top_k = int(collection["top_ks"].split("-")[0])
            l_nq = int(collection["nqs"].split("-")[0])
            milvus_instance.preload_collection()
            start_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            start_row_count = milvus_instance.count()
            logger.debug(milvus_instance.describe_index())
            logger.info(start_row_count)
            start_time = time.time()
            i = 0
            ids = []
            insert_vectors = [[random.random() for _ in range(dimension)] for _ in range(insert_xb)]
            query_vectors = [[random.random() for _ in range(dimension)] for _ in range(10000)]
            while time.time() < start_time + during_time * 60:
                i = i + 1
                for j in range(insert_interval):
                    top_k = random.randint(l_top_k, g_top_k)
                    nq = random.randint(l_nq, g_nq)
                    search_param = {}
                    for k, v in search_params.items():
                        search_param[k] = random.randint(int(v.split("-")[0]), int(v.split("-")[1]))
                    logger.debug("Query nq: %d, top-k: %d, param: %s" % (nq, top_k, json.dumps(search_param)))
                    result = milvus_instance.query(query_vectors[0:nq], top_k, search_param=search_param)
                count = milvus_instance.count()
                insert_ids = [(count+x) for x in range(len(insert_vectors))]
                ids.extend(insert_ids)
                status, res = milvus_instance.insert(insert_vectors, ids=insert_ids)
                logger.debug("%d, row_count: %d" % (i, milvus_instance.count()))
                milvus_instance.delete_vectors(ids[-delete_xb:])
                milvus_instance.flush()
                milvus_instance.compact()
            end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            end_row_count = milvus_instance.count()
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, collection_info, index_info, {})
            metric.metrics = {
                "type": "stability",
                "value": {
                    "during_time": during_time,
                    "start_mem_usage": start_mem_usage,
                    "end_mem_usage": end_mem_usage,
                    "diff_mem": end_mem_usage - start_mem_usage,
                    "row_count_increments": end_row_count - start_row_count
                } 
            }
            report(metric)

        else:
            logger.warning("Run type not defined")
            return
        logger.debug("Test finished")
