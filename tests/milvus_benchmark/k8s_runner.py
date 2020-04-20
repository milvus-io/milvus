import os
import logging
import pdb
import time
import re
import random
import traceback
from multiprocessing import Process
import numpy as np
from client import MilvusClient
import utils
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
        
    def init_env(self, server_config, args):
        self.hostname = args.hostname
        # update server_config
        helm_path = os.path.join(os.getcwd(), "../milvus-helm/milvus")
        server_config_file = helm_path+"/ci/config/sqlite/%s/server_config.yaml" % (args.image_type)
        if not os.path.exists(server_config_file):
            raise Exception("File %s not existed" % server_config_file)
        if server_config:
            logger.debug("Update server config")
            utils.update_server_config(server_config_file, server_config)
        # update log_config
        log_config_file = helm_path+"/config/log_config.conf"
        if not os.path.exists(log_config_file):
            raise Exception("File %s not existed" % log_config_file)
        src_log_config_file = helm_path+"/config/log_config.conf.src"
        if not os.path.exists(src_log_config_file):
            # copy
            os.system("cp %s %s" % (log_config_file, src_log_config_file))
        else:
            # reset
            os.system("cp %s %s" % (src_log_config_file, log_config_file))
        if "db_config.primary_path" in server_config:
            os.system("sed -i 's#%s#%s#g' %s" % (default_path, server_config["db_config.primary_path"], log_config_file))
        
        # with open(log_config_file, "r+") as fd:
        #     for line in fd.readlines():
        #         fd.write(re.sub(r'^%s' % default_path, server_config["db_config.primary_path"], line))
        # update values
        values_file_path = helm_path+"/values.yaml"
        if not os.path.exists(values_file_path):
            raise Exception("File %s not existed" % values_file_path)
        utils.update_values(values_file_path, args.hostname)
        try:
            logger.debug("Start install server")
            self.host, self.ip = utils.helm_install_server(helm_path, args.image_tag, args.image_type, self.name, namespace)
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
        utils.helm_del_server(self.name)

    def report_wrapper(self, milvus_instance, env_value, hostname, table_info, index_info, search_params):
        metric = Metric()
        metric.set_run_id(timestamp)
        metric.env = Env(env_value)
        metric.env.OMP_NUM_THREADS = 0
        metric.hardware = Hardware(name=hostname)
        server_version = milvus_instance.get_server_version()
        server_mode = milvus_instance.get_server_mode()
        commit = milvus_instance.get_server_commit()
        metric.server = Server(version=server_version, mode=server_mode, build_commit=commit)
        metric.table = table_info
        metric.index = index_info
        metric.search = search_params
        return metric

    def run(self, run_type, table):
        logger.debug(run_type)
        logger.debug(table)
        table_name = table["table_name"]
        milvus_instance = MilvusClient(table_name=table_name, ip=self.ip)
        self.env_value = milvus_instance.get_server_config()
        if run_type == "insert_performance":
            (data_type, table_size, index_file_size, dimension, metric_type) = parser.table_parser(table_name)
            ni_per = table["ni_per"]
            build_index = table["build_index"]
            if milvus_instance.exists_table():
                milvus_instance.delete()
                time.sleep(10)
            index_info = {}
            search_params = {}
            milvus_instance.create_table(table_name, dimension, index_file_size, metric_type)
            if build_index is True:
                index_type = table["index_type"]
                nlist = table["nlist"]
                index_info = {
                    "index_type": index_type,
                    "index_nlist": nlist
                }
                milvus_instance.create_index(index_type, nlist)
            res = self.do_insert(milvus_instance, table_name, data_type, dimension, table_size, ni_per)
            logger.info(res)
            table_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": table_name
            }
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, table_info, index_info, search_params)
            metric.metrics = {
                "type": "insert_performance",
                "value": {
                    "total_time": res["total_time"],
                    "qps": res["qps"],
                    "ni_time": res["ni_time"]
                } 
            }
            report(metric)
            logger.debug("Wait for file merge")
            time.sleep(120)

        elif run_type == "build_performance":
            (data_type, table_size, index_file_size, dimension, metric_type) = parser.table_parser(table_name)
            index_type = table["index_type"]
            nlist = table["nlist"]
            table_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": table_name
            }
            index_info = {
                "index_type": index_type,
                "index_nlist": nlist
            }
            if not milvus_instance.exists_table():
                logger.error("Table name: %s not existed" % table_name)
                return
            search_params = {}
            start_time = time.time()
            # drop index
            logger.debug("Drop index")
            milvus_instance.drop_index()
            start_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            milvus_instance.create_index(index_type, nlist)
            logger.debug(milvus_instance.describe_index())
            end_time = time.time()
            end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, table_info, index_info, search_params)
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

        elif run_type == "search_performance":
            (data_type, table_size, index_file_size, dimension, metric_type) = parser.table_parser(table_name)
            run_count = table["run_count"]
            search_params = table["search_params"]
            table_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": table_name
            }
            if not milvus_instance.exists_table():
                logger.error("Table name: %s not existed" % table_name)
                return
            logger.info(milvus_instance.count())
            result = milvus_instance.describe_index()
            index_info = {
                "index_type": result["index_type"],
                "index_nlist": result["nlist"]
            }
            logger.info(index_info)
            nprobes = search_params["nprobes"]
            top_ks = search_params["top_ks"]
            nqs = search_params["nqs"]
            milvus_instance.preload_table()
            logger.info("Start warm up query")
            res = self.do_query(milvus_instance, table_name, [1], [1], 1, 2)
            logger.info("End warm up query")
            for nprobe in nprobes:
                logger.info("Search nprobe: %s" % nprobe)
                res = self.do_query(milvus_instance, table_name, top_ks, nqs, nprobe, run_count)
                headers = ["Nq/Top-k"]
                headers.extend([str(top_k) for top_k in top_ks])
                utils.print_table(headers, nqs, res)
                for index_nq, nq in enumerate(nqs):
                    for index_top_k, top_k in enumerate(top_ks):
                        search_param = {
                            "nprobe": nprobe,
                            "nq": nq,
                            "topk": top_k
                        }
                        search_time = res[index_nq][index_top_k]
                        metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, table_info, index_info, search_param)
                        metric.metrics = {
                            "type": "search_performance",
                            "value": {
                                "search_time": search_time
                            } 
                        }
                        report(metric)

        # for sift/deep datasets
        elif run_type == "accuracy":
            (data_type, table_size, index_file_size, dimension, metric_type) = parser.table_parser(table_name)
            search_params = table["search_params"]
            table_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": table_name
            }
            if not milvus_instance.exists_table():
                logger.error("Table name: %s not existed" % table_name)
                return
            logger.info(milvus_instance.count())
            result = milvus_instance.describe_index()
            index_info = {
                "index_type": result["index_type"],
                "index_nlist": result["nlist"]
            }
            logger.info(index_info)
            nprobes = search_params["nprobes"]
            top_ks = search_params["top_ks"]
            nqs = search_params["nqs"]
            milvus_instance.preload_table()
            true_ids_all = self.get_groundtruth_ids(table_size)
            for nprobe in nprobes:
                logger.info("Search nprobe: %s" % nprobe)
                for top_k in top_ks:
                    for nq in nqs:
                        total = 0
                        search_param = {
                            "nprobe": nprobe,
                            "nq": nq,
                            "topk": top_k
                        }
                        result_ids, result_distances = self.do_query_ids(milvus_instance, table_name, top_k, nq, nprobe)
                        acc_value = self.get_recall_value(true_ids_all[:nq, :top_k].tolist(), result_ids)
                        logger.info("Query accuracy: %s" % acc_value)
                        metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, table_info, index_info, search_param)
                        metric.metrics = {
                            "type": "accuracy",
                            "value": {
                                "acc": acc_value
                            } 
                        }
                        report(metric)

        elif run_type == "ann_accuracy":
            hdf5_source_file = table["source_file"]
            table_name = table["table_name"]
            index_file_sizes = table["index_file_sizes"]
            index_types = table["index_types"]
            nlists = table["nlists"]
            search_params = table["search_params"]
            nprobes = search_params["nprobes"]
            top_ks = search_params["top_ks"]
            nqs = search_params["nqs"]
            data_type, dimension, metric_type = parser.parse_ann_table_name(table_name)
            table_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": table_name
            }
            dataset = utils.get_dataset(hdf5_source_file)
            if milvus_instance.exists_table(table_name):
                logger.info("Re-create table: %s" % table_name)
                milvus_instance.delete(table_name)
                time.sleep(DELETE_INTERVAL_TIME)
            true_ids = np.array(dataset["neighbors"])
            for index_file_size in index_file_sizes:
                milvus_instance.create_table(table_name, dimension, index_file_size, metric_type)
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
                time.sleep(20)
                logger.info("Table: %s, row count: %s" % (table_name, milvus_instance.count()))
                if milvus_instance.count() != len(insert_vectors):
                    logger.error("Table row count is not equal to insert vectors")
                    return
                for index_type in index_types:
                    for nlist in nlists:
                        milvus_instance.create_index(index_type, nlist)
                        # logger.info(milvus_instance.describe_index())
                        logger.info("Start preload table: %s, index_type: %s, nlist: %s" % (table_name, index_type, nlist))
                        milvus_instance.preload_table()
                        index_info = {
                            "index_type": index_type,
                            "index_nlist": nlist
                        }
                        for nprobe in nprobes:
                            for nq in nqs:
                                query_vectors = self.normalize(metric_type, np.array(dataset["test"][:nq]))
                                for top_k in top_ks:
                                    search_params = {
                                        "nq": len(query_vectors),
                                        "nprobe": nprobe,
                                        "topk": top_k
                                    }
                                    if not isinstance(query_vectors, list):
                                        result = milvus_instance.query(query_vectors.tolist(), top_k, nprobe)
                                    else:
                                        result = milvus_instance.query(query_vectors, top_k, nprobe)
                                    result_ids = result.id_array
                                    acc_value = self.get_recall_value(true_ids[:nq, :top_k].tolist(), result_ids)
                                    logger.info("Query ann_accuracy: %s" % acc_value)
                                    metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, table_info, index_info, search_params)
                                    metric.metrics = {
                                        "type": "ann_accuracy",
                                        "value": {
                                            "acc": acc_value
                                        } 
                                    }
                                    report(metric)
                milvus_instance.delete()

        elif run_type == "search_stability":
            (data_type, table_size, index_file_size, dimension, metric_type) = parser.table_parser(table_name)
            search_params = table["search_params"]
            during_time = table["during_time"]
            table_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": table_name
            }
            if not milvus_instance.exists_table():
                logger.error("Table name: %s not existed" % table_name)
                return
            logger.info(milvus_instance.count())
            result = milvus_instance.describe_index()
            index_info = {
                "index_type": result["index_type"],
                "index_nlist": result["nlist"]
            }
            search_param = {}
            logger.info(index_info)
            g_nprobe = int(search_params["nprobes"].split("-")[1])
            g_top_k = int(search_params["top_ks"].split("-")[1])
            g_nq = int(search_params["nqs"].split("-")[1])
            l_nprobe = int(search_params["nprobes"].split("-")[0])
            l_top_k = int(search_params["top_ks"].split("-")[0])
            l_nq = int(search_params["nqs"].split("-")[0])
            milvus_instance.preload_table()
            start_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            logger.debug(start_mem_usage)
            logger.info("Start warm up query")
            res = self.do_query(milvus_instance, table_name, [1], [1], 1, 2)
            logger.info("End warm up query")
            start_time = time.time()
            while time.time() < start_time + during_time * 60:
                top_k = random.randint(l_top_k, g_top_k)
                nq = random.randint(l_nq, g_nq)
                nprobe = random.randint(l_nprobe, g_nprobe)
                query_vectors = [[random.random() for _ in range(dimension)] for _ in range(nq)]
                logger.debug("Query nprobe:%d, nq:%d, top-k:%d" % (nprobe, nq, top_k))
                result = milvus_instance.query(query_vectors, top_k, nprobe)
            end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, table_info, index_info, search_param)
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
            (data_type, table_size, index_file_size, dimension, metric_type) = parser.table_parser(table_name)
            search_params = table["search_params"]
            insert_xb = table["insert_xb"]
            insert_interval = table["insert_interval"]
            during_time = table["during_time"]
            table_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": table_name
            }
            if not milvus_instance.exists_table():
                logger.error("Table name: %s not existed" % table_name)
                return
            logger.info(milvus_instance.count())
            result = milvus_instance.describe_index()
            index_info = {
                "index_type": result["index_type"],
                "index_nlist": result["nlist"]
            }
            search_param = {}
            logger.info(index_info)
            g_nprobe = int(search_params["nprobes"].split("-")[1])
            g_top_k = int(search_params["top_ks"].split("-")[1])
            g_nq = int(search_params["nqs"].split("-")[1])
            l_nprobe = int(search_params["nprobes"].split("-")[0])
            l_top_k = int(search_params["top_ks"].split("-")[0])
            l_nq = int(search_params["nqs"].split("-")[0])
            milvus_instance.preload_table()
            logger.info("Start warm up query")
            res = self.do_query(milvus_instance, table_name, [1], [1], 1, 2)
            logger.info("End warm up query")
            start_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            start_row_count = milvus_instance.count()
            start_time = time.time()
            i = 0
            while time.time() < start_time + during_time * 60:
                i = i + 1
                for j in range(insert_interval):
                    top_k = random.randint(l_top_k, g_top_k)
                    nq = random.randint(l_nq, g_nq)
                    nprobe = random.randint(l_nprobe, g_nprobe)
                    query_vectors = [[random.random() for _ in range(dimension)] for _ in range(nq)]
                    logger.debug("Query nprobe:%d, nq:%d, top-k:%d" % (nprobe, nq, top_k))
                    result = milvus_instance.query(query_vectors, top_k, nprobe)
                insert_vectors = [[random.random() for _ in range(dimension)] for _ in range(insert_xb)]
                status, res = milvus_instance.insert(insert_vectors, ids=[x for x in range(len(insert_vectors))])
                logger.debug("%d, row_count: %d" % (i, milvus_instance.count()))
            end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            end_row_count = milvus_instance.count()
            metric = self.report_wrapper(milvus_instance, self.env_value, self.hostname, table_info, index_info, search_param)
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