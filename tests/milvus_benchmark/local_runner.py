import os
import logging
import pdb
import time
import random
import string
import json
import csv
from multiprocessing import Process
import numpy as np
import concurrent.futures
from client import MilvusClient
import utils
import parser
from runner import Runner

DELETE_INTERVAL_TIME = 5
INSERT_INTERVAL = 50000
logger = logging.getLogger("milvus_benchmark.local_runner")


class LocalRunner(Runner):
    """run local mode"""
    def __init__(self, host, port):
        super(LocalRunner, self).__init__()
        self.host = host
        self.port = port

    def run(self, run_type, collection):
        logger.debug(run_type)
        logger.debug(collection)
        collection_name = collection["collection_name"] if "collection_name" in collection else None
        milvus_instance = MilvusClient(collection_name=collection_name, host=self.host, port=self.port)
        logger.info(milvus_instance.show_collections())
        env_value = milvus_instance.get_server_config()
        logger.debug(env_value)

        if run_type in ["insert_performance", "insert_flush_performance"]:
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            ni_per = collection["ni_per"]
            build_index = collection["build_index"]
            if milvus_instance.exists_collection():
                milvus_instance.drop()
                time.sleep(10)
            milvus_instance.create_collection(collection_name, dimension, index_file_size, metric_type)
            if build_index is True:
                index_type = collection["index_type"]
                index_param = collection["index_param"]
                milvus_instance.create_index(index_type, index_param)
                logger.debug(milvus_instance.describe_index())
            res = self.do_insert(milvus_instance, collection_name, data_type, dimension, collection_size, ni_per)
            milvus_instance.flush()
            logger.debug("Table row counts: %d" % milvus_instance.count())
            if build_index is True:
                logger.debug("Start build index for last file")
                milvus_instance.create_index(index_type, index_param)
                logger.debug(milvus_instance.describe_index())

        elif run_type == "delete_performance":
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            ni_per = collection["ni_per"]
            if not milvus_instance.exists_collection():
                logger.error(milvus_instance.show_collections())
                logger.warning("Table: %s not found" % collection_name)
                return
            length = milvus_instance.count() 
            ids = [i for i in range(length)] 
            loops = int(length / ni_per)
            for i in range(loops):
                delete_ids = ids[i*ni_per : i*ni_per+ni_per]
                logger.debug("Delete %d - %d" % (delete_ids[0], delete_ids[-1]))
                milvus_instance.delete(delete_ids)
                milvus_instance.flush() 
                logger.debug("Table row counts: %d" % milvus_instance.count())
            logger.debug("Table row counts: %d" % milvus_instance.count())
            milvus_instance.flush() 
            logger.debug("Table row counts: %d" % milvus_instance.count())

        elif run_type == "build_performance":
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            index_type = collection["index_type"]
            index_param = collection["index_param"]
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
            logger.debug("Table row counts: %d" % milvus_instance.count())
            end_time = time.time()
            end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            logger.debug("Diff memory: %s, current memory usage: %s, build time: %s" % ((end_mem_usage - start_mem_usage), end_mem_usage, round(end_time - start_time, 1)))

        elif run_type == "search_performance":
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            run_count = collection["run_count"]
            top_ks = collection["top_ks"]
            nqs = collection["nqs"]
            search_params = collection["search_params"]
            # for debugging
            # time.sleep(3600)
            if not milvus_instance.exists_collection():
                logger.error("Table name: %s not existed" % collection_name)
                return
            logger.info(milvus_instance.count())
            result = milvus_instance.describe_index()
            logger.info(result)
            milvus_instance.preload_collection()
            mem_usage = milvus_instance.get_mem_info()["memory_used"]
            logger.info(mem_usage)
            for search_param in search_params:
                logger.info("Search param: %s" % json.dumps(search_param))
                res = self.do_query(milvus_instance, collection_name, top_ks, nqs, run_count, search_param)
                headers = ["Nq/Top-k"]
                headers.extend([str(top_k) for top_k in top_ks])
                logger.info("Search param: %s" % json.dumps(search_param))
                utils.print_table(headers, nqs, res)
                mem_usage = milvus_instance.get_mem_info()["memory_used"]
                logger.info(mem_usage)

        elif run_type == "locust_search_performance":
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            ### spawn locust requests
            collection_num = collection["collection_num"]
            task = collection["task"]
            #. generate task code
            task_file = utils.get_unique_name()
            task_file_script = task_file+'.py'
            task_file_csv = task_file+'_stats.csv'
            task_type = task["type"]
            connection_type = "single"
            connection_num = task["connection_num"]
            if connection_num > 1:
                connection_type = "multi"
            clients_num = task["clients_num"]
            hatch_rate = task["hatch_rate"]
            during_time = task["during_time"]
            def_name = task_type
            task_params = task["params"]
            collection_names = []
            for i in range(collection_num):
                suffix = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(5))
                collection_names.append(collection_name + "_" + suffix)
            # collection_names = ['sift_1m_1024_128_l2_Kg6co', 'sift_1m_1024_128_l2_egkBK', 'sift_1m_1024_128_l2_D0wtE',
            #                     'sift_1m_1024_128_l2_9naps', 'sift_1m_1024_128_l2_iJ0jj', 'sift_1m_1024_128_l2_nqUTm',
            #                     'sift_1m_1024_128_l2_GIF0D', 'sift_1m_1024_128_l2_EL2qk', 'sift_1m_1024_128_l2_qLRnC',
            #                     'sift_1m_1024_128_l2_8Ditg']
            # #####
            ni_per = collection["ni_per"]
            build_index = collection["build_index"]
            TODO: debug
            for c_name in collection_names:
                milvus_instance = MilvusClient(collection_name=c_name, host=self.host, port=self.port)
                if milvus_instance.exists_collection(collection_name=c_name):
                    milvus_instance.drop(name=c_name)
                    time.sleep(10)
                milvus_instance.create_collection(c_name, dimension, index_file_size, metric_type)
                if build_index is True:
                    index_type = collection["index_type"]
                    index_param = collection["index_param"]
                    milvus_instance.create_index(index_type, index_param)
                    logger.debug(milvus_instance.describe_index())
                res = self.do_insert(milvus_instance, c_name, data_type, dimension, collection_size, ni_per)
                milvus_instance.flush()
                logger.debug("Table row counts: %d" % milvus_instance.count(name=c_name))
                if build_index is True:
                    logger.debug("Start build index for last file")
                    milvus_instance.create_index(index_type, index_param)
                    logger.debug(milvus_instance.describe_index())
            code_str = """
import random
import string
from locust import User, task, between
from locust_task import MilvusTask
from client import MilvusClient

host = '%s'
port = %s
dim = %s
connection_type = '%s'
collection_names = %s
m = MilvusClient(host=host, port=port)


def get_collection_name():
    return random.choice(collection_names)
    
    
def get_client(collection_name):
    if connection_type == 'single':
        return MilvusTask(m=m)
    elif connection_type == 'multi':
        return MilvusTask(connection_type='multi', host=host, port=port, collection_name=collection_name)
        

class QueryTask(User):
    wait_time = between(0.001, 0.002)
        
    @task()
    def %s(self):
        top_k = %s
        X = [[random.random() for i in range(dim)] for i in range(%s)]
        search_param = %s
        collection_name = get_collection_name()
        print(collection_name)
        client = get_client(collection_name)
        client.query(X, top_k, search_param, collection_name=collection_name)
            """ % (self.host, self.port, dimension, connection_type, collection_names, def_name, task_params["top_k"], task_params["nq"], task_params["search_param"])
            with open(task_file_script, 'w+') as fd:
                fd.write(code_str)
            locust_cmd = "locust -f %s --headless --csv=%s -u %d -r %d -t %s" % (
                    task_file_script,
                    task_file,
                    clients_num,
                    hatch_rate,
                    during_time)
            logger.info(locust_cmd)
            try:
                res = os.system(locust_cmd)
            except Exception as e:
                logger.error(str(e))
                return
            
            #. retrieve and collect test statistics
            metric = None
            with open(task_file_csv, newline='') as fd:
                dr = csv.DictReader(fd)
                for row in dr:
                    if row["Name"] != "Aggregated":
                        continue
                    metric = row
            logger.info(metric)
            # clean up temp files
                    
        elif run_type == "search_ids_stability":
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            search_params = collection["search_params"]
            during_time = collection["during_time"]
            ids_length = collection["ids_length"]
            ids = collection["ids"]
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
                l_ids = random.randint(l_id, g_id-ids_num)
                # ids_param = [random.randint(l_id_length, g_id_length) for _ in range(ids_num)]
                ids_param = [id for id in range(l_ids, l_ids+ids_num)]
                for k, v in search_params.items():
                    search_param[k] = random.randint(int(v.split("-")[0]), int(v.split("-")[1]))
                logger.debug("Query top-k: %d, ids_num: %d, param: %s" % (top_k, ids_num, json.dumps(search_param)))
                result = milvus_instance.query_ids(top_k, ids_param, search_param=search_param)
            end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            metrics = {
                "during_time": during_time,
                "start_mem_usage": start_mem_usage,
                "end_mem_usage": end_mem_usage,
                "diff_mem": end_mem_usage - start_mem_usage,
            }
            logger.info(metrics)

        elif run_type == "search_performance_concurrents":
            data_type, dimension, metric_type = parser.parse_ann_collection_name(collection_name)
            hdf5_source_file = collection["source_file"]
            use_single_connection = collection["use_single_connection"]
            concurrents = collection["concurrents"]
            top_ks = collection["top_ks"]
            nqs = collection["nqs"]
            search_params = self.generate_combinations(collection["search_params"])
            if not milvus_instance.exists_collection():
                logger.error("Table name: %s not existed" % collection_name)
                return
            logger.info(milvus_instance.count())
            result = milvus_instance.describe_index()
            logger.info(result)
            milvus_instance.preload_collection()
            dataset = utils.get_dataset(hdf5_source_file)
            for concurrent_num in concurrents:
                top_k = top_ks[0] 
                for nq in nqs:
                    mem_usage = milvus_instance.get_mem_info()["memory_used"]
                    logger.info(mem_usage)
                    query_vectors = self.normalize(metric_type, np.array(dataset["test"][:nq])) 
                    logger.debug(search_params)
                    for search_param in search_params:
                        logger.info("Search param: %s" % json.dumps(search_param))
                        total_time = 0.0
                        if use_single_connection is True:
                            connections = [MilvusClient(collection_name=collection_name, host=self.host, port=self.port)]
                            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_num) as executor:
                                future_results = {executor.submit(
                                    self.do_query_qps, connections[0], query_vectors, top_k, search_param=search_param) : index for index in range(concurrent_num)}
                        else:
                            connections = [MilvusClient(collection_name=collection_name, host=self.hos, port=self.port) for i in range(concurrent_num)]
                            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_num) as executor:
                                future_results = {executor.submit(
                                    self.do_query_qps, connections[index], query_vectors, top_k, search_param=search_param) : index for index in range(concurrent_num)}
                        for future in concurrent.futures.as_completed(future_results):
                            total_time = total_time + future.result()
                        qps_value = total_time / concurrent_num 
                        logger.debug("QPS value: %f, total_time: %f, request_nums: %f" % (qps_value, total_time, concurrent_num))
                    mem_usage = milvus_instance.get_mem_info()["memory_used"]
                    logger.info(mem_usage)

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
            dataset = utils.get_dataset(hdf5_source_file)
            if milvus_instance.exists_collection(collection_name):
                logger.info("Re-create collection: %s" % collection_name)
                milvus_instance.drop()
                time.sleep(DELETE_INTERVAL_TIME)
            true_ids = np.array(dataset["neighbors"])
            for index_file_size in index_file_sizes:
                milvus_instance.create_collection(collection_name, dimension, index_file_size, metric_type)
                logger.info(milvus_instance.describe())
                insert_vectors = self.normalize(metric_type, np.array(dataset["train"]))
                logger.debug(len(insert_vectors))
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
                        for search_param in search_params:
                            for nq in nqs:
                                query_vectors = self.normalize(metric_type, np.array(dataset["test"][:nq]))
                                for top_k in top_ks:
                                    logger.debug("Search nq: %d, top-k: %d, search_param: %s" % (nq, top_k, json.dumps(search_param)))
                                    if not isinstance(query_vectors, list):
                                        result = milvus_instance.query(query_vectors.tolist(), top_k, search_param=search_param)
                                    else:
                                        result = milvus_instance.query(query_vectors, top_k, search_param=search_param)
                                    result_ids = result.id_array
                                    acc_value = self.get_recall_value(true_ids[:nq, :top_k].tolist(), result_ids)
                                    logger.info("Query ann_accuracy: %s" % acc_value)


        elif run_type == "stability":
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            search_params = collection["search_params"]
            insert_xb = collection["insert_xb"]
            insert_interval = collection["insert_interval"]
            delete_xb = collection["delete_xb"]
            # flush_interval = collection["flush_interval"]
            # compact_interval = collection["compact_interval"]
            during_time = collection["during_time"]
            if not milvus_instance.exists_collection():
                logger.error(milvus_instance.show_collections())
                logger.error("Table name: %s not existed" % collection_name)
                return
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
                milvus_instance.delete(ids[-delete_xb:])
                milvus_instance.flush()
                milvus_instance.compact()
            end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            end_row_count = milvus_instance.count()
            metrics = {
                "during_time": during_time,
                "start_mem_usage": start_mem_usage,
                "end_mem_usage": end_mem_usage,
                "diff_mem": end_mem_usage - start_mem_usage,
                "row_count_increments": end_row_count - start_row_count
            }
            logger.info(metrics)

        elif run_type == "loop_stability":
            # init data
            milvus_instance.clean_db()
            pull_interval = collection["pull_interval"]
            collection_num = collection["collection_num"]
            dimension = collection["dimension"] if "dimension" in collection else 128
            insert_xb = collection["insert_xb"] if "insert_xb" in collection else 100000
            index_types = collection["index_types"] if "index_types" in collection else ['ivf_sq8']
            index_param = {"nlist": 2048}
            collection_names = []
            milvus_instances_map = {}
            insert_vectors = [[random.random() for _ in range(dimension)] for _ in range(insert_xb)]
            for i in range(collection_num):
                name = utils.get_unique_name(prefix="collection_")
                collection_names.append(name)
                metric_type = random.choice(["l2", "ip"])
                index_file_size = random.randint(10, 20)
                milvus_instance.create_collection(name, dimension, index_file_size, metric_type)
                milvus_instance = MilvusClient(collection_name=name, host=self.host)
                index_type = random.choice(index_types)
                milvus_instance.create_index(index_type, index_param=index_param)
                logger.info(milvus_instance.describe_index())
                insert_vectors = utils.normalize(metric_type, insert_vectors)
                milvus_instance.insert(insert_vectors)
                milvus_instance.flush()
                milvus_instances_map.update({name: milvus_instance})
                logger.info(milvus_instance.describe_index())
                logger.info(milvus_instance.describe())

            tasks = ["insert_rand", "delete_rand", "query_rand", "flush"]
            i = 1
            while True:
                logger.info("Loop time: %d" % i)
                start_time = time.time()
                while time.time() - start_time < pull_interval_seconds:
                    # choose collection
                    tmp_collection_name = random.choice(collection_names)
                    # choose task from task
                    task_name = random.choice(tasks)
                    logger.info(tmp_collection_name)
                    logger.info(task_name)
                    # execute task
                    task_run = getattr(milvus_instances_map[tmp_collection_name], task_name)
                    task_run()
                # new connection
                for name in collection_names:
                    milvus_instance = MilvusClient(collection_name=name, host=self.host)
                    milvus_instances_map.update({name: milvus_instance})
                i = i + 1
        elif run_type == "locust_mix_performance":
            (data_type, collection_size, index_file_size, dimension, metric_type) = parser.collection_parser(collection_name)
            # ni_per = collection["ni_per"]
            # build_index = collection["build_index"]
            # # TODO: debug
            # if milvus_instance.exists_collection():
            #     milvus_instance.drop()
            #     time.sleep(10)
            # milvus_instance.create_collection(collection_name, dimension, index_file_size, metric_type)
            # if build_index is True:
            #     index_type = collection["index_type"]
            #     index_param = collection["index_param"]
            #     milvus_instance.create_index(index_type, index_param)
            #     logger.debug(milvus_instance.describe_index())
            # res = self.do_insert(milvus_instance, collection_name, data_type, dimension, collection_size, ni_per)
            # milvus_instance.flush()
            # logger.debug("Table row counts: %d" % milvus_instance.count())
            # if build_index is True:
            #     logger.debug("Start build index for last file")
            #     milvus_instance.create_index(index_type, index_param)
            #     logger.debug(milvus_instance.describe_index())
            task = collection["tasks"]
            task_file = utils.get_unique_name()
            task_file_script = task_file + '.py'
            task_file_csv = task_file + '_stats.csv'
            task_types = task["types"]
            connection_type = "single"
            connection_num = task["connection_num"]
            if connection_num > 1:
                connection_type = "multi"
            clients_num = task["clients_num"]
            hatch_rate = task["hatch_rate"]
            during_time = task["during_time"]
            def_strs = ""
            for task_type in task_types:
                type = task_type["type"]
                weight = task_type["weight"]
                if type == "flush":
                    def_str = """
    @task(%d)
    def flush(self):
        client = get_client(collection_name)
        client.flush(collection_name=collection_name)
                    """ % weight
                if type == "compact":
                    def_str = """
    @task(%d)
    def compact(self):
        client = get_client(collection_name)
        client.compact(collection_name)
                    """ % weight
                if type == "query":
                    def_str = """
    @task(%d)
    def query(self):
        client = get_client(collection_name)
        params = %s
        X = [[random.random() for i in range(dim)] for i in range(params["nq"])]
        client.query(X, params["top_k"], params["search_param"], collection_name=collection_name)
                    """ % (weight, task_type["params"])
                if type == "insert":
                    def_str = """
    @task(%d)
    def insert(self):
        client = get_client(collection_name)
        params = %s
        ids = [random.randint(10, 1000000) for i in range(params["nb"])]
        X = [[random.random() for i in range(dim)] for i in range(params["nb"])]
        client.insert(X,ids=ids, collection_name=collection_name)
                """ % (weight, task_type["params"])
                if type == "delete":
                    def_str = """
    @task(%d)
    def delete(self):
        client = get_client(collection_name)
        ids = [random.randint(1, 1000000) for i in range(1)]
        client.delete(ids, collection_name)
                    """ % weight
                def_strs += def_str
                print(def_strs)
                code_str = """
import random
import json
from locust import User, task, between
from locust_task import MilvusTask
from client import MilvusClient

host = '%s'
port = %s
collection_name = '%s'
dim = %s
connection_type = '%s'
m = MilvusClient(host=host, port=port)


def get_client(collection_name):
    if connection_type == 'single':
        return MilvusTask(m=m)
    elif connection_type == 'multi':
        return MilvusTask(connection_type='multi', host=host, port=port, collection_name=collection_name)
        
        
class MixTask(User):
    wait_time = between(0.001, 0.002)
    %s
    """ % (self.host, self.port, collection_name, dimension, connection_type, def_strs)
            with open(task_file_script, "w+") as fd:
                fd.write(code_str)
            locust_cmd = "locust -f %s --headless --csv=%s -u %d -r %d -t %s" % (
                task_file_script,
                task_file,
                clients_num,
                hatch_rate,
                during_time)
            logger.info(locust_cmd)
            try:
                res = os.system(locust_cmd)
            except Exception as e:
                logger.error(str(e))
                return

            # . retrieve and collect test statistics
            metric = None
            with open(task_file_csv, newline='') as fd:
                dr = csv.DictReader(fd)
                for row in dr:
                    if row["Name"] != "Aggregated":
                        continue
                    metric = row
            logger.info(metric)

        else:
            logger.warning("Run type not defined")
            return
        logger.debug("Test finished")
