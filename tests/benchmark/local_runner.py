import os
import logging
import pdb
import string
import time
import random
import json
import csv
from multiprocessing import Process
import numpy as np
import concurrent.futures
from queue import Queue

import locust_user
from milvus import DataType
from client import MilvusClient
from runner import Runner
import utils
import parser


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
        # TODO:
        # self.env_value = milvus_instance.get_server_config()
        # ugly implemention
        # self.env_value = utils.convert_nested(self.env_value)
        # self.env_value.pop("logs")
        # self.env_value.pop("network")
        # logger.info(self.env_value)

        if run_type in ["insert_performance", "insert_flush_performance"]:
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
            ni_per = collection["ni_per"]
            build_index = collection["build_index"]
            if milvus_instance.exists_collection():
                milvus_instance.drop()
                time.sleep(10)
            vector_type = self.get_vector_type(data_type)
            other_fields = collection["other_fields"] if "other_fields" in collection else None
            milvus_instance.create_collection(dimension, data_type=vector_type, other_fields=other_fields)
            if build_index is True:
                index_type = collection["index_type"]
                index_param = collection["index_param"]
                index_field_name = utils.get_default_field_name(vector_type)
                milvus_instance.create_index(index_field_name, index_type, metric_type, index_param=index_param)
            res = self.do_insert(milvus_instance, collection_name, data_type, dimension, collection_size, ni_per)
            milvus_instance.flush()
            logger.debug("Table row counts: %d" % milvus_instance.count())
            if build_index is True:
                logger.debug("Start build index for last file")
                milvus_instance.create_index(index_field_name, index_type, metric_type, index_param=index_param)

        elif run_type == "delete_performance":
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
            ni_per = collection["ni_per"]
            auto_flush = collection["auto_flush"] if "auto_flush" in collection else True
            if not milvus_instance.exists_collection():
                logger.error(milvus_instance.show_collections())
                logger.error("Table: %s not found" % collection_name)
                return
            length = milvus_instance.count() 
            ids = [i for i in range(length)] 
            loops = int(length / ni_per)
            if auto_flush is False:
                milvus_instance.set_config("storage", "auto_flush_interval", BIG_FLUSH_INTERVAL)
            for i in range(loops):
                delete_ids = ids[i*ni_per: i*ni_per+ni_per]
                logger.debug("Delete %d - %d" % (delete_ids[0], delete_ids[-1]))
                milvus_instance.delete(delete_ids)
                logger.debug("Table row counts: %d" % milvus_instance.count())
            logger.debug("Table row counts: %d" % milvus_instance.count())
            milvus_instance.flush()
            logger.debug("Table row counts: %d" % milvus_instance.count())

        elif run_type == "build_performance":
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
            index_type = collection["index_type"]
            index_param = collection["index_param"]
            if not milvus_instance.exists_collection():
                logger.error("Table name: %s not existed" % collection_name)
                return
            vector_type = self.get_vector_type(data_type)
            index_field_name = utils.get_default_field_name(vector_type)
            # drop index
            logger.debug("Drop index")
            milvus_instance.drop_index(index_field_name)
            start_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            start_time = time.time()
            milvus_instance.create_index(index_field_name, index_type, metric_type, index_param=index_param)
            end_time = time.time()
            logger.debug("Table row counts: %d" % milvus_instance.count())
            end_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            logger.debug("Diff memory: %s, current memory usage: %s, build time: %s" % ((end_mem_usage - start_mem_usage), end_mem_usage, round(end_time - start_time, 1)))

        elif run_type == "search_performance":
            (data_type, collection_size,  dimension, metric_type) = parser.collection_parser(collection_name)
            run_count = collection["run_count"]
            top_ks = collection["top_ks"]
            nqs = collection["nqs"]
            search_params = collection["search_params"]
            filter_query = []
            filters = collection["filters"] if "filters" in collection else []
            # pdb.set_trace()
            # ranges = collection["range"] if "range" in collection else None
            # terms = collection["term"] if "term" in collection else None
            # if ranges:
            #     filter_query.append(eval(ranges))
            # if terms:
            #     filter_query.append(eval(terms))
            vector_type = self.get_vector_type(data_type)
            vec_field_name = utils.get_default_field_name(vector_type)
            # for debugging
            # time.sleep(3600)
            if not milvus_instance.exists_collection():
                logger.error("Table name: %s not existed" % collection_name)
                return
            vector_type = self.get_vector_type(data_type)
            vec_field_name = utils.get_default_field_name(vector_type)
            logger.info(milvus_instance.count())
            result = milvus_instance.describe_index()
            logger.info(result)
            milvus_instance.preload_collection()
            mem_usage = milvus_instance.get_mem_info()["memory_used"]
            logger.info(mem_usage)
            for search_param in search_params:
                logger.info("Search param: %s" % json.dumps(search_param))
                filter_param = []
                if not filters:
                    filters.append(None)
                for filter in filters:
                    if isinstance(filter, dict) and "range" in filter:
                        filter_query.append(eval(filter["range"]))
                        filter_param.append(filter["range"])
                    if isinstance(filter, dict) and "term" in filter:
                        filter_query.append(eval(filter["term"]))
                        filter_param.append(filter["term"])
                    logger.info("filter param: %s" % json.dumps(filter_param))
                    res = self.do_query(milvus_instance, collection_name, vec_field_name, top_ks, nqs, run_count, search_param, filter_query)
                    headers = ["Nq/Top-k"]
                    headers.extend([str(top_k) for top_k in top_ks])
                    logger.info("Search param: %s" % json.dumps(search_param))
                    utils.print_table(headers, nqs, res)
                    mem_usage = milvus_instance.get_mem_info()["memory_used"]
                    logger.info(mem_usage)

        elif run_type == "locust_search_performance":
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
            ni_per = collection["ni_per"]
            build_index = collection["build_index"]
            vector_type = self.get_vector_type(data_type)
            index_field_name = utils.get_default_field_name(vector_type)
            # if build_index is True:
            #     index_type = collection["index_type"]
            #     index_param = collection["index_param"]
            # # TODO: debug
            # if milvus_instance.exists_collection():
            #     milvus_instance.drop()
            #     time.sleep(10)
            # other_fields = collection["other_fields"] if "other_fields" in collection else None
            # milvus_instance.create_collection(dimension, data_type=vector_type, other_fields=other_fields)
            # milvus_instance.create_index(index_field_name, index_type, metric_type, index_param=index_param)
            # res = self.do_insert(milvus_instance, collection_name, data_type, dimension, collection_size, ni_per)
            # milvus_instance.flush()
            # logger.debug("Table row counts: %d" % milvus_instance.count())
            # if build_index is True:
            #     logger.debug("Start build index for last file")
            #     milvus_instance.create_index(index_field_name, index_type, metric_type, index_param=index_param)
            real_metric_type = utils.metric_type_trans(metric_type)
            ### spawn locust requests
            task = collection["task"]
            connection_type = "single"
            connection_num = task["connection_num"]
            if connection_num > 1:
                connection_type = "multi"
            clients_num = task["clients_num"]
            hatch_rate = task["hatch_rate"]
            during_time = utils.timestr_to_int(task["during_time"])
            task_types = task["types"]
            # """
            # task: 
            #     connection_num: 1
            #     clients_num: 100
            #     hatch_rate: 2
            #     during_time: 5m
            #     types:
            #     -
            #         type: query
            #         weight: 1
            #         params:
            #         top_k: 10
            #         nq: 1
            #         # filters:
            #         #   -
            #         #     range:
            #         #       int64:
            #         #         LT: 0
            #         #         GT: 1000000
            #         search_param:
            #             nprobe: 16
            # """
            run_params = {"tasks": {}, "clients_num": clients_num, "spawn_rate": hatch_rate, "during_time": during_time}
            for task_type in task_types:
                run_params["tasks"].update({task_type["type"]: task_type["weight"] if "weight" in task_type else 1})

            #. collect stats
            locust_stats = locust_user.locust_executor(self.host, self.port, collection_name, connection_type=connection_type, run_params=run_params)
            logger.info(locust_stats)

        elif run_type == "search_ids_stability":
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
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
            true_ids = np.array(dataset["neighbors"])
            vector_type = self.get_vector_type_from_metric(metric_type)
            vec_field_name = utils.get_default_field_name(vector_type)
            real_metric_type = utils.metric_type_trans(metric_type)

            # re-create collection
            if milvus_instance.exists_collection(collection_name):
                milvus_instance.drop()
                time.sleep(DELETE_INTERVAL_TIME)
            milvus_instance.create_collection(dimension, data_type=vector_type)
            insert_vectors = self.normalize(metric_type, np.array(dataset["train"]))
            if len(insert_vectors) != dataset["train"].shape[0]:
                raise Exception("Row count of insert vectors: %d is not equal to dataset size: %d" % (len(insert_vectors), dataset["train"].shape[0]))
            logger.debug("The row count of entities to be inserted: %d" % len(insert_vectors))
            # insert batch once
            # milvus_instance.insert(insert_vectors)
            loops = len(insert_vectors) // INSERT_INTERVAL + 1
            for i in range(loops):
                start = i*INSERT_INTERVAL
                end = min((i+1)*INSERT_INTERVAL, len(insert_vectors))
                if start < end:
                    tmp_vectors = insert_vectors[start:end]
                    ids = [i for i in range(start, end)]
                    if not isinstance(tmp_vectors, list):
                        entities = milvus_instance.generate_entities(tmp_vectors.tolist(), ids)
                        res_ids = milvus_instance.insert(entities, ids=ids)
                    else:
                        entities = milvus_instance.generate_entities(tmp_vectors, ids)
                        res_ids = milvus_instance.insert(entities, ids=ids)
                    assert res_ids == ids
            milvus_instance.flush()
            res_count = milvus_instance.count()
            logger.info("Table: %s, row count: %d" % (collection_name, res_count))
            if res_count != len(insert_vectors):
                raise Exception("Table row count is not equal to insert vectors")
            for index_type in index_types:
                for index_param in index_params:
                    logger.debug("Building index with param: %s, metric_type: %s" % (json.dumps(index_param), metric_type))
                    milvus_instance.create_index(vec_field_name, index_type, metric_type, index_param=index_param)
                    logger.info("Start preload collection: %s" % collection_name)
                    milvus_instance.preload_collection()
                    for search_param in search_params:
                        for nq in nqs:
                            query_vectors = self.normalize(metric_type, np.array(dataset["test"][:nq]))
                            if not isinstance(query_vectors, list):
                                query_vectors = query_vectors.tolist()
                            for top_k in top_ks:
                                logger.debug("Search nq: %d, top-k: %d, search_param: %s, metric_type: %s" % (nq, top_k, json.dumps(search_param), metric_type))
                                vector_query = {"vector": {vec_field_name: {
                                    "topk": top_k,
                                    "query": query_vectors,
                                    "metric_type": real_metric_type,
                                    "params": search_param}
                                }}
                                result = milvus_instance.query(vector_query)
                                result_ids = milvus_instance.get_ids(result)
                                # pdb.set_trace()
                                acc_value = self.get_recall_value(true_ids[:nq, :top_k].tolist(), result_ids)
                                logger.info("Query ann_accuracy: %s" % acc_value)

        elif run_type == "accuracy":
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
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
            vector_type = self.get_vector_type(data_type)
            vec_field_name = utils.get_default_field_name(vector_type)
            for search_param in search_params:
                headers = ["Nq/Top-k"]
                res = []
                for nq in nqs:
                    tmp_res = []
                    for top_k in top_ks:
                        search_param_group = {
                            "nq": nq,
                            "topk": top_k,
                            "search_param": search_param,
                            "metric_type": metric_type
                        }
                        logger.info("Query params: %s" % json.dumps(search_param_group))
                        result_ids = self.do_query_ids(milvus_instance, collection_name, vec_field_name, top_k, nq, search_param=search_param)
                        mem_used = milvus_instance.get_mem_info()["memory_used"]
                        acc_value = self.get_recall_value(true_ids_all[:nq, :top_k].tolist(), result_ids)
                        logger.info("Query accuracy: %s" % acc_value)
                        tmp_res.append(acc_value)
                        logger.info("Memory usage: %s" % mem_used)
                    res.append(tmp_res)
                headers.extend([str(top_k) for top_k in top_ks])
                logger.info("Search param: %s" % json.dumps(search_param))
                utils.print_table(headers, nqs, res)

        elif run_type == "stability":
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
            during_time = collection["during_time"]
            operations = collection["operations"]
            if not milvus_instance.exists_collection():
                logger.error(milvus_instance.show_collections())
                raise Exception("Table name: %s not existed" % collection_name)
            milvus_instance.preload_collection()
            start_mem_usage = milvus_instance.get_mem_info()["memory_used"]
            start_row_count = milvus_instance.count()
            logger.info(start_row_count)
            vector_type = self.get_vector_type(data_type)
            vec_field_name = utils.get_default_field_name(vector_type)
            real_metric_type = utils.metric_type_trans(metric_type)
            query_vectors = [[random.random() for _ in range(dimension)] for _ in range(10000)]
            if "insert" in operations:
                insert_xb = operations["insert"]["xb"]
            if "delete" in operations:
                delete_xb = operations["delete"]["xb"]
            if "query" in operations:
                g_top_k = int(operations["query"]["top_ks"].split("-")[1])
                l_top_k = int(operations["query"]["top_ks"].split("-")[0])
                g_nq = int(operations["query"]["nqs"].split("-")[1])
                l_nq = int(operations["query"]["nqs"].split("-")[0])
                search_params = operations["query"]["search_params"]
            i = 0
            start_time = time.time()
            while time.time() < start_time + during_time * 60:
                i = i + 1
                q = self.gen_executors(operations)
                for name in q:
                    try:
                        if name == "insert":
                            insert_ids = random.sample(list(range(collection_size)), insert_xb)
                            insert_vectors = [[random.random() for _ in range(dimension)] for _ in range(insert_xb)]
                            entities = milvus_instance.generate_entities(insert_vectors, insert_ids)
                            milvus_instance.insert(entities, ids=insert_ids)
                        elif name == "delete":
                            delete_ids = random.sample(list(range(collection_size)), delete_xb)
                            milvus_instance.delete(delete_ids)
                        elif name == "query":
                            top_k = random.randint(l_top_k, g_top_k)
                            nq = random.randint(l_nq, g_nq)
                            search_param = {}
                            for k, v in search_params.items():
                                search_param[k] = random.randint(int(v.split("-")[0]), int(v.split("-")[1]))
                            logger.debug("Query nq: %d, top-k: %d, param: %s" % (nq, top_k, json.dumps(search_param)))
                            vector_query = {"vector": {vec_field_name: {
                                "topk": top_k,
                                "query": query_vectors[:nq],
                                "metric_type": real_metric_type,
                                "params": search_param}
                            }}
                            result = milvus_instance.query(vector_query)
                        elif name in ["flush", "compact"]:
                            func = getattr(milvus_instance, name)
                            func()
                        logger.debug(milvus_instance.count())
                    except Exception as e:
                        logger.error(name)
                        logger.error(str(e))
                        raise
                logger.debug("Loop time: %d" % i)
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
            concurrent = collection["concurrent"] if "concurrent" in collection else False
            concurrent_num = collection_num
            dimension = collection["dimension"] if "dimension" in collection else 128
            insert_xb = collection["insert_xb"] if "insert_xb" in collection else 100000
            index_types = collection["index_types"] if "index_types" in collection else ['ivf_sq8']
            index_param = {"nlist": 256}
            collection_names = []
            milvus_instances_map = {}
            insert_vectors = [[random.random() for _ in range(dimension)] for _ in range(insert_xb)]
            ids = [i for i in range(insert_xb)]
            # initialize and prepare
            for i in range(collection_num):
                name = utils.get_unique_name(prefix="collection_%d_" % i)
                collection_names.append(name)
                metric_type = random.choice(["l2", "ip"])
                # default float_vector
                milvus_instance = MilvusClient(collection_name=name, host=self.host)
                milvus_instance.create_collection(dimension, other_fields=None)
                index_type = random.choice(index_types)
                field_name = utils.get_default_field_name()
                milvus_instance.create_index(field_name, index_type, metric_type, index_param=index_param)
                logger.info(milvus_instance.describe_index())
                insert_vectors = utils.normalize(metric_type, insert_vectors)
                entities = milvus_instance.generate_entities(insert_vectors, ids)
                res_ids = milvus_instance.insert(entities, ids=ids)
                milvus_instance.flush()
                milvus_instances_map.update({name: milvus_instance})
                logger.info(milvus_instance.describe_index())

            # loop time unit: min -> s
            pull_interval_seconds = pull_interval * 60
            tasks = ["insert_rand", "delete_rand", "query_rand", "flush", "compact"]
            i = 1
            while True:
                logger.info("Loop time: %d" % i)
                start_time = time.time()
                while time.time() - start_time < pull_interval_seconds:
                    if concurrent:
                        threads = []
                        for name in collection_names:
                            task_name = random.choice(tasks)
                            task_run = getattr(milvus_instances_map[name], task_name)
                            t = threading.Thread(target=task_run, args=())
                            threads.append(t)
                            t.start()
                        for t in threads:
                            t.join()
                        # with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_num) as executor:
                        #     future_results = {executor.submit(getattr(milvus_instances_map[mp[j][0]], mp[j][1])): j for j in range(concurrent_num)}
                        #     for future in concurrent.futures.as_completed(future_results):
                        #         future.result()
                    else:
                        tmp_collection_name = random.choice(collection_names)
                        task_name = random.choice(tasks)
                        logger.info(tmp_collection_name)
                        logger.info(task_name)
                        task_run = getattr(milvus_instances_map[tmp_collection_name], task_name)
                        task_run()
                # new connection
                # for name in collection_names:
                #     milvus_instance = MilvusClient(collection_name=name, host=self.host)
                #     milvus_instances_map.update({name: milvus_instance})
                i = i + 1

        elif run_type == "locust_mix_performance":
            (data_type, collection_size, dimension, metric_type) = parser.collection_parser(
                collection_name)
            ni_per = collection["ni_per"]
            build_index = collection["build_index"]
            vector_type = self.get_vector_type(data_type)
            index_field_name = utils.get_default_field_name(vector_type)
            # drop exists collection
            if milvus_instance.exists_collection():
                milvus_instance.drop()
                time.sleep(10)
            # create collection
            other_fields = collection["other_fields"] if "other_fields" in collection else None
            milvus_instance.create_collection(dimension, data_type=DataType.FLOAT_VECTOR, collection_name=collection_name, other_fields=other_fields)
            logger.info(milvus_instance.get_info())
            # insert entities
            insert_vectors = [[random.random() for _ in range(dimension)] for _ in range(ni_per)]
            insert_ids = random.sample(list(range(collection_size)), ni_per)
            insert_vectors = utils.normalize(metric_type, insert_vectors)
            entities = milvus_instance.generate_entities(insert_vectors, insert_ids, collection_name)
            milvus_instance.insert(entities, ids=insert_ids)
            # flush
            milvus_instance.flush()
            logger.info(milvus_instance.get_stats())
            logger.debug("Table row counts: %d" % milvus_instance.count())
            # create index
            if build_index is True:
                index_type = collection["index_type"]
                index_param = collection["index_param"]
                logger.debug("Start build index for last file")
                milvus_instance.create_index(index_field_name, index_type, metric_type, index_param)
                logger.debug(milvus_instance.describe_index())
            # locust
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
            # define def str
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
        vector_query = {"vector": {"%s": {
        "topk": params["top_k"], 
        "query": X, 
        "metric_type": "%s", 
        "params": params["search_param"]}}}
        client.query(vector_query, filter_query=params["filters"], collection_name=collection_name)
                        """ % (weight, task_type["params"], index_field_name, utils.metric_type_trans(metric_type))
                if type == "insert":
                    def_str = """
    @task(%d)
    def insert(self):
        client = get_client(collection_name)
        params = %s
        insert_ids = random.sample(list(range(100000)), params["nb"])
        insert_vectors = [[random.random() for _ in range(dim)] for _ in range(params["nb"])]
        insert_vectors = utils.normalize("l2", insert_vectors)
        entities = generate_entities(insert_vectors, insert_ids)
        client.insert(entities,ids=insert_ids, collection_name=collection_name)
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
                # define locust code str
                code_str = """
import random
import json
from locust import User, task, between
from locust_task import MilvusTask
from client import MilvusClient
import utils

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
  
        
def generate_entities(vectors, ids):
    return m.generate_entities(vectors, ids, collection_name)


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
            raise Exception("Run type not defined")
        logger.debug("All test finished")
