import pdb
import time
import copy
import logging
from . import locust_user
from .base import BaseRunner
from milvus_benchmark import parser
from milvus_benchmark import utils
from milvus_benchmark.runners import utils as runner_utils

logger = logging.getLogger("milvus_benchmark.runners.locust")


class LocustRunner(BaseRunner):
    def __init__(self, env, metric):
        super(LocustRunner, self).__init__(env, metric)

    def run_case(self, case_metric, **case_param):
        collection_name = case_param["collection_name"]
        task = case_param["task"]
        connection_type = case_param["connection_type"]

        # spawn locust requests
        task["during_time"] = utils.timestr_to_int(task["during_time"])
        task_types = task["types"]
        run_params = {"tasks": {}}
        run_params.update(task)
        info_in_params = {
            "index_field_name": case_param["index_field_name"],
            "vector_field_name": case_param["vector_field_name"],
            "dimension": case_param["dimension"],
            "collection_info": self.milvus.get_info(collection_name)}
        logger.info(info_in_params)
        run_params.update({"op_info": info_in_params})
        for task_type in task_types:
            run_params["tasks"].update({
                    task_type["type"]: {
                        "weight": task_type["weight"] if "weight" in task_type else 1,
                        "params": task_type["params"] if "params" in task_type else None,
                    }
                })
        # collect stats
        # pdb.set_trace()
        logger.info(run_params)
        locust_stats = locust_user.locust_executor(self.hostname, self.port, collection_name,
                                                   connection_type=connection_type, run_params=run_params)
        return locust_stats


class LocustInsertRunner(LocustRunner):
    """run insert"""
    name = "locust_insert_performance"

    def __init__(self, env, metric):
        super(LocustInsertRunner, self).__init__(env, metric)

    def extract_cases(self, collection):
        collection_name = collection["collection_name"] if "collection_name" in collection else None

        (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
        ni_per = collection["ni_per"]
        build_index = collection["build_index"] if "build_index" in collection else False
        vector_type = runner_utils.get_vector_type(data_type)
        other_fields = collection["other_fields"] if "other_fields" in collection else None
        collection_info = {
            "dimension": dimension,
            "metric_type": metric_type,
            "dataset_name": collection_name,
            "collection_size": collection_size,
            "other_fields": other_fields,
            "ni_per": ni_per
        }
        index_field_name = None
        index_type = None
        index_param = None
        index_info = None
        vector_field_name = runner_utils.get_default_field_name(vector_type)
        if build_index is True:
            index_type = collection["index_type"]
            index_param = collection["index_param"]
            index_info = {
                "index_type": index_type,
                "index_param": index_param
            }
            index_field_name = runner_utils.get_default_field_name(vector_type)
        task = collection["task"]
        connection_type = "single"
        connection_num = task["connection_num"]
        if connection_num > 1:
            connection_type = "multi"
        run_params = {
            "task": collection["task"],
            "connection_type": connection_type,
        }
        self.init_metric(self.name, collection_info, index_info, None, run_params)
        case_metric = copy.deepcopy(self.metric)
        # set metric type as case
        case_metric.set_case_metric_type()
        case_metrics = list()
        case_params = list()
        case_metrics.append(case_metric)
        case_param = {
            "collection_name": collection_name,
            "data_type": data_type,
            "dimension": dimension,
            "collection_size": collection_size,
            "ni_per": ni_per,
            "metric_type": metric_type,
            "vector_type": vector_type,
            "other_fields": other_fields,
            "build_index": build_index,
            "index_field_name": index_field_name,
            "vector_field_name": vector_field_name,
            "index_type": index_type,
            "index_param": index_param,
            "task": collection["task"],
            "connection_type": connection_type,
        }
        case_params.append(case_param)
        return case_params, case_metrics

    def prepare(self, **case_param):
        collection_name = case_param["collection_name"]
        dimension = case_param["dimension"]
        vector_type = case_param["vector_type"]
        other_fields = case_param["other_fields"]
        index_field_name = case_param["index_field_name"]
        build_index = case_param["build_index"]

        self.milvus.set_collection(collection_name)
        if self.milvus.exists_collection():
            logger.debug("Start drop collection")
            self.milvus.drop()
            time.sleep(runner_utils.DELETE_INTERVAL_TIME)
        self.milvus.create_collection(dimension, data_type=vector_type,
                                          other_fields=other_fields)
        # TODO: update fields in collection_info
        # fields = self.get_fields(self.milvus, collection_name)
        # collection_info = {
        #     "dimension": dimension,
        #     "metric_type": metric_type,
        #     "dataset_name": collection_name,
        #     "fields": fields
        # }
        if build_index is True:
            if case_param["index_type"]:
                self.milvus.create_index(index_field_name, case_param["index_type"], case_param["metric_type"], index_param=case_param["index_param"])
                logger.debug(self.milvus.describe_index(index_field_name))
            else:
                build_index = False
                logger.warning("Please specify the index_type")


class LocustSearchRunner(LocustRunner):
    """run search"""
    name = "locust_search_performance"

    def __init__(self, env, metric):
        super(LocustSearchRunner, self).__init__(env, metric)

    def extract_cases(self, collection):
        collection_name = collection["collection_name"] if "collection_name" in collection else None
        (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
        ni_per = collection["ni_per"]
        build_index = collection["build_index"] if "build_index" in collection else False
        vector_type = runner_utils.get_vector_type(data_type)
        other_fields = collection["other_fields"] if "other_fields" in collection else None

        collection_info = {
            "dimension": dimension,
            "metric_type": metric_type,
            "dataset_name": collection_name,
            "collection_size": collection_size,
            "other_fields": other_fields,
            "ni_per": ni_per
        }
        index_field_name = None
        index_type = None
        index_param = None
        index_info = None
        if build_index is True:
            index_type = collection["index_type"]
            index_param = collection["index_param"]
            index_info = {
                "index_type": index_type,
                "index_param": index_param
            }
            index_field_name = runner_utils.get_default_field_name(vector_type)
        vector_field_name = runner_utils.get_default_field_name(vector_type)
        task = collection["task"]
        connection_type = "single"
        connection_num = task["connection_num"]
        if connection_num > 1:
            connection_type = "multi"
        run_params = {
            "task": collection["task"],
            "connection_type": connection_type,
        }
        self.init_metric(self.name, collection_info, index_info, None, run_params)
        case_metric = copy.deepcopy(self.metric)
        # set metric type as case
        case_metric.set_case_metric_type()
        case_metrics = list()
        case_params = list()
        case_metrics.append(case_metric)
        case_param = {
            "collection_name": collection_name,
            "data_type": data_type,
            "dimension": dimension,
            "collection_size": collection_size,
            "ni_per": ni_per,
            "metric_type": metric_type,
            "vector_type": vector_type,
            "other_fields": other_fields,
            "build_index": build_index,
            "index_field_name": index_field_name,
            "vector_field_name": vector_field_name,
            "index_type": index_type,
            "index_param": index_param,
            "task": collection["task"],
            "connection_type": connection_type,
        }
        case_params.append(case_param)
        return case_params, case_metrics

    def prepare(self, **case_param):
        collection_name = case_param["collection_name"]
        dimension = case_param["dimension"]
        vector_type = case_param["vector_type"]
        other_fields = case_param["other_fields"]
        index_field_name = case_param["index_field_name"]
        metric_type = case_param["metric_type"]
        build_index = case_param["build_index"]

        self.milvus.set_collection(collection_name)
        if self.milvus.exists_collection():
            logger.debug("Start drop collection")
            self.milvus.drop()
            time.sleep(runner_utils.DELETE_INTERVAL_TIME)
        self.milvus.create_collection(dimension, data_type=vector_type,
                                          other_fields=other_fields)
        # TODO: update fields in collection_info
        # fields = self.get_fields(self.milvus, collection_name)
        # collection_info = {
        #     "dimension": dimension,
        #     "metric_type": metric_type,
        #     "dataset_name": collection_name,
        #     "fields": fields
        # }
        if build_index is True:
            if case_param["index_type"]:
                self.milvus.create_index(index_field_name, case_param["index_type"], case_param["metric_type"], index_param=case_param["index_param"])
                logger.debug(self.milvus.describe_index(index_field_name))
            else:
                build_index = False
                logger.warning("Please specify the index_type")
        self.insert(self.milvus, collection_name, case_param["data_type"], dimension, case_param["collection_size"], case_param["ni_per"])
        build_time = 0.0
        start_time = time.time()
        self.milvus.flush()
        flush_time = round(time.time()-start_time, 2)
        logger.debug(self.milvus.count())
        if build_index is True:
            logger.debug("Start build index for last file")
            start_time = time.time()
            self.milvus.create_index(index_field_name, case_param["index_type"], case_param["metric_type"], index_param=case_param["index_param"])
            build_time = round(time.time()-start_time, 2)
        logger.debug({"flush_time": flush_time, "build_time": build_time})
        logger.info(self.milvus.count())
        logger.info("Start load collection")
        load_start_time = time.time() 
        self.milvus.load_collection()
        logger.debug({"load_time": round(time.time()-load_start_time, 2)})
        # search_param = None
        # for op in case_param["task"]["types"]:
        #     if op["type"] == "query":
        #         search_param = op["params"]["search_param"]
        #         break
        # logger.info("index_field_name: {}".format(index_field_name))
        # TODO: enable warm query
        # self.milvus.warm_query(index_field_name, search_param, metric_type, times=2)


class LocustRandomRunner(LocustRunner):
    """run random interface"""
    name = "locust_random_performance"

    def __init__(self, env, metric):
        super(LocustRandomRunner, self).__init__(env, metric)

    def extract_cases(self, collection):
        collection_name = collection["collection_name"] if "collection_name" in collection else None
        (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
        ni_per = collection["ni_per"]
        build_index = collection["build_index"] if "build_index" in collection else False
        vector_type = runner_utils.get_vector_type(data_type)
        other_fields = collection["other_fields"] if "other_fields" in collection else None

        collection_info = {
            "dimension": dimension,
            "metric_type": metric_type,
            "dataset_name": collection_name,
            "collection_size": collection_size,
            "other_fields": other_fields,
            "ni_per": ni_per
        }
        index_field_name = None
        index_type = None
        index_param = None
        index_info = None
        vector_field_name = runner_utils.get_default_field_name(vector_type)
        if build_index is True:
            index_type = collection["index_type"]
            index_param = collection["index_param"]
            index_info = {
                "index_type": index_type,
                "index_param": index_param
            }
            index_field_name = runner_utils.get_default_field_name(vector_type)
        task = collection["task"]
        connection_type = "single"
        connection_num = task["connection_num"]
        if connection_num > 1:
            connection_type = "multi"
        run_params = {
            "task": collection["task"],
            "connection_type": connection_type,
        }
        self.init_metric(self.name, collection_info, index_info, None, run_params)
        case_metric = copy.deepcopy(self.metric)
        case_metric.set_case_metric_type()
        case_metrics = list()
        case_params = list()
        case_metrics.append(case_metric)
        case_param = {
            "collection_name": collection_name,
            "data_type": data_type,
            "dimension": dimension,
            "collection_size": collection_size,
            "ni_per": ni_per,
            "metric_type": metric_type,
            "vector_type": vector_type,
            "other_fields": other_fields,
            "build_index": build_index,
            "index_field_name": index_field_name,
            "vector_field_name": vector_field_name,
            "index_type": index_type,
            "index_param": index_param,
            "task": collection["task"],
            "connection_type": connection_type,
        }
        case_params.append(case_param)
        return case_params, case_metrics

    def prepare(self, **case_param):
        collection_name = case_param["collection_name"]
        dimension = case_param["dimension"]
        vector_type = case_param["vector_type"]
        other_fields = case_param["other_fields"]
        index_field_name = case_param["index_field_name"]
        build_index = case_param["build_index"]

        self.milvus.set_collection(collection_name)
        if self.milvus.exists_collection():
            logger.debug("Start drop collection")
            self.milvus.drop()
            time.sleep(runner_utils.DELETE_INTERVAL_TIME)
        self.milvus.create_collection(dimension, data_type=vector_type,
                                          other_fields=other_fields)
        # TODO: update fields in collection_info
        # fields = self.get_fields(self.milvus, collection_name)
        # collection_info = {
        #     "dimension": dimension,
        #     "metric_type": metric_type,
        #     "dataset_name": collection_name,
        #     "fields": fields
        # }
        if build_index is True:
            if case_param["index_type"]:
                self.milvus.create_index(index_field_name, case_param["index_type"], case_param["metric_type"], index_param=case_param["index_param"])
                logger.debug(self.milvus.describe_index(index_field_name))
            else:
                build_index = False
                logger.warning("Please specify the index_type")
        self.insert(self.milvus, collection_name, case_param["data_type"], dimension, case_param["collection_size"], case_param["ni_per"])
        build_time = 0.0
        start_time = time.time()
        self.milvus.flush()
        flush_time = round(time.time()-start_time, 2)
        logger.debug(self.milvus.count())
        if build_index is True:
            logger.debug("Start build index for last file")
            start_time = time.time()
            self.milvus.create_index(index_field_name, case_param["index_type"], case_param["metric_type"], index_param=case_param["index_param"])
            build_time = round(time.time()-start_time, 2)
        logger.debug({"flush_time": flush_time, "build_time": build_time})
        logger.info(self.milvus.count())
        logger.info("Start load collection")
        load_start_time = time.time() 
        self.milvus.load_collection()
        logger.debug({"load_time": round(time.time()-load_start_time, 2)})
