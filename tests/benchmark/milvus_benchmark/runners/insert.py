import time
import pdb
import copy
import logging
from milvus_benchmark import parser
from milvus_benchmark.runners import utils
from milvus_benchmark.runners.base import BaseRunner

logger = logging.getLogger("milvus_benchmark.runners.insert")


class InsertRunner(BaseRunner):
    """run insert"""
    name = "insert_performance"

    def __init__(self, env, metric):
        super(InsertRunner, self).__init__(env, metric)

    def extract_cases(self, collection):
        collection_name = collection["collection_name"] if "collection_name" in collection else None
        (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
        ni_per = collection["ni_per"]
        build_index = collection["build_index"] if "build_index" in collection else False
        index_info = None
        vector_type = utils.get_vector_type(data_type)
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
        if build_index is True:
            index_type = collection["index_type"]
            index_param = collection["index_param"]
            index_info = {
                "index_type": index_type,
                "index_param": index_param
            }
            index_field_name = utils.get_default_field_name(vector_type)
        flush = True
        if "flush" in collection and collection["flush"] == "no":
            flush = False
        self.init_metric(self.name, collection_info, index_info, None)
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
            "flush_after_insert": flush,
            "index_field_name": index_field_name,
            "index_type": index_type,
            "index_param": index_param,
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
            time.sleep(utils.DELETE_INTERVAL_TIME)
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

    # TODO: error handler
    def run_case(self, case_metric, **case_param):
        collection_name = case_param["collection_name"]
        dimension = case_param["dimension"]
        index_field_name = case_param["index_field_name"]
        build_index = case_param["build_index"]

        tmp_result = self.insert(self.milvus, collection_name, case_param["data_type"], dimension, case_param["collection_size"], case_param["ni_per"])
        flush_time = 0.0
        build_time = 0.0
        if case_param["flush_after_insert"] is True:
            start_time = time.time()
            self.milvus.flush()
            flush_time = round(time.time()-start_time, 2)
            logger.debug(self.milvus.count())
        if build_index is True:
            logger.debug("Start build index for last file")
            start_time = time.time()
            self.milvus.create_index(index_field_name, case_param["index_type"], case_param["metric_type"], index_param=case_param["index_param"])
            build_time = round(time.time()-start_time, 2)
        tmp_result.update({"flush_time": flush_time, "build_time": build_time})
        return tmp_result


class BPInsertRunner(BaseRunner):
    """run insert"""
    name = "bp_insert_performance"

    def __init__(self, env, metric):
        super(BPInsertRunner, self).__init__(env, metric)

    def extract_cases(self, collection):
        collection_name = collection["collection_name"] if "collection_name" in collection else None
        (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
        ni_pers = collection["ni_pers"]
        build_index = collection["build_index"] if "build_index" in collection else False
        index_info = None
        vector_type = utils.get_vector_type(data_type)
        other_fields = collection["other_fields"] if "other_fields" in collection else None
        index_field_name = None
        index_type = None
        index_param = None
        if build_index is True:
            index_type = collection["index_type"]
            index_param = collection["index_param"]
            index_info = {
                "index_type": index_type,
                "index_param": index_param
            }
            index_field_name = utils.get_default_field_name(vector_type)
        flush = True
        if "flush" in collection and collection["flush"] == "no":
            flush = False
        case_metrics = list()
        case_params = list()
        
        for ni_per in ni_pers:
            collection_info = {
                "dimension": dimension,
                "metric_type": metric_type,
                "dataset_name": collection_name,
                "collection_size": collection_size,
                "other_fields": other_fields,
                "ni_per": ni_per
            }
            self.init_metric(self.name, collection_info, index_info, None)
            case_metric = copy.deepcopy(self.metric)
            case_metric.set_case_metric_type()
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
                "flush_after_insert": flush,
                "index_field_name": index_field_name,
                "index_type": index_type,
                "index_param": index_param,
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
            time.sleep(utils.DELETE_INTERVAL_TIME)
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

    # TODO: error handler
    def run_case(self, case_metric, **case_param):
        collection_name = case_param["collection_name"]
        dimension = case_param["dimension"]
        index_field_name = case_param["index_field_name"]
        build_index = case_param["build_index"]
        # TODO:
        tmp_result = self.insert(self.milvus, collection_name, case_param["data_type"], dimension, case_param["collection_size"], case_param["ni_per"])
        flush_time = 0.0
        build_time = 0.0
        if case_param["flush_after_insert"] is True:
            start_time = time.time()
            self.milvus.flush()
            flush_time = round(time.time()-start_time, 2)
            logger.debug(self.milvus.count())
        if build_index is True:
            logger.debug("Start build index for last file")
            start_time = time.time()
            self.milvus.create_index(index_field_name, case_param["index_type"], case_param["metric_type"], index_param=case_param["index_param"])
            build_time = round(time.time()-start_time, 2)
        tmp_result.update({"flush_time": flush_time, "build_time": build_time})
        return tmp_result
