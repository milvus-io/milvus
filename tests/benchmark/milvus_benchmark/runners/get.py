import time
import copy
import logging
from milvus_benchmark import parser
from milvus_benchmark.runners import utils
from milvus_benchmark.runners.base import BaseRunner

logger = logging.getLogger("milvus_benchmark.runners.get")


def get_ids(length, size):
    ids_list = []
    step = size // length
    for i in range(length):
        ids_list.append(step * i)
    return ids_list


class GetRunner(BaseRunner):
    """run get"""
    name = "get_performance"

    def __init__(self, env, metric):
        super(GetRunner, self).__init__(env, metric)

    def extract_cases(self, collection):
        collection_name = collection["collection_name"] if "collection_name" in collection else None
        (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
        ni_per = collection["ni_per"]
        vector_type = utils.get_vector_type(data_type)
        other_fields = collection["other_fields"] if "other_fields" in collection else None
        ids_length_list = collection["ids_length_list"]
        collection_info = {
            "dimension": dimension,
            "metric_type": metric_type,
            "dataset_name": collection_name,
            "collection_size": collection_size,
            "other_fields": other_fields,
            "ni_per": ni_per
        }
        index_field_name = utils.get_default_field_name(vector_type)
        index_type = collection["index_type"]
        index_param = collection["index_param"]
        index_info = {
            "index_type": index_type,
            "index_param": index_param
        }
        flush = True
        if "flush" in collection and collection["flush"] == "no":
            flush = False
        self.init_metric(self.name, collection_info, index_info, search_info=None)
        case_metrics = list()
        for ids_length in ids_length_list:
            ids = get_ids(ids_length, collection_size)
            case_metric = copy.deepcopy(self.metric)
            # set metric type as case
            case_metric.set_case_metric_type()
            case_params = list()
            case_metric.run_params = {"ids_length": ids_length}
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
                "flush_after_insert": flush,
                "index_field_name": index_field_name,
                "index_type": index_type,
                "index_param": index_param,
                "ids": ids
            }
            case_params.append(case_param)
        return case_params, case_metrics

    def prepare(self, **case_param):
        collection_name = case_param["collection_name"]
        self.milvus.set_collection(collection_name)
        if not self.milvus.exists_collection():
            logger.info("collection not exist")
        logger.debug({"collection count": self.milvus.count()})

    def run_case(self, case_metric, **case_param):
        ids = case_param["ids"]
        start_time = time.time()
        self.milvus.get(ids)
        get_time = round(time.time() - start_time, 2)
        tmp_result = {"get_time": get_time}
        return tmp_result


class InsertGetRunner(GetRunner):
    """run insert and get"""
    name = "insert_get_performance"

    def __init__(self, env, metric):
        super(InsertGetRunner, self).__init__(env, metric)

    def prepare(self, **case_param):
        collection_name = case_param["collection_name"]
        dimension = case_param["dimension"]
        vector_type = case_param["vector_type"]
        other_fields = case_param["other_fields"]
        self.milvus.set_collection(collection_name)
        if self.milvus.exists_collection():
            logger.debug("Start drop collection")
            self.milvus.drop()
            time.sleep(utils.DELETE_INTERVAL_TIME)
        self.milvus.create_collection(dimension, data_type=vector_type, other_fields=other_fields)
        self.insert(self.milvus, collection_name, case_param["data_type"], dimension,
                               case_param["collection_size"], case_param["ni_per"])
        start_time = time.time()
        self.milvus.flush()
        flush_time = round(time.time() - start_time, 2)
        logger.debug({"collection count": self.milvus.count()})
        logger.debug({"flush_time": flush_time})
        logger.debug("Start load collection")
        self.milvus.load_collection(timeout=1200)
        logger.debug("Load collection end")
