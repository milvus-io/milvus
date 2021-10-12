import json
import time
import copy
import logging
import numpy as np

from milvus_benchmark import parser
from milvus_benchmark.runners import utils
from milvus_benchmark.runners.base import BaseRunner

logger = logging.getLogger("milvus_benchmark.runners.accuracy")
INSERT_INTERVAL = 50000


class AccuracyRunner(BaseRunner):
    """run accuracy"""
    name = "accuracy"

    def __init__(self, env, metric):
        super(AccuracyRunner, self).__init__(env, metric)

    def extract_cases(self, collection):
        collection_name = collection["collection_name"] if "collection_name" in collection else None
        (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
        vector_type = utils.get_vector_type(data_type)
        index_field_name = utils.get_default_field_name(vector_type)
        base_query_vectors = utils.get_vectors_from_binary(utils.MAX_NQ, dimension, data_type)
        collection_info = {
            "dimension": dimension,
            "metric_type": metric_type,
            "dataset_name": collection_name,
            "collection_size": collection_size
        }
        index_info = self.milvus.describe_index(index_field_name, collection_name)
        filters = collection["filters"] if "filters" in collection else []
        filter_query = []
        top_ks = collection["top_ks"]
        nqs = collection["nqs"]
        search_params = collection["search_params"]
        search_params = utils.generate_combinations(search_params)
        cases = list()
        case_metrics = list()
        self.init_metric(self.name, collection_info, index_info, search_info=None)
        for search_param in search_params:
            if not filters:
                filters.append(None)
            for filter in filters:
                filter_param = []
                if isinstance(filter, dict) and "range" in filter:
                    filter_query.append(eval(filter["range"]))
                    filter_param.append(filter["range"])
                if isinstance(filter, dict) and "term" in filter:
                    filter_query.append(eval(filter["term"]))
                    filter_param.append(filter["term"])
                for nq in nqs:
                    query_vectors = base_query_vectors[0:nq]
                    for top_k in top_ks:
                        search_info = {
                            "topk": top_k,
                            "query": query_vectors,
                            "metric_type": utils.metric_type_trans(metric_type),
                            "params": search_param}
                        # TODO: only update search_info
                        case_metric = copy.deepcopy(self.metric)
                        # set metric type as case
                        case_metric.set_case_metric_type()
                        case_metric.search = {
                            "nq": nq,
                            "topk": top_k,
                            "search_param": search_param,
                            "filter": filter_param
                        }
                        vector_query = {"vector": {index_field_name: search_info}}
                        case = {
                            "collection_name": collection_name,
                            "index_field_name": index_field_name,
                            "dimension": dimension,
                            "data_type": data_type,
                            "metric_type": metric_type,
                            "vector_type": vector_type,
                            "collection_size": collection_size,
                            "filter_query": filter_query,
                            "vector_query": vector_query
                        }
                        cases.append(case)
                        case_metrics.append(case_metric)
        return cases, case_metrics

    def prepare(self, **case_param):
        collection_name = case_param["collection_name"]
        self.milvus.set_collection(collection_name)
        if not self.milvus.exists_collection():
            logger.info("collection not exist")
        self.milvus.load_collection(timeout=600)

    def run_case(self, case_metric, **case_param):
        collection_size = case_param["collection_size"]
        nq = case_metric.search["nq"]
        top_k = case_metric.search["topk"]
        query_res = self.milvus.query(case_param["vector_query"], filter_query=case_param["filter_query"])
        true_ids = utils.get_ground_truth_ids(collection_size)
        logger.debug({"true_ids": [len(true_ids[0]), len(true_ids[0])]})
        result_ids = self.milvus.get_ids(query_res)
        logger.debug({"result_ids": len(result_ids[0])})
        acc_value = utils.get_recall_value(true_ids[:nq, :top_k].tolist(), result_ids)
        tmp_result = {"acc": acc_value}
        return tmp_result


class AccAccuracyRunner(AccuracyRunner):
    """run ann accuracy"""
    """
    1. entities from hdf5
    2. one collection test different index
    """
    name = "ann_accuracy"

    def __init__(self, env, metric):
        super(AccAccuracyRunner, self).__init__(env, metric)

    def extract_cases(self, collection):
        collection_name = collection["collection_name"] if "collection_name" in collection else None
        (data_type, dimension, metric_type) = parser.parse_ann_collection_name(collection_name)
        # hdf5_source_file: The path of the source data file saved on the NAS
        hdf5_source_file = collection["source_file"]
        index_types = collection["index_types"]
        index_params = collection["index_params"]
        top_ks = collection["top_ks"]
        nqs = collection["nqs"]
        search_params = collection["search_params"]
        vector_type = utils.get_vector_type(data_type)
        index_field_name = utils.get_default_field_name(vector_type)
        dataset = utils.get_dataset(hdf5_source_file)
        collection_info = {
            "dimension": dimension,
            "metric_type": metric_type,
            "dataset_name": collection_name
        }
        filters = collection["filters"] if "filters" in collection else []
        filter_query = []
        # Convert list data into a set of dictionary data
        search_params = utils.generate_combinations(search_params)
        index_params = utils.generate_combinations(index_params)
        cases = list()
        case_metrics = list()
        self.init_metric(self.name, collection_info, {}, search_info=None)

        # true_ids: The data set used to verify the results returned by query
        true_ids = np.array(dataset["neighbors"])
        for index_type in index_types:
            for index_param in index_params:
                index_info = {
                    "index_type": index_type,
                    "index_param": index_param
                }
                for search_param in search_params:
                    if not filters:
                        filters.append(None)
                    for filter in filters:
                        filter_param = []
                        if isinstance(filter, dict) and "range" in filter:
                            filter_query.append(eval(filter["range"]))
                            filter_param.append(filter["range"])
                        if isinstance(filter, dict) and "term" in filter:
                            filter_query.append(eval(filter["term"]))
                            filter_param.append(filter["term"])
                        for nq in nqs:
                            query_vectors = utils.normalize(metric_type, np.array(dataset["test"][:nq]))
                            for top_k in top_ks:
                                search_info = {
                                    "topk": top_k,
                                    "query": query_vectors,
                                    "metric_type": utils.metric_type_trans(metric_type),
                                    "params": search_param}
                                # TODO: only update search_info
                                case_metric = copy.deepcopy(self.metric)
                                # set metric type as case
                                case_metric.set_case_metric_type()
                                case_metric.index = index_info
                                case_metric.search = {
                                    "nq": nq,
                                    "topk": top_k,
                                    "search_param": search_param,
                                    "filter": filter_param
                                }
                                vector_query = {"vector": {index_field_name: search_info}}
                                case = {
                                    "collection_name": collection_name,
                                    "dataset": dataset,
                                    "index_field_name": index_field_name,
                                    "dimension": dimension,
                                    "data_type": data_type,
                                    "metric_type": metric_type,
                                    "vector_type": vector_type,
                                    "index_type": index_type,
                                    "index_param": index_param,
                                    "filter_query": filter_query,
                                    "vector_query": vector_query,
                                    "true_ids": true_ids
                                }
                                # Obtain the parameters of the use case to be tested
                                cases.append(case)
                                case_metrics.append(case_metric)
        return cases, case_metrics

    def prepare(self, **case_param):
        """ According to the test case parameters, initialize the test """

        collection_name = case_param["collection_name"]
        metric_type = case_param["metric_type"]
        dimension = case_param["dimension"]
        vector_type = case_param["vector_type"]
        index_type = case_param["index_type"]
        index_param = case_param["index_param"]
        index_field_name = case_param["index_field_name"]
        
        self.milvus.set_collection(collection_name)
        if self.milvus.exists_collection(collection_name):
            logger.info("Re-create collection: %s" % collection_name)
            self.milvus.drop()
        dataset = case_param["dataset"]
        self.milvus.create_collection(dimension, data_type=vector_type)
        # Get the data set train for inserting into the collection
        insert_vectors = utils.normalize(metric_type, np.array(dataset["train"]))
        if len(insert_vectors) != dataset["train"].shape[0]:
            raise Exception("Row count of insert vectors: %d is not equal to dataset size: %d" % (
                len(insert_vectors), dataset["train"].shape[0]))
        logger.debug("The row count of entities to be inserted: %d" % len(insert_vectors))
        # Insert batch once
        # milvus_instance.insert(insert_vectors)
        info = self.milvus.get_info(collection_name)
        loops = len(insert_vectors) // INSERT_INTERVAL + 1
        for i in range(loops):
            start = i * INSERT_INTERVAL
            end = min((i + 1) * INSERT_INTERVAL, len(insert_vectors))
            if start < end:
                # Insert up to INSERT_INTERVAL=50000 at a time
                tmp_vectors = insert_vectors[start:end]
                ids = [i for i in range(start, end)]
                if not isinstance(tmp_vectors, list):
                    entities = utils.generate_entities(info, tmp_vectors.tolist(), ids)
                    res_ids = self.milvus.insert(entities)
                else:
                    entities = utils.generate_entities(tmp_vectors, ids)
                    res_ids = self.milvus.insert(entities)
                assert res_ids == ids
        logger.debug("End insert, start flush")
        self.milvus.flush()
        logger.debug("End flush")
        res_count = self.milvus.count()
        logger.info("Table: %s, row count: %d" % (collection_name, res_count))
        if res_count != len(insert_vectors):
            raise Exception("Table row count is not equal to insert vectors")
        if self.milvus.describe_index(index_field_name):
            self.milvus.drop_index(index_field_name)
            logger.info("Re-create index: %s" % collection_name)
        self.milvus.create_index(index_field_name, index_type, metric_type, index_param=index_param)
        logger.info(self.milvus.describe_index(index_field_name))
        logger.info("Start load collection: %s" % collection_name)
        # self.milvus.release_collection()
        self.milvus.load_collection(timeout=600)
        logger.info("End load collection: %s" % collection_name)

    def run_case(self, case_metric, **case_param):
        true_ids = case_param["true_ids"]
        nq = case_metric.search["nq"]
        top_k = case_metric.search["topk"]
        query_res = self.milvus.query(case_param["vector_query"], filter_query=case_param["filter_query"])
        result_ids = self.milvus.get_ids(query_res)
        # Calculate the accuracy of the result of query
        acc_value = utils.get_recall_value(true_ids[:nq, :top_k].tolist(), result_ids)
        tmp_result = {"acc": acc_value}
        # Return accuracy results for reporting
        return tmp_result

