import time
import pdb
import copy
import json
import logging
from milvus_benchmark import parser
from milvus_benchmark.runners import utils
from milvus_benchmark.runners.base import BaseRunner

logger = logging.getLogger("milvus_benchmark.runners.search")


class SearchRunner(BaseRunner):
    """run search"""
    name = "search_performance"

    def __init__(self, env, metric):
        super(SearchRunner, self).__init__(env, metric)

    def extract_cases(self, collection):
        collection_name = collection["collection_name"] if "collection_name" in collection else None
        (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
        run_count = collection["run_count"]
        top_ks = collection["top_ks"]
        nqs = collection["nqs"]
        filters = collection["filters"] if "filters" in collection else []
        
        search_params = collection["search_params"]
        # TODO: get fields by describe_index
        # fields = self.get_fields(self.milvus, collection_name)
        fields = None
        collection_info = {
            "dimension": dimension,
            "metric_type": metric_type,
            "dataset_name": collection_name,
            "collection_size": collection_size,
            "fields": fields
        }
        # TODO: need to get index_info
        index_info = None
        vector_type = utils.get_vector_type(data_type)
        index_field_name = utils.get_default_field_name(vector_type)
        base_query_vectors = utils.get_vectors_from_binary(utils.MAX_NQ, dimension, data_type)
        cases = list()
        case_metrics = list()
        self.init_metric(self.name, collection_info, index_info, None)
        for search_param in search_params:
            logger.info("Search param: %s" % json.dumps(search_param))
            for filter in filters:
                filter_query = []
                filter_param = []
                if filter and isinstance(filter, dict):
                    if "range" in filter:
                        filter_query.append(eval(filter["range"]))
                        filter_param.append(filter["range"])
                    elif "term" in filter:
                        filter_query.append(eval(filter["term"]))
                        filter_param.append(filter["term"])
                    else:
                        raise Exception("%s not supported" % filter)
                logger.info("filter param: %s" % json.dumps(filter_param))
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
                            "run_count": run_count,
                            "filter_query": filter_query,
                            "vector_query": vector_query,
                        }
                        cases.append(case)
                        case_metrics.append(case_metric)
        return cases, case_metrics

    def prepare(self, **case_param):
        collection_name = case_param["collection_name"]
        self.milvus.set_collection(collection_name)
        if not self.milvus.exists_collection():
            logger.error("collection name: {} not existed".format(collection_name))
            return False
        logger.debug(self.milvus.count())
        logger.info("Start load collection")
        self.milvus.load_collection(timeout=1200)
        # TODO: enable warm query
        # self.milvus.warm_query(index_field_name, search_params[0], times=2)

    def run_case(self, case_metric, **case_param):
        # index_field_name = case_param["index_field_name"]
        run_count = case_param["run_count"]
        avg_query_time = 0.0
        min_query_time = 0.0
        total_query_time = 0.0        
        for i in range(run_count):
            logger.debug("Start run query, run %d of %s" % (i+1, run_count))
            start_time = time.time()
            _query_res = self.milvus.query(case_param["vector_query"], filter_query=case_param["filter_query"])
            interval_time = time.time() - start_time
            total_query_time += interval_time
            if (i == 0) or (min_query_time > interval_time):
                min_query_time = round(interval_time, 2)
        avg_query_time = round(total_query_time/run_count, 2)
        tmp_result = {"search_time": min_query_time, "avc_search_time": avg_query_time}
        return tmp_result


class InsertSearchRunner(BaseRunner):
    """run insert and search"""
    name = "insert_search_performance"

    def __init__(self, env, metric):
        super(InsertSearchRunner, self).__init__(env, metric)
        self.build_time = None
        self.insert_result = None

    def extract_cases(self, collection):
        collection_name = collection["collection_name"] if "collection_name" in collection else None
        (data_type, collection_size, dimension, metric_type) = parser.collection_parser(collection_name)
        build_index = collection["build_index"] if "build_index" in collection else False
        index_type = collection["index_type"] if "index_type" in collection else None
        index_param = collection["index_param"] if "index_param" in collection else None
        run_count = collection["run_count"]
        top_ks = collection["top_ks"]
        nqs = collection["nqs"]
        other_fields = collection["other_fields"] if "other_fields" in collection else None
        filters = collection["filters"] if "filters" in collection else []
        filter_query = []
        search_params = collection["search_params"]
        ni_per = collection["ni_per"]

        # TODO: get fields by describe_index
        # fields = self.get_fields(self.milvus, collection_name)
        fields = None
        collection_info = {
            "dimension": dimension,
            "metric_type": metric_type,
            "dataset_name": collection_name,
            "fields": fields
        }
        index_info = {
            "index_type": index_type,
            "index_param": index_param
        }
        vector_type = utils.get_vector_type(data_type)
        index_field_name = utils.get_default_field_name(vector_type)
        # Get the path of the query.npy file stored on the NAS and get its data
        base_query_vectors = utils.get_vectors_from_binary(utils.MAX_NQ, dimension, data_type)
        cases = list()
        case_metrics = list()
        self.init_metric(self.name, collection_info, index_info, None)
        
        for search_param in search_params:
            if not filters:
                filters.append(None)
            for filter in filters:
                # filter_param = []
                filter_query = []
                if isinstance(filter, dict) and "range" in filter:
                    filter_query.append(eval(filter["range"]))
                    # filter_param.append(filter["range"])
                if isinstance(filter, dict) and "term" in filter:
                    filter_query.append(eval(filter["term"]))
                    # filter_param.append(filter["term"])
                # logger.info("filter param: %s" % json.dumps(filter_param))
                for nq in nqs:
                    # Take nq groups of data for query
                    query_vectors = base_query_vectors[0:nq]
                    for top_k in top_ks:
                        search_info = {
                            "topk": top_k, 
                            "query": query_vectors, 
                            "metric_type": utils.metric_type_trans(metric_type), 
                            "params": search_param}
                        # TODO: only update search_info
                        case_metric = copy.deepcopy(self.metric)
                        case_metric.set_case_metric_type()
                        case_metric.search = {
                            "nq": nq,
                            "topk": top_k,
                            "search_param": search_param,
                            "filter": filter_query
                        }
                        vector_query = {"vector": {index_field_name: search_info}}
                        case = {
                            "collection_name": collection_name,
                            "index_field_name": index_field_name,
                            "other_fields": other_fields,
                            "dimension": dimension,
                            "data_type": data_type,
                            "vector_type": vector_type,
                            "collection_size": collection_size,
                            "ni_per": ni_per,
                            "build_index": build_index,
                            "index_type": index_type,
                            "index_param": index_param,
                            "metric_type": metric_type,
                            "run_count": run_count,
                            "filter_query": filter_query,
                            "vector_query": vector_query,
                        }
                        cases.append(case)
                        case_metrics.append(case_metric)
        return cases, case_metrics

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
        insert_result = self.insert(self.milvus, collection_name, case_param["data_type"], dimension, case_param["collection_size"], case_param["ni_per"])
        self.insert_result = insert_result
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
        # build_time includes flush and index time
        logger.debug({"flush_time": flush_time, "build_time": build_time})
        self.build_time = build_time
        logger.info(self.milvus.count())
        logger.info("Start load collection")
        load_start_time = time.time() 
        self.milvus.load_collection(timeout=1200)
        logger.debug({"load_time": round(time.time()-load_start_time, 2)})
        
    def run_case(self, case_metric, **case_param):
        run_count = case_param["run_count"]
        avg_query_time = 0.0
        min_query_time = 0.0
        total_query_time = 0.0        
        for i in range(run_count):
            # Number of successive queries
            logger.debug("Start run query, run %d of %s" % (i+1, run_count))
            logger.info(case_metric.search)
            start_time = time.time()
            _query_res = self.milvus.query(case_param["vector_query"], filter_query=case_param["filter_query"])
            interval_time = time.time() - start_time
            total_query_time += interval_time
            if (i == 0) or (min_query_time > interval_time):
                min_query_time = round(interval_time, 2)
        avg_query_time = round(total_query_time/run_count, 2)
        logger.info("Min query time: %.2f, avg query time: %.2f" % (min_query_time, avg_query_time))
        # insert_result: "total_time", "rps", "ni_time"
        tmp_result = {"insert": self.insert_result, "build_time": self.build_time, "search_time": min_query_time, "avc_search_time": avg_query_time}
        # 
        # logger.info("Start load collection")
        # self.milvus.load_collection(timeout=1200)
        # logger.info("Release load collection")
        # self.milvus.release_collection()
        return tmp_result