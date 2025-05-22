import numpy as np
from pymilvus.orm.types import CONSISTENCY_STRONG, CONSISTENCY_BOUNDED, CONSISTENCY_SESSION, CONSISTENCY_EVENTUALLY
from pymilvus import AnnSearchRequest, RRFRanker, WeightedRanker
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
    Collection
)
from common.constants import *
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_base import TestcaseBase
import heapq
from time import sleep
from decimal import Decimal, getcontext
import decimal
import multiprocessing
import numbers
import random
import math
import numpy
import threading
import pytest
import pandas as pd
from faker import Faker

Faker.seed(19530)
fake_en = Faker("en_US")
fake_zh = Faker("zh_CN")

# patch faker to generate text with specific distribution
cf.patch_faker_text(fake_en, cf.en_vocabularies_distribution)
cf.patch_faker_text(fake_zh, cf.zh_vocabularies_distribution)

pd.set_option("expand_frame_repr", False)

prefix = "search_collection"
search_num = 10
max_dim = ct.max_dim
min_dim = ct.min_dim
epsilon = ct.epsilon
hybrid_search_epsilon = 0.01
gracefulTime = ct.gracefulTime
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
max_limit = ct.max_limit
default_search_exp = "int64 >= 0"
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_invaild_string_exp = "varchar >= 0"
default_json_search_exp = "json_field[\"number\"] >= 0"
perfix_expr = 'varchar like "0%"'
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_json_field_name = ct.default_json_field_name
default_index_params = ct.default_index
vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
uid = "test_search"
nq = 1
epsilon = 0.001
field_name = default_float_vec_field_name
binary_field_name = default_binary_vec_field_name
search_param = {"nprobe": 1}
entity = gen_entities(1, is_normal=True)
entities = gen_entities(default_nb, is_normal=True)
raw_vectors, binary_entities = gen_binary_entities(default_nb)
default_query, _ = gen_search_vectors_params(field_name, entities, default_top_k, nq)
index_name1 = cf.gen_unique_str("float")
index_name2 = cf.gen_unique_str("varhar")
half_nb = ct.default_nb // 2
max_hybrid_search_req_num = ct.max_hybrid_search_req_num


class TestCollectionHybridSearchValid(TestcaseBase):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[1, 10])
    def nq(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[default_nb_medium])
    def nb(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[32, 128])
    def dim(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def _async(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["JACCARD", "HAMMING"])
    def metrics(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def is_flush(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def enable_dynamic_field(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["IP", "COSINE", "L2"])
    def metric_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def random_primary_key(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=ct.all_dense_vector_types)
    def vector_data_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases for hybrid_search
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("offset", [0, 5])
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_normal(self, nq, is_flush, offset, primary_field, vector_data_type):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        self._connect()
        # create db
        db_name = cf.gen_unique_str(prefix)
        self.database_wrap.create_database(db_name)
        # using db and create collection
        self.database_wrap.using_database(db_name)

        # 1. initialize collection with data
        dim = 64
        enable_dynamic_field = True
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_flush=is_flush,
                                         primary_field=primary_field, enable_dynamic_field=enable_dynamic_field,
                                         multiple_dim_array=multiple_dim_array,
                                         vector_data_type=vector_data_type,
                                         nullable_fields={ct.default_float_field_name: 1})[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        metrics = []
        search_res_dict_array = []
        search_res_dict_array_nq = []
        vectors = cf.gen_vectors(nq, dim, vector_data_type)

        # get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            metrics.append("COSINE")

        # get the result of search with the same params of the following hybrid search
        single_search_param = {"metric_type": "COSINE", "params": {"nprobe": 32}, "offset": offset}
        for k in range(nq):
            for i in range(len(vector_name_list)):
                search_res_dict = {}
                search_res_dict_array = []
                vectors_search = vectors[k]
                # 5. search to get the baseline of hybrid_search
                search_res = collection_w.search([vectors_search], vector_name_list[i],
                                                 single_search_param, default_limit,
                                                 default_search_exp,
                                                 check_task=CheckTasks.check_search_results,
                                                 check_items={"nq": 1,
                                                              "ids": insert_ids,
                                                              "pk_name": ct.default_int64_field_name,
                                                              "limit": default_limit})[0]
                ids = search_res[0].ids
                distance_array = search_res[0].distances
                for j in range(len(ids)):
                    search_res_dict[ids[j]] = distance_array[j]
                search_res_dict_array.append(search_res_dict)
            search_res_dict_array_nq.append(search_res_dict_array)

        # 6. calculate hybrid search baseline
        score_answer_nq = []
        for k in range(nq):
            ids_answer, score_answer = cf.get_hybrid_search_base_results(search_res_dict_array_nq[k], weights, metrics)
            score_answer_nq.append(score_answer)
        # 7. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                                offset=offset,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": nq,
                                                             "ids": insert_ids,
                                                             "limit": default_limit,
                                                             "pk_name": ct.default_int64_field_name})[0]
        # 8. compare results through the re-calculated distances
        for k in range(len(score_answer_nq)):
            for i in range(len(score_answer_nq[k][:default_limit])):
                assert score_answer_nq[k][i] - hybrid_res[k].distances[i] < hybrid_search_epsilon

        # 9. drop db
        collection_w.drop()
        self.database_wrap.drop_database(db_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [16384])
    def test_hybrid_search_normal_max_nq(self, nq):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [1]
        vectors = cf.gen_vectors(nq, default_dim, vector_data_type=DataType.FLOAT_VECTOR)
        # 4. get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 5. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": nq,
                                                             "ids": insert_ids,
                                                             "limit": default_limit,
                                                             "pk_name": ct.default_int64_field_name})[0]

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_normal_expr(self):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with search param templates
        """
        # 1. initialize collection with data
        nq = 10
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [1]
        vectors = cf.gen_vectors(nq, default_dim, vector_data_type=DataType.FLOAT_VECTOR)
        # 4. get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > {value_0}",
                "expr_params": {"value_0": 0}
            }
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 5. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq, "ids": insert_ids, "limit": default_limit,
                                                "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue 32288")
    @pytest.mark.parametrize("nq", [0, 16385])
    def test_hybrid_search_normal_over_max_nq(self, nq):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [1]
        vectors = cf.gen_vectors(nq, default_dim, vector_data_type=DataType.FLOAT_VECTOR)
        # 4. get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 5. hybrid search
        err_msg = "nq (number of search vector per search request) should be in range [1, 16384]"
        collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                   check_task=CheckTasks.err_res,
                                   check_items={"err_code": 65535,
                                                "err_msg": err_msg})

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_no_limit(self):
        """
        target: test hybrid search with no limit
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        multiple_dim_array = [default_dim, default_dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        vectors = cf.gen_vectors(nq, default_dim, vector_data_type=DataType.FLOAT_VECTOR)

        # get hybrid search req list
        search_param = {
            "data": vectors,
            "anns_field": vector_name_list[0],
            "param": {"metric_type": "COSINE"},
            "limit": default_limit,
            "expr": "int64 > 0"}
        req = AnnSearchRequest(**search_param)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_WeightedRanker_empty_reqs(self, primary_field):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, primary_field=primary_field,
                                         multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. hybrid search with empty reqs
        collection_w.hybrid_search([], WeightedRanker(), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": 0})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue 29839")
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_as_search(self, nq, primary_field, is_flush):
        """
        target: test hybrid search to search as the original search interface
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK), and the result should be equal to search
        """
        # 1. initialize collection with data
        dim = 3
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_flush=is_flush,
                                         primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=[dim, dim])[0:5]

        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        for search_field in vector_name_list:
            # 2. prepare search params
            req_list = []
            search_param = {
                "data": vectors,
                "anns_field": search_field,
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            # 3. hybrid search
            hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(1), default_limit,
                                                    check_task=CheckTasks.check_search_results,
                                                    check_items={"nq": nq,
                                                                 "ids": insert_ids,
                                                                 "limit": default_limit,
                                                                 "pk_name": ct.default_int64_field_name})[0]
            search_res = collection_w.search(vectors[:nq], search_field,
                                             default_search_params, default_limit,
                                             default_search_exp,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": nq,
                                                          "ids": insert_ids,
                                                          "limit": default_limit,
                                                          "pk_name": ct.default_int64_field_name})[0]
            # 4. the effect of hybrid search to one field should equal to search
            log.info("The distance list is:\n")
            for i in range(nq):
                log.info(hybrid_res[0].distances)
                log.info(search_res[0].distances)
                assert hybrid_res[i].ids == search_res[i].ids

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_different_metric_type(self, nq, primary_field, is_flush, metric_type):
        """
        target: test hybrid search for fields with different metric type
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 128
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_flush=is_flush, is_index=False,
                                         primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=[dim, dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for vector_name in vector_name_list:
            search_param = {
                "data": [[random.random() for _ in range(dim)] for _ in range(nq)],
                "anns_field": vector_name,
                "param": {"metric_type": metric_type, "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9, 1), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq,
                                                "ids": insert_ids,
                                                "limit": default_limit,
                                                "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_different_metric_type_each_field(self, nq, primary_field, is_flush, metric_type):
        """
        target: test hybrid search for fields with different metric type
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 91
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_flush=is_flush, is_index=False,
                                         primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=[dim, dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "L2"}
        collection_w.create_index(vector_name_list[0], flat_index)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "IP"}
        collection_w.create_index(vector_name_list[1], flat_index)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "COSINE"}
        collection_w.create_index(vector_name_list[2], flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        search_param = {
            "data": [[random.random() for _ in range(dim)] for _ in range(nq)],
            "anns_field": vector_name_list[0],
            "param": {"metric_type": "L2", "offset": 0},
            "limit": default_limit,
            "expr": "int64 > 0"}
        req = AnnSearchRequest(**search_param)
        req_list.append(req)
        search_param = {
            "data": [[random.random() for _ in range(dim)] for _ in range(nq)],
            "anns_field": vector_name_list[1],
            "param": {"metric_type": "IP", "offset": 0},
            "limit": default_limit,
            "expr": "int64 > 0"}
        req = AnnSearchRequest(**search_param)
        req_list.append(req)
        search_param = {
            "data": [[random.random() for _ in range(dim)] for _ in range(nq)],
            "anns_field": vector_name_list[2],
            "param": {"metric_type": "COSINE", "offset": 0},
            "limit": default_limit,
            "expr": "int64 > 0"}
        req = AnnSearchRequest(**search_param)
        req_list.append(req)
        # 4. hybrid search
        hybrid_search = collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9, 1), default_limit,
                                                   check_task=CheckTasks.check_search_results,
                                                   check_items={"nq": nq,
                                                                "ids": insert_ids,
                                                                "limit": default_limit,
                                                                "pk_name": ct.default_int64_field_name})[0]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    @pytest.mark.skip(reason="issue 29923")
    def test_hybrid_search_different_dim(self, nq, primary_field, metric_type):
        """
        target: test hybrid search for fields with different dim
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        default_limit = 100
        # 1. initialize collection with data
        dim = 121
        multiple_dim_array = [dim + dim, dim - 10]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False, primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for i in range(len(vector_name_list)):
            search_param = {
                "data": [[random.random() for _ in range(multiple_dim_array[i])] for _ in range(nq)],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": metric_type, "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        hybrid_search_0 = collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9), default_limit,
                                                     check_task=CheckTasks.check_search_results,
                                                     check_items={"nq": nq,
                                                                  "ids": insert_ids,
                                                                  "limit": default_limit,
                                                                  "pk_name": ct.default_int64_field_name})[0]
        hybrid_search_1 = collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9), default_limit,
                                                     check_task=CheckTasks.check_search_results,
                                                     check_items={"nq": nq,
                                                                  "ids": insert_ids,
                                                                  "limit": default_limit,
                                                                  "pk_name": ct.default_int64_field_name})[0]
        for i in range(nq):
            assert hybrid_search_0[i].ids == hybrid_search_1[i].ids
            assert hybrid_search_0[i].distances == hybrid_search_1[i].distances

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/36273")
    def test_hybrid_search_overall_limit_larger_sum_each_limit(self, nq, primary_field, metric_type):

        """
        target: test hybrid search: overall limit which is larger than sum of each limit
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 200
        multiple_dim_array = [dim + dim, dim - 10]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False, primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        id_list_nq = []
        vectors = []
        default_search_params = {"metric_type": metric_type, "offset": 0}
        for i in range(len(vector_name_list)):
            vectors.append([])
        for i in range(nq):
            id_list_nq.append([])
        for k in range(nq):
            for i in range(len(vector_name_list)):
                vectors_search = [random.random() for _ in range(multiple_dim_array[i])]
                vectors[i].append(vectors_search)
        # 4. search for the comparision for hybrid search
        for i in range(len(vector_name_list)):
            search_res = collection_w.search(vectors[i], vector_name_list[i],
                                             default_search_params, default_limit,
                                             default_search_exp,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": nq,
                                                          "ids": insert_ids,
                                                          "limit": default_limit,
                                                          "pk_name": ct.default_int64_field_name})[0]
            for k in range(nq):
                id_list_nq[k].extend(search_res[k].ids)
        # 5. prepare hybrid search params
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors[i],
                "anns_field": vector_name_list[i],
                "param": default_search_params,
                "limit": default_limit,
                "expr": default_search_exp}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 6. hybrid search
        hybrid_search = \
            collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9), default_limit * len(req_list) + 1)[0]
        assert len(hybrid_search) == nq
        for i in range(nq):
            assert len(hybrid_search[i].ids) == len(list(set(id_list_nq[i])))
            assert set(hybrid_search[i].ids) == set(id_list_nq[i])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_overall_different_limit(self, primary_field, metric_type):
        """
        target: test hybrid search with different limit params
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 100
        multiple_dim_array = [dim + dim, dim - 10]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False, primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for i in range(len(vector_name_list)):
            search_param = {
                "data": [[random.random() for _ in range(multiple_dim_array[i])] for _ in range(nq)],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": metric_type, "offset": 0},
                "limit": default_limit - i,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq,
                                                "ids": insert_ids,
                                                "limit": default_limit,
                                                "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/36273")
    def test_hybrid_search_min_limit(self, primary_field, metric_type):
        """
        target: test hybrid search with minimum limit params
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 99
        multiple_dim_array = [dim + dim, dim - 10]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False, primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        id_list = []
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(multiple_dim_array[i])] for _ in range(1)]
            search_params = {"metric_type": metric_type, "offset": 0}
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": search_params,
                "limit": min_dim,
                "expr": default_search_exp}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            search_res = collection_w.search(vectors[:1], vector_name_list[i],
                                             search_params, min_dim,
                                             default_search_exp,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "ids": insert_ids,
                                                          "limit": min_dim,
                                                          "pk_name": ct.default_int64_field_name})[0]
            id_list.extend(search_res[0].ids)
        # 4. hybrid search
        hybrid_search = collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9), default_limit)[0]
        assert len(hybrid_search) == 1
        assert len(hybrid_search[0].ids) == len(list(set(id_list)))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_max_limit(self, primary_field, metric_type):
        """
        target: test hybrid search with maximum limit params
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 66
        multiple_dim_array = [dim + dim, dim - 10]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False, primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for i in range(len(vector_name_list)):
            search_param = {
                "data": [[random.random() for _ in range(multiple_dim_array[i])] for _ in range(nq)],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": metric_type},
                "limit": max_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq,
                                                "ids": insert_ids,
                                                "limit": default_limit,
                                                "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_max_min_limit(self, primary_field, metric_type):
        """
        target: test hybrid search with maximum and minimum limit params
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 66
        multiple_dim_array = [dim + dim, dim - 10]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False, primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for i in range(len(vector_name_list)):
            limit = max_limit
            if i == 1:
                limit = 1
            search_param = {
                "data": [[random.random() for _ in range(multiple_dim_array[i])] for _ in range(nq)],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": metric_type},
                "limit": limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq,
                                                "ids": insert_ids,
                                                "limit": default_limit,
                                                "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_same_anns_field(self, primary_field, metric_type):
        """
        target: test hybrid search: multiple search on same anns field
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 55
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False, primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for i in range(len(vector_name_list)):
            search_param = {
                "data": [[random.random() for _ in range(multiple_dim_array[i])] for _ in range(nq)],
                "anns_field": vector_name_list[0],
                "param": {"metric_type": metric_type, "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq,
                                                "ids": insert_ids,
                                                "limit": default_limit,
                                                "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_different_offset_single_field(self, primary_field, is_flush, metric_type):
        """
        target: test hybrid search for fields with different offset
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 100
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=False, dim=dim, is_flush=is_flush, is_index=False,
                                         primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=[dim, dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for i in range(len(vector_name_list)):
            search_param = {
                "data": [[random.random() for _ in range(dim)] for _ in range(nq)],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": metric_type, "offset": i},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(0.1, 0.9, 1), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": nq,
                                                "ids": insert_ids,
                                                "limit": default_limit,
                                                "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_max_reqs_num(self, primary_field):
        """
        target: test hybrid search with maximum reqs number
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 128
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=dim, is_index=False, primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "COSINE"}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        reqs_max_num = max_hybrid_search_req_num
        # 3. prepare search params
        req_list = []
        for i in range(reqs_max_num):
            search_param = {
                "data": [[random.random() for _ in range(dim)] for _ in range(1)],
                "anns_field": default_search_field,
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        weights = [random.random() for _ in range(len(req_list))]
        log.info(weights)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": 1,
                                                "ids": insert_ids,
                                                "limit": default_limit,
                                                "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_WeightedRanker_different_parameters(self, primary_field, is_flush, metric_type):
        """
        target: test hybrid search for fields with different offset
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 63
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=True, dim=dim, is_flush=is_flush, is_index=False,
                                         primary_field=primary_field,
                                         enable_dynamic_field=False, multiple_dim_array=[dim, dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": metric_type}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.load()
        # 3. prepare search params
        req_list = []
        for i in range(len(vector_name_list)):
            search_param = {
                "data": [[random.random() for _ in range(dim)] for _ in range(1)],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": metric_type, "offset": i},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(0.2, 0.03, 0.9), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": 1,
                                                "ids": insert_ids,
                                                "limit": default_limit,
                                                "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("issue: #29840")
    def test_hybrid_search_invalid_WeightedRanker_params(self):
        """
        target: test hybrid search with invalid params type to WeightedRanker
        method: create connection, collection, insert and search
        expected: raise exception
        """
        # 1. initialize collection with data
        multiple_dim_array = [default_dim, default_dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=default_dim, is_index=False,
                                         multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "COSINE"}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        reqs_num = 2
        # 3. prepare search params
        req_list = []
        for i in range(reqs_num):
            search_param = {
                "data": [[random.random() for _ in range(default_dim)] for _ in range(1)],
                "anns_field": default_search_field,
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search with list in WeightedRanker
        collection_w.hybrid_search(req_list, WeightedRanker([0.9, 0.1]), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": 1,
                                                "ids": insert_ids,
                                                "limit": default_limit,
                                                "pk_name": ct.default_int64_field_name})
        # 5. hybrid search with two-dim list in WeightedRanker
        weights = [[random.random() for _ in range(1)] for _ in range(len(req_list))]
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": 1,
                                                "ids": insert_ids,
                                                "limit": default_limit,
                                                "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_search_over_maximum_reqs_num(self):
        """
        target: test hybrid search over maximum reqs number
        method: create connection, collection, insert and search
        expected: raise exception
        """
        # 1. initialize collection with data
        multiple_dim_array = [default_dim, default_dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=default_dim, is_index=False,
                                         multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "COSINE"}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        reqs_max_num = max_hybrid_search_req_num + 1
        # 3. prepare search params
        req_list = []
        for i in range(reqs_max_num):
            search_param = {
                "data": [[random.random() for _ in range(default_dim)] for _ in range(1)],
                "anns_field": default_search_field,
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        weights = [random.random() for _ in range(len(req_list))]
        log.info(weights)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                   check_task=CheckTasks.err_res,
                                   check_items={"err_code": 65535,
                                                "err_msg": 'maximum of ann search requests is 1024'})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_with_range_search(self, primary_field):
        """
        target: test hybrid search with range search
        method: create connection, collection, insert and search
        expected: raise exception (not support yet)
        """
        # 1. initialize collection with data
        multiple_dim_array = [default_dim, default_dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=default_dim, is_index=False,
                                         primary_field=primary_field,
                                         multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "COSINE"}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, flat_index)
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        collection_w.load()
        reqs_max_num = 2
        # 3. prepare search params
        req_list = []
        for i in range(reqs_max_num):
            search_param = {
                "data": [[random.random() for _ in range(default_dim)] for _ in range(1)],
                "anns_field": default_search_field,
                "param": {"metric_type": "COSINE", "params": {"radius": 0, "range_filter": 1000}},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        weights = [random.random() for _ in range(len(req_list))]
        log.info(weights)
        # 4. hybrid search
        collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": 1,
                                                "ids": insert_ids,
                                                "limit": default_limit,
                                                "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_RRFRanker_default_parameter(self, primary_field):
        """
        target: test hybrid search with default value to RRFRanker
        method: create connection, collection, insert and search.
                Note: here the result check is through comparing the score, the ids could not be compared
                because the high probability of the same score, then the id is not fixed in the range of
                the same score
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=default_dim, primary_field=primary_field,
                                         multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params for each vector field
        req_list = []
        search_res_dict_array = []
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(1)]
            search_res_dict = {}
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE", "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            # search for get the base line of hybrid_search
            search_res = collection_w.search(vectors[:1], vector_name_list[i],
                                             default_search_params, default_limit,
                                             default_search_exp,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "ids": insert_ids,
                                                          "limit": default_limit,
                                                          "pk_name": ct.default_int64_field_name})[0]
            ids = search_res[0].ids
            for j in range(len(ids)):
                search_res_dict[ids[j]] = 1 / (j + 60 + 1)
            search_res_dict_array.append(search_res_dict)
        # 4. calculate hybrid search base line for RRFRanker
        ids_answer, score_answer = cf.get_hybrid_search_base_results_rrf(search_res_dict_array)
        # 5. hybrid search
        hybrid_search_0 = collection_w.hybrid_search(req_list, RRFRanker(), default_limit,
                                                     check_task=CheckTasks.check_search_results,
                                                     check_items={"nq": 1,
                                                                  "ids": insert_ids,
                                                                  "limit": default_limit,
                                                                  "pk_name": ct.default_int64_field_name})[0]
        # 6. compare results through the re-calculated distances
        for i in range(len(score_answer[:default_limit])):
            assert score_answer[i] - hybrid_search_0[0].distances[i] < hybrid_search_epsilon
        # 7. run hybrid search with the same parameters twice, and compare the results
        hybrid_search_1 = collection_w.hybrid_search(req_list, RRFRanker(), default_limit,
                                                     check_task=CheckTasks.check_search_results,
                                                     check_items={"nq": 1,
                                                                  "ids": insert_ids,
                                                                  "limit": default_limit,
                                                                  "pk_name": ct.default_int64_field_name})[0]

        assert hybrid_search_0[0].ids == hybrid_search_1[0].ids
        assert hybrid_search_0[0].distances == hybrid_search_1[0].distances

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("k", [1, 60, 1000, 16383])
    @pytest.mark.parametrize("offset", [0, 1, 5])
    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/32650")
    def test_hybrid_search_RRFRanker_different_k(self, is_flush, k, offset):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search.
                Note: here the result check is through comparing the score, the ids could not be compared
                because the high probability of the same score, then the id is not fixed in the range of
                the same score
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 200
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=False, dim=dim, is_flush=is_flush,
                                         enable_dynamic_field=False, multiple_dim_array=[dim, dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params for each vector field
        req_list = []
        search_res_dict_array = []
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(dim)] for _ in range(1)]
            search_res_dict = {}
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            # search for get the baseline of hybrid_search
            search_res = collection_w.search(vectors[:1], vector_name_list[i],
                                             default_search_params, default_limit,
                                             default_search_exp, offset=0,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "ids": insert_ids,
                                                          "limit": default_limit,
                                                          "pk_name": ct.default_int64_field_name})[0]
            ids = search_res[0].ids
            for j in range(len(ids)):
                search_res_dict[ids[j]] = 1 / (j + k + 1)
            search_res_dict_array.append(search_res_dict)
        # 4. calculate hybrid search baseline for RRFRanker
        ids_answer, score_answer = cf.get_hybrid_search_base_results_rrf(search_res_dict_array)
        # 5. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, RRFRanker(k), default_limit,
                                                offset=offset,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": 1,
                                                             "ids": insert_ids,
                                                             "limit": default_limit,
                                                             "pk_name": ct.default_int64_field_name})[0]
        # 6. compare results through the re-calculated distances
        for i in range(len(score_answer[:default_limit])):
            assert score_answer[i] - hybrid_res[0].distances[i] < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [0, 1, 5])
    @pytest.mark.parametrize("rerank", [RRFRanker(), WeightedRanker(0.1, 0.9, 1)])
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_offset_inside_outside_params(self, primary_field, offset, rerank):
        """
        target: test hybrid search with offset inside and outside params
        method: create connection, collection, insert and search.
                Note: here the result check is through comparing the score, the ids could not be compared
                because the high probability of the same score, then the id is not fixed in the range of
                the same score
        expected: hybrid search successfully with limit(topK), and the result should be the same
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, primary_field=primary_field,
                                         multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        req_list = []
        vectors_list = []
        # 3. generate vectors
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(1)]
            vectors_list.append(vectors)
        # 4. prepare search params for each vector field
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors_list[i],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE", "offset": offset},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search with offset inside the params
        hybrid_res_inside = collection_w.hybrid_search(req_list, rerank, default_limit,
                                                       check_task=CheckTasks.check_search_results,
                                                       check_items={"nq": 1,
                                                                    "ids": insert_ids,
                                                                    "limit": default_limit,
                                                                    "pk_name": ct.default_int64_field_name})[0]
        # 5. hybrid search with offset parameter
        req_list = []
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors_list[i],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        hybrid_res = collection_w.hybrid_search(req_list, rerank, default_limit - offset,
                                                offset=offset,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": 1,
                                                             "ids": insert_ids,
                                                             "limit": default_limit - offset,
                                                             "pk_name": ct.default_int64_field_name})[0]

        assert hybrid_res_inside[0].distances[offset:] == hybrid_res[0].distances

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_search_RRFRanker_empty_reqs(self):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. hybrid search with empty reqs
        collection_w.hybrid_search([], RRFRanker(), default_limit,
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": 0})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("k", [0, 16385])
    @pytest.mark.skip(reason="issue #29867")
    def test_hybrid_search_RRFRanker_k_out_of_range(self, k):
        """
        target: test hybrid search with default value to RRFRanker
        method: create connection, collection, insert and search.
                Note: here the result check is through comparing the score, the ids could not be compared
                because the high probability of the same score, then the id is not fixed in the range of
                the same score
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=default_dim,
                                         multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params for each vector field
        req_list = []
        search_res_dict_array = []
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(1)]
            search_res_dict = {}
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE", "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            # search for get the base line of hybrid_search
            search_res = collection_w.search(vectors[:1], vector_name_list[i],
                                             default_search_params, default_limit,
                                             default_search_exp,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "ids": insert_ids,
                                                          "limit": default_limit,
                                                          "pk_name": ct.default_int64_field_name})[0]
            ids = search_res[0].ids
            for j in range(len(ids)):
                search_res_dict[ids[j]] = 1 / (j + k + 1)
            search_res_dict_array.append(search_res_dict)
        # 4. calculate hybrid search base line for RRFRanker
        ids_answer, score_answer = cf.get_hybrid_search_base_results_rrf(search_res_dict_array)
        # 5. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, RRFRanker(k), default_limit,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": 1,
                                                             "ids": insert_ids,
                                                             "limit": default_limit,
                                                             "pk_name": ct.default_int64_field_name})[0]
        # 6. compare results through the re-calculated distances
        for i in range(len(score_answer[:default_limit])):
            delta = math.fabs(score_answer[i] - hybrid_res[0].distances[i])
            assert delta < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [1, 100, 16384])
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_different_limit_round_decimal(self, primary_field, limit):
        """
        target: test hybrid search with different valid limit and round decimal
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, primary_field=primary_field,
                                         multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        search_res_dict_array = []
        if limit > default_nb:
            limit = default_limit
        metrics = []
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(1)]
            search_res_dict = {}
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE", "offset": 0},
                "limit": limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            metrics.append("COSINE")
            # search to get the base line of hybrid_search
            search_res = collection_w.search(vectors[:1], vector_name_list[i],
                                             default_search_params, limit,
                                             default_search_exp, round_decimal=5,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "ids": insert_ids,
                                                          "limit": limit,
                                                          "pk_name": ct.default_int64_field_name})[0]
            ids = search_res[0].ids
            distance_array = search_res[0].distances
            for j in range(len(ids)):
                search_res_dict[ids[j]] = distance_array[j]
            search_res_dict_array.append(search_res_dict)
        # 4. calculate hybrid search base line
        ids_answer, score_answer = cf.get_hybrid_search_base_results(search_res_dict_array, weights, metrics, 5)
        # 5. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), limit,
                                                round_decimal=5,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": 1,
                                                             "ids": insert_ids,
                                                             "limit": limit,
                                                             "pk_name": ct.default_int64_field_name})[0]
        # 6. compare results through the re-calculated distances
        for i in range(len(score_answer[:limit])):
            delta = math.fabs(score_answer[i] - hybrid_res[0].distances[i])
            if delta >= hybrid_search_epsilon:
                # print id and distance for debug
                # answer and hybrid search result
                for i1 in range(len(score_answer)):
                    log.info("answer id: %d, distance: %f" % (ids_answer[i1], score_answer[i1]))
                for i2 in range(len(hybrid_res[0].ids)):
                    log.info("hybrid search res id: %d, distance: %f" % (hybrid_res[0].ids[i2], hybrid_res[0].distances[i2]))
            assert delta < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_limit_out_of_range_max(self):
        """
        target: test hybrid search with over maximum limit
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(1)]
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE", "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search with over maximum limit
        limit = 16385
        error = {ct.err_code: 65535, ct.err_msg: "invalid max query result window, (offset+limit) "
                                                 "should be in range [1, 16384], but got %d" % limit}
        collection_w.hybrid_search(req_list, WeightedRanker(*weights), limit,
                                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_limit_out_of_range_min(self):
        """
        target: test hybrid search with over minimum limit
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(1)]
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE", "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search with over maximum limit
        limit = 0
        error = {ct.err_code: 1, ct.err_msg: "`limit` value 0 is illegal"}
        collection_w.hybrid_search(req_list, WeightedRanker(*weights), limit,
                                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_with_output_fields(self, nq, dim, auto_id, is_flush, enable_dynamic_field,
                                              primary_field, vector_data_type):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        nq = 10
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_flush=is_flush,
                                         primary_field=primary_field,
                                         enable_dynamic_field=enable_dynamic_field,
                                         multiple_dim_array=multiple_dim_array,
                                         vector_data_type=vector_data_type)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        metrics = []
        search_res_dict_array = []
        search_res_dict_array_nq = []
        vectors = cf.gen_vectors(nq, dim, vector_data_type)

        # get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            metrics.append("COSINE")

        # get the result of search with the same params of the following hybrid search
        single_search_param = {"metric_type": "COSINE", "params": {"nprobe": 10}}
        for k in range(nq):
            for i in range(len(vector_name_list)):
                search_res_dict = {}
                search_res_dict_array = []
                vectors_search = vectors[k]
                # 5. search to get the base line of hybrid_search
                search_res = collection_w.search([vectors_search], vector_name_list[i],
                                                 single_search_param, default_limit,
                                                 default_search_exp,
                                                 check_task=CheckTasks.check_search_results,
                                                 check_items={"nq": 1,
                                                              "ids": insert_ids,
                                                              "limit": default_limit,
                                                              "pk_name": ct.default_int64_field_name})[0]
                ids = search_res[0].ids
                distance_array = search_res[0].distances
                for j in range(len(ids)):
                    search_res_dict[ids[j]] = distance_array[j]
                search_res_dict_array.append(search_res_dict)
            search_res_dict_array_nq.append(search_res_dict_array)

        # 6. calculate hybrid search base line
        score_answer_nq = []
        for k in range(nq):
            ids_answer, score_answer = cf.get_hybrid_search_base_results(search_res_dict_array_nq[k], weights, metrics)
            score_answer_nq.append(score_answer)
        # 7. hybrid search
        output_fields = [default_int64_field_name]
        hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                                output_fields=output_fields,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": nq,
                                                             "ids": insert_ids,
                                                             "limit": default_limit,
                                                             "pk_name": ct.default_int64_field_name})[0]
        # 8. compare results through the re-calculated distances
        for k in range(len(score_answer_nq)):
            for i in range(len(score_answer_nq[k][:default_limit])):
                assert score_answer_nq[k][i] - hybrid_res[k].distances[i] < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_with_output_fields_all_fields(self, nq, dim, auto_id, is_flush, enable_dynamic_field,
                                                         primary_field, vector_data_type):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        nq = 10
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_flush=is_flush,
                                         primary_field=primary_field,
                                         enable_dynamic_field=enable_dynamic_field,
                                         multiple_dim_array=multiple_dim_array,
                                         vector_data_type=vector_data_type)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        metrics = []
        search_res_dict_array = []
        search_res_dict_array_nq = []
        vectors = cf.gen_vectors(nq, dim, vector_data_type)

        # get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            metrics.append("COSINE")

        # get the result of search with the same params of the following hybrid search
        single_search_param = {"metric_type": "COSINE", "params": {"nprobe": 10}}
        for k in range(nq):
            for i in range(len(vector_name_list)):
                search_res_dict = {}
                search_res_dict_array = []
                vectors_search = vectors[k]
                # 5. search to get the base line of hybrid_search
                search_res = collection_w.search([vectors_search], vector_name_list[i],
                                                 single_search_param, default_limit,
                                                 default_search_exp,
                                                 check_task=CheckTasks.check_search_results,
                                                 check_items={"nq": 1,
                                                              "ids": insert_ids,
                                                              "limit": default_limit,
                                                              "pk_name": ct.default_int64_field_name})[0]
                ids = search_res[0].ids
                distance_array = search_res[0].distances
                for j in range(len(ids)):
                    search_res_dict[ids[j]] = distance_array[j]
                search_res_dict_array.append(search_res_dict)
            search_res_dict_array_nq.append(search_res_dict_array)

        # 6. calculate hybrid search base line
        score_answer_nq = []
        for k in range(nq):
            ids_answer, score_answer = cf.get_hybrid_search_base_results(search_res_dict_array_nq[k], weights, metrics)
            score_answer_nq.append(score_answer)
        # 7. hybrid search
        output_fields = [default_int64_field_name, default_float_field_name, default_string_field_name,
                         default_json_field_name]
        output_fields = output_fields + vector_name_list
        hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                                output_fields=output_fields,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": nq,
                                                             "ids": insert_ids,
                                                             "limit": default_limit,
                                                             "pk_name": ct.default_int64_field_name})[0]
        # 8. compare results through the re-calculated distances
        for k in range(len(score_answer_nq)):
            for i in range(len(score_answer_nq[k][:default_limit])):
                assert score_answer_nq[k][i] - hybrid_res[k].distances[i] < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_with_output_fields_all_fields(self, nq, dim, auto_id, is_flush, enable_dynamic_field,
                                                         primary_field, vector_data_type):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        nq = 10
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_flush=is_flush,
                                         primary_field=primary_field,
                                         enable_dynamic_field=enable_dynamic_field,
                                         multiple_dim_array=multiple_dim_array,
                                         vector_data_type=vector_data_type)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        metrics = []
        search_res_dict_array = []
        search_res_dict_array_nq = []
        vectors = cf.gen_vectors(nq, dim, vector_data_type)

        # get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            metrics.append("COSINE")

        # get the result of search with the same params of the following hybrid search
        single_search_param = {"metric_type": "COSINE", "params": {"nprobe": 10}}
        for k in range(nq):
            for i in range(len(vector_name_list)):
                search_res_dict = {}
                search_res_dict_array = []
                vectors_search = vectors[k]
                # 5. search to get the base line of hybrid_search
                search_res = collection_w.search([vectors_search], vector_name_list[i],
                                                 single_search_param, default_limit,
                                                 default_search_exp,
                                                 check_task=CheckTasks.check_search_results,
                                                 check_items={"nq": 1,
                                                              "ids": insert_ids,
                                                              "limit": default_limit,
                                                              "pk_name": ct.default_int64_field_name})[0]
                ids = search_res[0].ids
                distance_array = search_res[0].distances
                for j in range(len(ids)):
                    search_res_dict[ids[j]] = distance_array[j]
                search_res_dict_array.append(search_res_dict)
            search_res_dict_array_nq.append(search_res_dict_array)

        # 6. calculate hybrid search base line
        score_answer_nq = []
        for k in range(nq):
            ids_answer, score_answer = cf.get_hybrid_search_base_results(search_res_dict_array_nq[k], weights, metrics)
            score_answer_nq.append(score_answer)
        # 7. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                                output_fields=["*"],
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": nq,
                                                             "ids": insert_ids,
                                                             "limit": default_limit,
                                                             "pk_name": ct.default_int64_field_name})[0]
        # 8. compare results through the re-calculated distances
        for k in range(len(score_answer_nq)):
            for i in range(len(score_answer_nq[k][:default_limit])):
                assert score_answer_nq[k][i] - hybrid_res[k].distances[i] < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("output_fields",
                             [[default_search_field], [default_search_field, default_int64_field_name]])
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_with_output_fields_sync_async(self, nq, primary_field, output_fields, _async):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        multiple_dim_array = [default_dim, default_dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, dim=default_dim,
                                         primary_field=primary_field,
                                         multiple_dim_array=multiple_dim_array)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        metrics = []
        search_res_dict_array = []
        search_res_dict_array_nq = []
        vectors = cf.gen_vectors(nq, default_dim, vector_data_type=DataType.FLOAT_VECTOR)

        # get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            metrics.append("COSINE")

        # get the result of search with the same params of the following hybrid search
        single_search_param = {"metric_type": "COSINE", "params": {"nprobe": 10}}
        for k in range(nq):
            for i in range(len(vector_name_list)):
                search_res_dict = {}
                search_res_dict_array = []
                vectors_search = vectors[k]
                # 5. search to get the base line of hybrid_search
                search_res = collection_w.search([vectors_search], vector_name_list[i],
                                                 single_search_param, default_limit,
                                                 default_search_exp, _async=_async,
                                                 check_task=CheckTasks.check_search_results,
                                                 check_items={"nq": 1,
                                                              "ids": insert_ids,
                                                              "limit": default_limit,
                                                              "pk_name": ct.default_int64_field_name,
                                                              "_async": _async})[0]
                if _async:
                    search_res.done()
                    search_res = search_res.result()
                ids = search_res[0].ids
                distance_array = search_res[0].distances
                for j in range(len(ids)):
                    search_res_dict[ids[j]] = distance_array[j]
                search_res_dict_array.append(search_res_dict)
            search_res_dict_array_nq.append(search_res_dict_array)

        # 6. calculate hybrid search base line
        score_answer_nq = []
        for k in range(nq):
            ids_answer, score_answer = cf.get_hybrid_search_base_results(search_res_dict_array_nq[k], weights, metrics)
            score_answer_nq.append(score_answer)
        # 7. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                                output_fields=output_fields, _async=_async,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": nq,
                                                             "ids": insert_ids,
                                                             "limit": default_limit,
                                                             "_async": _async,
                                                             "pk_name": ct.default_int64_field_name})[0]
        if _async:
            hybrid_res.done()
            hybrid_res = hybrid_res.result()
        # 8. compare results through the re-calculated distances
        for k in range(len(score_answer_nq)):
            for i in range(len(score_answer_nq[k][:default_limit])):
                assert score_answer_nq[k][i] - hybrid_res[k].distances[i] < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("rerank", [RRFRanker(), WeightedRanker(0.1, 0.9, 1)])
    def test_hybrid_search_offset_both_inside_outside_params(self, rerank):
        """
        target: test hybrid search with offset inside and outside params
        method: create connection, collection, insert and search.
                Note: here the result check is through comparing the score, the ids could not be compared
                because the high probability of the same score, then the id is not fixed in the range of
                the same score
        expected: Raise exception
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        req_list = []
        vectors_list = []
        # 3. generate vectors
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(1)]
            vectors_list.append(vectors)
        # 4. prepare search params for each vector field
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors_list[i],
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE", "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search with offset inside the params
        error = {ct.err_code: 1, ct.err_msg: "Provide offset both in kwargs and param, expect just one"}
        collection_w.hybrid_search(req_list, rerank, default_limit, offset=2,
                                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [1, 100, 16384])
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name, ct.default_string_field_name])
    def test_hybrid_search_is_partition_key(self, nq, primary_field, limit, vector_data_type):
        """
        target: test hybrid search with different valid limit and round decimal
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, primary_field=primary_field,
                                         multiple_dim_array=[default_dim, default_dim],
                                         vector_data_type=vector_data_type,
                                         is_partition_key=ct.default_float_field_name)[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        metrics = []
        search_res_dict_array = []
        search_res_dict_array_nq = []
        vectors = cf.gen_vectors(nq, default_dim, vector_data_type)

        # get hybrid search req list
        for i in range(len(vector_name_list)):
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE"},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            metrics.append("COSINE")

        # get the result of search with the same params of the following hybrid search
        single_search_param = {"metric_type": "COSINE", "params": {"nprobe": 10}}
        for k in range(nq):
            for i in range(len(vector_name_list)):
                search_res_dict = {}
                search_res_dict_array = []
                vectors_search = vectors[k]
                # 5. search to get the base line of hybrid_search
                search_res = collection_w.search([vectors_search], vector_name_list[i],
                                                 single_search_param, default_limit,
                                                 default_search_exp,
                                                 check_task=CheckTasks.check_search_results,
                                                 check_items={"nq": 1,
                                                              "ids": insert_ids,
                                                              "limit": default_limit,
                                                              "pk_name": ct.default_int64_field_name})[0]
                ids = search_res[0].ids
                distance_array = search_res[0].distances
                for j in range(len(ids)):
                    search_res_dict[ids[j]] = distance_array[j]
                search_res_dict_array.append(search_res_dict)
            search_res_dict_array_nq.append(search_res_dict_array)

        # 6. calculate hybrid search base line
        score_answer_nq = []
        for k in range(nq):
            ids_answer, score_answer = cf.get_hybrid_search_base_results(search_res_dict_array_nq[k], weights, metrics)
            score_answer_nq.append(score_answer)
        # 7. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), default_limit,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": nq,
                                                             "ids": insert_ids,
                                                             "limit": default_limit,
                                                             "pk_name": ct.default_int64_field_name})[0]
        # 8. compare results through the re-calculated distances
        for k in range(len(score_answer_nq)):
            for i in range(len(score_answer_nq[k][:default_limit])):
                assert score_answer_nq[k][i] - hybrid_res[k].distances[i] < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_result_L2_order(self, nq):
        """
        target: test hybrid search result having correct order for L2 distance
        method: create connection, collection, insert and search
        expected: hybrid search successfully and result order is correct
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, is_index=False,
                                         multiple_dim_array=[default_dim, default_dim])[0:5]

        # 2. create index
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        for i in range(len(vector_name_list)):
            default_index = {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}, }
            collection_w.create_index(vector_name_list[i], default_index)
        collection_w.load()

        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(nq)]
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "L2", "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), 10)[0]
        is_sorted_descend = lambda lst: all(lst[i] >= lst[i + 1] for i in range(len(lst) - 1))
        for i in range(nq):
            assert is_sorted_descend(res[i].distances)

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_result_order(self, nq):
        """
        target: test hybrid search result having correct order for cosine distance
        method: create connection, collection, insert and search
        expected: hybrid search successfully and result order is correct
        """
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        # 3. prepare search params
        req_list = []
        weights = [0.2, 0.3, 0.5]
        for i in range(len(vector_name_list)):
            vectors = [[random.random() for _ in range(default_dim)] for _ in range(nq)]
            search_param = {
                "data": vectors,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "COSINE", "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        # 4. hybrid search
        res = collection_w.hybrid_search(req_list, WeightedRanker(*weights), 10)[0]
        is_sorted_descend = lambda lst: all(lst[i] >= lst[i + 1] for i in range(len(lst) - 1))
        for i in range(nq):
            assert is_sorted_descend(res[i].distances)

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_search_sparse_normal(self):
        """
        target: test hybrid search after loading sparse vectors
        method: Test hybrid search after loading sparse vectors
        expected: hybrid search successfully with limit(topK)
        """
        nb, auto_id, dim, enable_dynamic_field = 20000, False, 768, False
        # 1. init collection
        collection_w, insert_vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, nb=nb, multiple_dim_array=[dim, dim * 2],
                                         with_json=False, vector_data_type=DataType.SPARSE_FLOAT_VECTOR)[0:4]
        # 2. extract vector field name
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        # 3. prepare search params
        req_list = []
        search_res_dict_array = []
        k = 60

        for i in range(len(vector_name_list)):
            # vector = cf.gen_sparse_vectors(1, dim)
            vector = insert_vectors[0][i + 3][-1:]
            search_res_dict = {}
            search_param = {
                "data": vector,
                "anns_field": vector_name_list[i],
                "param": {"metric_type": "IP", "offset": 0},
                "limit": default_limit,
                "expr": "int64 > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            # search for get the base line of hybrid_search
            search_res = collection_w.search(vector, vector_name_list[i],
                                             default_search_params, default_limit,
                                             default_search_exp,
                                             )[0]
            ids = search_res[0].ids
            for j in range(len(ids)):
                search_res_dict[ids[j]] = 1 / (j + k + 1)
            search_res_dict_array.append(search_res_dict)
        # 4. calculate hybrid search base line for RRFRanker
        ids_answer, score_answer = cf.get_hybrid_search_base_results_rrf(search_res_dict_array)
        # 5. hybrid search
        hybrid_res = collection_w.hybrid_search(req_list, RRFRanker(k), default_limit,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": 1,
                                                             "ids": insert_ids,
                                                             "limit": default_limit,
                                                             "pk_name": ct.default_int64_field_name})[0]
        # 6. compare results through the re-calculated distances
        for i in range(len(score_answer[:default_limit])):
            delta = math.fabs(score_answer[i] - hybrid_res[0].distances[i])
            assert delta < hybrid_search_epsilon
