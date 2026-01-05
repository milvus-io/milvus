import numpy as np
from pymilvus.orm.types import CONSISTENCY_STRONG, CONSISTENCY_BOUNDED, CONSISTENCY_SESSION, CONSISTENCY_EVENTUALLY
from pymilvus import AnnSearchRequest, RRFRanker, WeightedRanker
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
    Collection, Function, FunctionType, FunctionScore
)

from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_base import TestcaseBase
from base.client_v2_base import TestMilvusClientV2Base

import random
import math
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
epsilon = 0.001
hybrid_search_epsilon = 0.01

# test parameters for test client v2 base class
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_limit = 100


@pytest.mark.xdist_group("TestMilvusClientHybridSearch")
class TestMilvusClientHybridSearch(TestMilvusClientV2Base):
    """Test search with hybrid search functionality"""

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestMilvusClientHybridSearch" + cf.gen_unique_str("_")
        self.partition_names = ["partition_1", "partition_2"]
        self.primary_key_field_name = "id"
        self.float_vector_field_name1 = "float_vector1"
        self.float_vector_field_name2 = "float_vector2"
        self.sparse_vector_field_name1 = "sparse_vector1"
        self.sparse_vector_field_name2 = "sparse_vector2"
        self.dynamic_field_name1 = "dynamic_1"
        self.dynamic_field_name2 = "dynamic_2"
        self.text_field_name1 = "text1"
        self.text_field_name2 = "text2"
        self.json_field_name = "json"
        self.string_field_name = "string"
        self.int64_field_name = "int64"
        self.all_fields = [
            self.primary_key_field_name,
            self.float_vector_field_name1,
            self.float_vector_field_name2,
            self.sparse_vector_field_name1,
            self.sparse_vector_field_name2,
            self.dynamic_field_name1,
            self.dynamic_field_name2,
            self.text_field_name1,
            self.text_field_name2,
            self.json_field_name,
            self.string_field_name,
            self.int64_field_name
        ]

        self.float_vector_dim = 128
        self.primary_keys = []
        self.enable_dynamic_field = True
        self.datas = []

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        """
        Initialize collection before test class runs
        """
        # Get client connection
        client = self._client()
        analyzer_params = {
            "tokenizer": "standard",
        }

        # Create collection
        collection_schema = self.create_schema(client, enable_dynamic_field=self.enable_dynamic_field)[0]
        collection_schema.add_field(self.primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        collection_schema.add_field(self.float_vector_field_name1, DataType.FLOAT_VECTOR, dim=self.float_vector_dim)
        collection_schema.add_field(self.float_vector_field_name2, DataType.FLOAT_VECTOR, dim=self.float_vector_dim)
        collection_schema.add_field(self.sparse_vector_field_name1, DataType.SPARSE_FLOAT_VECTOR)
        collection_schema.add_field(self.sparse_vector_field_name2, DataType.SPARSE_FLOAT_VECTOR)
        collection_schema.add_field(self.text_field_name1, DataType.VARCHAR, max_length=6553,
                                    enable_analyzer=True, analyzer_params=analyzer_params)
        collection_schema.add_field(self.text_field_name2, DataType.VARCHAR, max_length=6553,
                                    enable_analyzer=True, analyzer_params=analyzer_params)
        collection_schema.add_field(self.int64_field_name, DataType.INT64)
        collection_schema.add_field(self.json_field_name, DataType.JSON)
        collection_schema.add_field(self.string_field_name, DataType.VARCHAR, max_length=256)
        bm25_function1 = Function(
            name=self.sparse_vector_field_name1,
            function_type=FunctionType.BM25,
            input_field_names=[self.text_field_name1],
            output_field_names=self.sparse_vector_field_name1,
            params={},
        )
        bm25_function2 = Function(
            name=self.sparse_vector_field_name2,
            function_type=FunctionType.BM25,
            input_field_names=[self.text_field_name2],
            output_field_names=self.sparse_vector_field_name2,
            params={},
        )
        collection_schema.add_function(bm25_function1)
        collection_schema.add_function(bm25_function2)
        self.create_collection(client, self.collection_name, schema=collection_schema,
                               force_teardown=False)
        for partition_name in self.partition_names:
            self.create_partition(client, self.collection_name, partition_name=partition_name)

        # Define number of insert iterations
        insert_times = 2

        # Generate vectors for each type and store in self
        float_vectors = cf.gen_vectors(default_nb * insert_times, dim=self.float_vector_dim,
                                       vector_data_type=DataType.FLOAT_VECTOR)
        float_vectors2 = cf.gen_vectors(default_nb * insert_times, dim=self.float_vector_dim,
                                        vector_data_type=DataType.FLOAT_VECTOR)
        texts1 = cf.gen_varchar_data(length=10, nb=default_nb * insert_times, text_mode=True)
        texts2 = cf.gen_varchar_data(length=10, nb=default_nb * insert_times, text_mode=True)

        # Insert data multiple times with non-duplicated primary keys
        for j in range(insert_times):
            # Group rows by partition based on primary key mod 3
            default_rows = []
            partition1_rows = []
            partition2_rows = []

            for i in range(default_nb):
                pk = i + j * default_nb
                row = {
                    self.primary_key_field_name: pk,
                    self.float_vector_field_name1: list(float_vectors[pk]),
                    self.float_vector_field_name2: list(float_vectors2[pk]),
                    self.text_field_name1: texts1[pk],
                    self.text_field_name2: texts2[pk],
                    self.json_field_name: {"float": pk * 1.0, "str": str(pk)},
                    self.string_field_name: str(pk),
                    self.int64_field_name: pk,
                    self.dynamic_field_name1: f"dynamic_value_{pk}",
                    self.dynamic_field_name2: pk * 1.0,
                }
                self.datas.append(row)

                # Distribute to partitions based on pk mod 3
                if pk % 3 == 0:
                    default_rows.append(row)
                elif pk % 3 == 1:
                    partition1_rows.append(row)
                else:
                    partition2_rows.append(row)

            # Insert into respective partitions
            if default_rows:
                self.insert(client, self.collection_name, data=default_rows)
            if partition1_rows:
                self.insert(client, self.collection_name, data=partition1_rows, partition_name=self.partition_names[0])
            if partition2_rows:
                self.insert(client, self.collection_name, data=partition2_rows, partition_name=self.partition_names[1])

            # Track all inserted data and primary keys
            self.primary_keys.extend([i + j * default_nb for i in range(default_nb)])

        self.flush(client, self.collection_name)

        # Create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=self.float_vector_field_name1,
                               metric_type="COSINE",
                               index_type="IVF_FLAT",
                               params={"nlist": 128})
        index_params.add_index(field_name=self.float_vector_field_name2,
                               metric_type="L2",
                               index_type="HNSW",
                               params={})
        index_params.add_index(field_name=self.sparse_vector_field_name1,
                               metric_type="BM25",
                               index_type="SPARSE_INVERTED_INDEX",
                               params={})
        index_params.add_index(field_name=self.sparse_vector_field_name2,
                               metric_type="BM25",
                               index_type="SPARSE_INVERTED_INDEX",
                               params={})
        self.create_index(client, self.collection_name, index_params=index_params, timeout=300)

        # Load collection
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(), self.collection_name)

        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("nq", [1, 5])
    @pytest.mark.parametrize("vector_data_type", [DataType.FLOAT_VECTOR, DataType.SPARSE_FLOAT_VECTOR])
    def test_hybrid_search_default_with_nqs_and_offset(self, nq, vector_data_type):
        """
        Test hybrid search functionality with multiple search requests and offset parameter.
        Steps:
            - Create connection, set up collection with multiple vector fields.
            - Insert records.
            - Perform hybrid search with varying nq values and offset settings.
        Expected:
            - Hybrid search returns results correctly that match the limit and offset parameters.
        """
        client = self._client()

        if vector_data_type == DataType.FLOAT_VECTOR:
            search_data = cf.gen_vectors(nq, self.float_vector_dim, vector_data_type=vector_data_type)
            field_names = [self.float_vector_field_name1, self.float_vector_field_name2]
        else:
            field_names = [self.sparse_vector_field_name1, self.sparse_vector_field_name2]
            search_data = cf.gen_varchar_data(length=10, nb=nq, text_mode=True)

        # generate hybrid search request list
        req_list = []
        for field_name in field_names:
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": field_name,
                "param": {},
                "limit": default_limit,
            })
            req_list.append(req)

        # perform hybrid search
        self.hybrid_search(client, self.collection_name, reqs=req_list,
                           ranker=WeightedRanker(*[0.6, 0.4]),
                           limit=default_limit,
                           output_fields=[self.primary_key_field_name, self.string_field_name],
                           check_task=CheckTasks.check_search_results,
                           check_items={"nq": nq,
                                        "ids": self.primary_keys,
                                        "limit": default_limit,
                                        "pk_name": self.primary_key_field_name,
                                        "original_entities": self.datas,
                                        "output_fields": [self.primary_key_field_name,
                                                          self.string_field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("req_limit_ratio", [1, 2])
    def test_hybrid_search_different_dim_and_with_full_text_search(self, req_limit_ratio):
        """
        Test hybrid search functionality combining different dimension vector fields and full text search.
        Steps:
            - Create connection, set up collection with multiple vector fields of different dims.
            - Insert records.
            - Perform hybrid search using topK limit along with a full text filter.
        Expected:
            - Hybrid search returns results correctly that match the filter and honor the topK limit.
        """
        client = self._client()

        nq = 3
        req_list = []
        for field_name in [self.float_vector_field_name1, self.sparse_vector_field_name2]:
            if field_name == self.float_vector_field_name1:
                search_data = cf.gen_vectors(nq, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
            else:
                search_data = cf.gen_varchar_data(length=10, nb=nq, text_mode=True)
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": field_name,
                "param": {},
                "limit": default_limit * req_limit_ratio,
            })
            req_list.append(req)
        hybrid_search_0 = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                             ranker=WeightedRanker(*[0.6, 0.4]),
                                             limit=default_limit,
                                             filter=f"{self.int64_field_name} > 1000",
                                             output_fields=[self.primary_key_field_name, self.string_field_name],
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": nq,
                                                          "ids": self.primary_keys,
                                                          "limit": default_limit,
                                                          "enable_milvus_client_api": True,
                                                          "pk_name": self.primary_key_field_name,
                                                          "original_entities": self.datas,
                                                          "output_fields": [self.primary_key_field_name,
                                                                            self.string_field_name]})[0]

        hybrid_search_1 = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                             ranker=WeightedRanker(*[0.6, 0.4]),
                                             limit=default_limit,
                                             filter=f"{self.int64_field_name} > 1000",
                                             output_fields=[self.primary_key_field_name, self.string_field_name],
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": nq,
                                                          "ids": self.primary_keys,
                                                          "limit": default_limit,
                                                          "enable_milvus_client_api": True,
                                                          "pk_name": self.primary_key_field_name,
                                                          "original_entities": self.datas,
                                                          "output_fields": [self.primary_key_field_name,
                                                                            self.string_field_name]})[0]
        # verify the hybrid search results are consistent
        for i in range(nq):
            assert hybrid_search_0[i].ids == hybrid_search_1[i].ids

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_search_with_expr(self):
        """
        Test hybrid search functionality with expression filter.
        Steps:
            - Create connection, set up collection with multiple vector fields.
            - Insert records.
            - Perform hybrid search using expression filter along with a topK limit.
        Expected:
            - Hybrid search returns results correctly that match the filter and honor the topK limit.
        """
        client = self._client()

        nq = 2
        search_data = cf.gen_vectors(nq, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
        # generate hybrid search request list
        req_list = []
        filter_min_value = 800
        filter_max_value = 1800
        for field_name in [self.float_vector_field_name1, self.float_vector_field_name2]:
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": field_name,
                "param": {},
                "limit": default_limit,
                "expr": f"{filter_min_value} < {self.primary_key_field_name} <= {filter_max_value}"
            })
            req_list.append(req)

        ranker = WeightedRanker(*[0.5, 0.5])
        res = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                 ranker=ranker,
                                 limit=default_limit,
                                 # fitler=f"{default_primary_key_field_name} <= {filter_max_value}",
                                 output_fields=[self.primary_key_field_name, self.string_field_name],
                                 check_task=CheckTasks.check_search_results,
                                 check_items={"nq": nq,
                                              "ids": self.primary_keys,
                                              "limit": default_limit,
                                              "pk_name": self.primary_key_field_name,
                                              "original_entities": self.datas,
                                              "output_fields": [self.primary_key_field_name,
                                                                self.string_field_name]})[0]

        # verify the hybrid search results meet the filter                                          
        for i in range(nq):
            assert max(res[i].ids) <= filter_max_value
            assert min(res[i].ids) > filter_min_value

        # hybrid search again with filter
        filter_max_value2 = np.mean(res[0].ids)
        filter = f"{self.primary_key_field_name} <= {filter_max_value2}"
        res2 = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                  ranker=ranker,
                                  limit=default_limit,
                                  fitler=filter,
                                  output_fields=[self.primary_key_field_name, self.string_field_name],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": self.primary_keys,
                                               "limit": default_limit,
                                               "pk_name": self.primary_key_field_name,
                                               "original_entities": self.datas,
                                               "output_fields": [self.primary_key_field_name,
                                                                 self.string_field_name]})[0]
        # verify filter in hybrid search is not effective
        assert max(res2[i].ids) > filter_max_value2

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("req_num", [1, 5, ct.max_hybrid_search_req_num, ct.max_hybrid_search_req_num + 1])
    def test_hybrid_search_on_same_anns_field(self, req_num):
        """
        Test hybrid search functionality on the same anns field.
        Steps:
            - Create connection, set up collection with multiple vector fields.
            - Insert records with different anns fields.
            - Perform hybrid search on the same anns field with different expressions.
        Expected:
            - Hybrid search returns results correctly that match the expressions and honor the topK limit.
        """
        nq = 3
        client = self._client()

        # generate hybrid search request list
        req_list = []
        for _ in range(req_num):
            search_data = cf.gen_vectors(nq, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": self.float_vector_field_name1,  # on the same anns field
                "param": {},
                "limit": default_limit,
                "expr": f"{self.int64_field_name} > 100"
            })
            req_list.append(req)

        ranker = RRFRanker()

        if req_num > ct.max_hybrid_search_req_num:
            check_task = CheckTasks.err_res
            check_items = {"err_code": 65535,
                           "err_msg": "maximum of ann search requests is 1024"}
        else:
            check_task = CheckTasks.check_search_results
            check_items = {"nq": nq,
                           "ids": self.primary_keys,
                           "limit": default_limit,
                           "pk_name": self.primary_key_field_name,
                           "original_entities": self.datas,
                           "output_fields": [self.primary_key_field_name, self.string_field_name]}
        self.hybrid_search(client, self.collection_name, reqs=req_list,
                           ranker=ranker,
                           limit=default_limit,
                           output_fields=[self.primary_key_field_name, self.string_field_name],
                           check_task=check_task,
                           check_items=check_items)

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_over_max_limit(self):
        """
        target: test hybrid search with over maximum limit
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        over_max_limit = ct.max_limit + 1
        # 1. initialize client
        client = self._client()
        # 2. extract vector field name
        vector_name_list = [self.float_vector_field_name1, self.float_vector_field_name2]
        search_data = cf.gen_vectors(1, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
        # 3. prepare search params
        weights = [0.3, 0.7]
        ranker = WeightedRanker(*weights)
        req_list1 = []
        for i in range(len(vector_name_list)):
            _search_param = {
                "data": search_data,
                "anns_field": vector_name_list[i],
                "param": {},
                "limit": 50,
            }
            req = AnnSearchRequest(**_search_param)
            req_list1.append(req)

        # hybrid search with over max limit
        error = {"err_code": 65535, "err_msg": f"invalid max query result window, (offset+limit) "
                                               f"should be in range [1, 16384], but got {over_max_limit}"}
        self.hybrid_search(client, self.collection_name, reqs=req_list1,
                           ranker=ranker, limit=over_max_limit,
                           check_task=CheckTasks.err_res, check_items=error)

        # hybrid search with over max limit in sub requests
        req_list = []
        for i in range(len(vector_name_list)):
            search_data = cf.gen_vectors(1, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
            search_param = {
                "data": search_data,
                "anns_field": vector_name_list[i],
                "param": {},
                "limit": over_max_limit,
            }
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
        error = {"err_code": 65535, "err_msg": f"topk [{over_max_limit}] is invalid, "
                                               f"it should be in range [1, 16384], but got {over_max_limit}"}
        self.hybrid_search(client, self.collection_name, reqs=req_list,
                           ranker=ranker, limit=default_limit,
                           check_task=CheckTasks.err_res, check_items=error)

        # TODO: hybrid search with over max limit+offset in sub requests after #45939 fixed
        # req_list = []
        # offset = 10
        # for i in range(len(vector_name_list)):
        #     search_data = cf.gen_vectors(1, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
        #     search_param = {
        #         "data": search_data,
        #         "anns_field": vector_name_list[i],
        #         "param": {"offset": offset},
        #         "limit": over_max_limit - offset,
        #     }
        #     req = AnnSearchRequest(**search_param)
        #     req_list.append(req)
        # error = {"err_code": 65535, "err_msg": f"topk [{over_max_limit}] is invalid, "
        #                                        f"it should be in range [1, 16384], but got {over_max_limit}"}
        # self.hybrid_search(client, self.collection_name, reqs=req_list,
        #                    ranker=ranker, limit=default_limit,
        #                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_with_less_than_min_limit(self):
        """
        target: test hybrid search with less than minimum limit
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        client = self._client()
        # 2. extract vector field name
        vector_name_list = [self.float_vector_field_name1, self.float_vector_field_name2]
        # 3. hybrid search with less than minimum limit
        req_list = []
        ranker = WeightedRanker(*[0.5, 0.5])
        limit = ct.min_limit - 1
        for i in range(len(vector_name_list)):
            search_data = cf.gen_vectors(1, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": vector_name_list[i],
                "param": {},
                "limit": ct.default_limit,
            })
            req_list.append(req)
        error = {"err_code": 1,
                 "err_msg": f"`limit` value {limit} is illegal"}
        self.hybrid_search(client, self.collection_name, reqs=req_list,
                           ranker=ranker, limit=limit,
                           check_task=CheckTasks.err_res, check_items=error)

        # 4. hybrid search with less than minimum limit in sub request
        req_list = []
        ranker = WeightedRanker(*[0.5, 0.5])
        limit = ct.min_limit - 1
        for i in range(len(vector_name_list)):
            search_data = cf.gen_vectors(1, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": vector_name_list[i],
                "param": {},
                "limit": limit,
            })
            req_list.append(req)
        error = {"err_code": 1,
                 "err_msg": f"topk [{limit}] is invalid, it should be in range [1, 16384], but got {limit}"}
        self.hybrid_search(client, self.collection_name, reqs=req_list,
                           ranker=ranker, limit=ct.default_limit,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("req_limit", [None, 1, ct.default_limit * 2, ct.max_limit])
    def test_hybrid_search_diff_limits_in_search_req(self, req_limit):
        """
        Test case: Hybrid search where individual search requests omit 'limit'
        Scenario: 
            - Create connection, collection, and insert data.
            - Perform hybrid search with search requests with different 'limit' parameters.
        Expected:
            - Hybrid search completes successfully and returns results up to the specified topK limit.
        """
        client = self._client()
        nq = 2
        search_data = cf.gen_vectors(nq, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
        # generate hybrid search request list
        req_list = []
        for field_name in [self.float_vector_field_name1, self.float_vector_field_name2]:
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": field_name,
                "param": {},
                "limit": req_limit
            })
            req_list.append(req)

        ranker = WeightedRanker(*[0.5, 0.5])
        expected_limit = ct.default_limit if req_limit is None else min(req_limit * len(req_list), ct.default_limit)
        self.hybrid_search(client, self.collection_name, reqs=req_list,
                           ranker=ranker,
                           limit=ct.default_limit,
                           fitler=f"{self.int64_field_name} <= 18000",
                           output_fields=[self.primary_key_field_name, self.string_field_name],
                           check_task=CheckTasks.check_search_results,
                           check_items={"nq": nq,
                                        "ids": self.primary_keys,
                                        "limit": expected_limit,
                                        "pk_name": self.primary_key_field_name,
                                        "original_entities": self.datas,
                                        "output_fields": [self.primary_key_field_name,
                                                          self.string_field_name]})

    @pytest.mark.tags(CaseLabel.L0)
    def test_hybrid_search_as_search(self):
        """
        target: test hybrid search to search as the original search interface
        method: create connection, collection, insert and search
        expected: hybrid search on one vector field with limit(topK), and the result should be equal to search
                 on the same vector field with the same params.
        """
        # 1. initialize collection with data
        client = self._client()

        nq = 3
        for field_name in [self.float_vector_field_name1, self.sparse_vector_field_name1]:
            if field_name == self.float_vector_field_name1:
                search_data = cf.gen_vectors(nq, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
            else:
                search_data = cf.gen_varchar_data(length=10, nb=nq, text_mode=True)
            req_list = []
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": field_name,
                "param": {},
                "limit": default_limit,
            })
            req_list.append(req)
            # hybrid search
            hybrid_res = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                            ranker=WeightedRanker(1),
                                            limit=default_limit,
                                            output_fields=[self.primary_key_field_name, self.string_field_name],
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": nq,
                                                         "ids": self.primary_keys,
                                                         "limit": default_limit,
                                                         "pk_name": self.primary_key_field_name,
                                                         "enable_milvus_client_api": True,
                                                         "original_entities": self.datas,
                                                         "output_fields": [self.primary_key_field_name,
                                                                           self.string_field_name]})[0]
            search_res = self.search(client, self.collection_name, data=search_data,
                                     anns_field=field_name,
                                     search_params={},
                                     limit=default_limit,
                                     output_fields=[self.primary_key_field_name, self.string_field_name],
                                     check_task=CheckTasks.check_search_results,
                                     check_items={"nq": nq,
                                                  "ids": self.primary_keys,
                                                  "limit": default_limit,
                                                  "enable_milvus_client_api": True,
                                                  "pk_name": self.primary_key_field_name,
                                                  "original_entities": self.datas,
                                                  "output_fields": [self.primary_key_field_name,
                                                                    self.string_field_name]})[0]
            for i in range(nq):
                assert hybrid_res[i].ids == search_res[i].ids

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_RRFRanker_default_parameter(self):
        """
        Test hybrid_search with the default RRFRanker configuration.

        This test:
        - Connects to the collection and prepares two different vector search requests.
        - Performs a standard search on each vector field to build reference scoring.
        - Merges the results using the RRFRanker (default parameters) for hybrid search.
        - Compares the hybrid search scores with the expected RRFRanker baseline (ignoring IDs,
        since matches can have equal scores and non-deterministic IDs).
        - Verifies repeated hybrid searches with the same parameters produce consistent results.

        The test passes if hybrid search completes successfully and the scores match the manually computed baseline.
        """
        client = self._client()
        vector_name_list = [self.float_vector_field_name1, self.float_vector_field_name2]
        # 3. prepare search params for each vector field
        req_list = []
        nq = 1  # only works for nq=1, as the limitation of get_hybrid_search_base_results_rrf()
        search_res_dict_array = []
        for field_name in vector_name_list:
            search_data = cf.gen_vectors(nq, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
            search_res_dict = {}
            search_param = {
                "data": search_data,
                "anns_field": field_name,
                "param": {},
                "limit": default_limit,
                "expr": f"{self.int64_field_name} > 0"}
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            # search for get the baseline of hybrid_search
            search_res = self.search(client, self.collection_name, data=search_data,
                                     anns_field=field_name,
                                     search_params={},
                                     limit=default_limit,
                                     filter=f"{self.int64_field_name} > 0",
                                     check_task=CheckTasks.check_search_results,
                                     check_items={"nq": 1,
                                                  "ids": self.primary_keys,
                                                  "limit": default_limit,
                                                  "pk_name": self.primary_key_field_name})[0]
            ids = search_res[0].ids
            for j in range(len(ids)):
                search_res_dict[ids[j]] = 1 / (j + 60 + 1)
            search_res_dict_array.append(search_res_dict)
        # 4. calculate hybrid search baseline for RRFRanker
        ids_answer, score_answer = cf.get_hybrid_search_base_results_rrf(search_res_dict_array)
        # 5. hybrid search
        hybrid_search_0 = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                             ranker=RRFRanker(),
                                             limit=default_limit,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "ids": self.primary_keys,
                                                          "limit": default_limit,
                                                          "pk_name": self.primary_key_field_name})[0]
        # 6. compare results through the re-calculated distances
        for i in range(len(score_answer[:default_limit])):
            assert score_answer[i] - hybrid_search_0[0].distances[i] < hybrid_search_epsilon
        # 7. run hybrid search with the same parameters twice, and compare the results
        hybrid_search_1 = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                             ranker=RRFRanker(),
                                             limit=default_limit,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": 1,
                                                          "ids": self.primary_keys,
                                                          "limit": default_limit,
                                                          "pk_name": self.primary_key_field_name})[0]

        assert hybrid_search_0[0].ids == hybrid_search_1[0].ids
        assert hybrid_search_0[0].distances == hybrid_search_1[0].distances

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_search_overall_limit_larger_sum_each_limit(self):
        """
        Test hybrid search functionality with overall limit larger than sum of each limit.
        Steps:
            - Create connection, set up collection with multiple vector fields.
            - Insert records.
            - Perform hybrid search using overall limit larger than sum of each limit.
        Expected:
            - Hybrid search returns results correctly that match the overall limit and honor the topK limit.

        """
        client = self._client()
        nq = 3
        limit = 10
        search_data1 = cf.gen_vectors(nq, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
        search_data2 = cf.gen_varchar_data(length=10, nb=nq, text_mode=True)
        vector_filed_names = [self.float_vector_field_name1, self.sparse_vector_field_name2]
        search_data_list = [search_data1, search_data2]
        id_list_nq = []
        for i in range(nq):
            id_list_nq.append([])
        # search the data1 and data2 separately
        for i in range(len(vector_filed_names)):
            search_res = self.search(client, self.collection_name, data=search_data_list[i],
                                     anns_field=vector_filed_names[i],
                                     search_params={},
                                     limit=limit,
                                     output_fields=[self.primary_key_field_name, self.string_field_name],
                                     check_task=CheckTasks.check_search_results,
                                     check_items={"nq": nq,
                                                  "ids": self.primary_keys,
                                                  "limit": limit,
                                                  "pk_name": self.primary_key_field_name,
                                                  "output_fields": [self.primary_key_field_name,
                                                                    self.string_field_name]})[0]
            for j in range(nq):
                id_list_nq[j].extend(search_res[j].ids)

        # generate hybrid search request list
        req_list = []
        for i in range(len(vector_filed_names)):
            req = AnnSearchRequest(**{
                "data": search_data_list[i],
                "anns_field": vector_filed_names[i],
                "param": {},
                "limit": limit,
            })
            req_list.append(req)
        ranker = WeightedRanker(*[0.6, 0.4])
        # hybrid search
        larger_limit = limit * len(req_list) + 1
        hybrid_search_res = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                               ranker=ranker,
                                               limit=larger_limit,
                                               output_fields=[self.primary_key_field_name,
                                                              self.string_field_name],
                                               check_task=CheckTasks.check_search_results,
                                               check_items={"nq": nq})[0]
        # verify the hybrid search results are consistent
        for i in range(nq):
            assert len(hybrid_search_res[i].ids) == len(list(set(id_list_nq[i])))

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_search_with_range_search(self):
        """
        target: test hybrid search with range search
        method:
        expected: raise exception (not support yet)
        """
        client = self._client()
        limit = 200

        field_names = [self.sparse_vector_field_name1, self.sparse_vector_field_name2]
        nq = len(field_names)  # nq should equal to number of filed names, as it would search nq by nq next
        search_data = cf.gen_varchar_data(length=10, nb=nq, text_mode=True)

        # 0. search
        mid_distances = []
        for i in range(len(field_names)):
            field_name = field_names[i]
            res_search = self.search(client, self.collection_name, data=search_data,
                                     anns_field=field_name,
                                     limit=limit)[0]
            field_mid_distances = []
            for j in range(nq):
                field_mid_distances.append(res_search[j].distances[limit // 2 - 1])
            mid_distances.append(np.mean(field_mid_distances))

        # 1. hybrid search without range search
        req_list = []
        for field_name in field_names:
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": field_name,
                "param": {},
                "limit": limit,
            })
            req_list.append(req)
        res1 = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                  ranker=WeightedRanker(0.5, 0.5),
                                  limit=limit,
                                  output_fields=[self.primary_key_field_name, self.string_field_name],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq, "ids": self.primary_keys, "limit": limit,
                                               "pk_name": self.primary_key_field_name,
                                               "output_fields": [self.primary_key_field_name,
                                                                 self.string_field_name]})[0]

        # 2. hybrid search with range search one nq by one nq
        for i in range(nq):
            req_list2 = []
            for j in range(len(field_names)):
                field_name = field_names[j]
                req = AnnSearchRequest(**{
                    "data": [search_data[i]],
                    "anns_field": field_name,
                    "param": {"params": {"radius": float(mid_distances[j]), "range_filter": 9999}},
                    "limit": limit // 2,
                })
                req_list2.append(req)
            res2 = self.hybrid_search(client, self.collection_name, reqs=req_list2,
                                      ranker=WeightedRanker(0.5, 0.5),
                                      limit=limit // 2,
                                      output_fields=[self.primary_key_field_name, self.string_field_name],
                                      check_task=CheckTasks.check_search_results,
                                      check_items={"nq": 1, "ids": self.primary_keys,  # "limit": limit // 2,
                                                   "pk_name": self.primary_key_field_name,
                                                   "output_fields": [self.primary_key_field_name,
                                                                     self.string_field_name]})[0]
            hit_rate = len(set(res2[0].ids).intersection(set(res1[i].ids[:limit // 2]))) / len(res2[0].ids)
            # log.debug(f"hybrid search with range nq={i} hit hybrid search without rage, hit rate: {hit_rate}")
            assert hit_rate >= 0.7, f"failed in nq={i}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_search_with_diff_output_fields(self):
        """
        target: test hybrid search with different output fields
        method: create connection, collection, insert and search
        expected: hybrid search successfully with different output fields
        """
        client = self._client()
        nq = 2
        limit = 100
        search_data = cf.gen_vectors(nq, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
        vector_filed_names = [self.float_vector_field_name1, self.float_vector_field_name2]
        req_list = []
        for i in range(len(vector_filed_names)):
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": vector_filed_names[i],
                "param": {},
                "limit": limit,
            })
            req_list.append(req)

        # output * fields   
        output_fields = ["*"]
        # sparse fields cannot be output, so specify the expected output fields
        expected_output_fields = [field_name for field_name in self.all_fields
                                  if field_name not in [self.sparse_vector_field_name1, self.sparse_vector_field_name2]]
        res1 = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                  ranker=WeightedRanker(0.5, 0.5),
                                  limit=limit, output_fields=output_fields,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq, "ids": self.primary_keys, "limit": limit,
                                               "pk_name": self.primary_key_field_name,
                                               "output_fields": expected_output_fields})[0]
        output_fields = self.all_fields
        # verify the error message when output sparse vector field
        err_msg = {"err_code": 999,
                   "err_msg": "not allowed to retrieve raw data of field sparse_vector1"}
        self.hybrid_search(client, self.collection_name, reqs=req_list,
                           ranker=WeightedRanker(0.5, 0.5),
                           limit=limit, output_fields=output_fields,
                           check_task=CheckTasks.err_res,
                           check_items=err_msg)
        # output all listed fields
        output_fields = expected_output_fields
        res2 = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                  ranker=WeightedRanker(0.5, 0.5),
                                  limit=limit, output_fields=output_fields,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq, "ids": self.primary_keys, "limit": limit,
                                               "pk_name": self.primary_key_field_name,
                                               "output_fields": expected_output_fields})[0]
        # output some fields
        output_fields = [self.primary_key_field_name, self.string_field_name, self.float_vector_field_name1,
                         self.float_vector_field_name2]
        res3 = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                  ranker=WeightedRanker(0.5, 0.5),
                                  limit=limit, output_fields=output_fields,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq, "ids": self.primary_keys, "limit": limit,
                                               "pk_name": self.primary_key_field_name,
                                               "output_fields": output_fields})[0]
        # output with dynamic field
        output_fields = [self.primary_key_field_name, self.string_field_name, self.dynamic_field_name1,
                         self.dynamic_field_name2]
        res4 = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                  ranker=WeightedRanker(0.5, 0.5),
                                  limit=limit, output_fields=output_fields,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq, "ids": self.primary_keys, "limit": limit,
                                               "pk_name": self.primary_key_field_name,
                                               "output_fields": output_fields})[0]

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_result_always_descending_order(self):
        """
        target: test hybrid search result always descending order in distance
        method: create connection, collection, insert and search
        expected: hybrid search successfully with result always descending order in distance
        """
        client = self._client()
        nq = 2
        limit = 100
        # test with float vector field
        vector_filed_names = [self.float_vector_field_name1, self.float_vector_field_name2]
        search_data = cf.gen_vectors(nq, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
        req_list = []
        for i in range(len(vector_filed_names)):
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": vector_filed_names[i],
                "param": {},
                "limit": limit,
            })
            req_list.append(req)
        descend_metric = "IP"  # here only impacts the distance verification in descending order or not
        self.hybrid_search(client, self.collection_name, reqs=req_list,
                           ranker=WeightedRanker(0.5, 0.5),
                           limit=limit,
                           check_task=CheckTasks.check_search_results,
                           check_items={"nq": nq, "ids": self.primary_keys,
                                        "limit": limit,
                                        "pk_name": self.primary_key_field_name,
                                        "metric": descend_metric})

        # test with sparse vector field
        vector_filed_names = [self.sparse_vector_field_name1, self.sparse_vector_field_name2]
        search_data = cf.gen_varchar_data(length=10, nb=nq, text_mode=True)
        req_list = []
        for i in range(len(vector_filed_names)):
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": vector_filed_names[i],
                "param": {},
                "limit": limit,
            })
            req_list.append(req)
        self.hybrid_search(client, self.collection_name, reqs=req_list,
                           ranker=WeightedRanker(0.5, 0.5),
                           limit=limit,
                           check_task=CheckTasks.check_search_results,
                           check_items={"nq": nq, "ids": self.primary_keys,
                                        "limit": limit,
                                        "pk_name": self.primary_key_field_name,
                                        "metric": descend_metric})

    @pytest.mark.tags(CaseLabel.L2)
    # @pytest.mark.parametrize("k", [1, 60, 1000])
    # @pytest.mark.parametrize("offset", [0, 5])
    @pytest.mark.skip(reason="milvus issue #32650")
    def test_hybrid_search_RRFRanker_different_k(self):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search.
                Note: here the result check is through comparing the score, the ids could not be compared
                because the high probability of the same score, then the id is not fixed in the range of
                the same score
        expected: hybrid search successfully with limit(topK)
        """
        client = self._client()
        k = 1
        offset = 0
        # 2. extract vector field name
        nq = 1  # TODO: the verification function only works for nq = 1 for now
        vector_name_list = [self.float_vector_field_name1, self.float_vector_field_name2]
        req_list = []
        search_res_dict_array = []
        for i in range(len(vector_name_list)):
            search_data = cf.gen_vectors(nq, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
            search_res_dict = {}
            search_param = {
                "data": search_data,
                "anns_field": vector_name_list[i],
                "param": {},
                "limit": default_limit,
            }
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            # search for get the baseline of hybrid_search
            search_res = self.search(client, self.collection_name, data=search_data,
                                     anns_field=vector_name_list[i],
                                     limit=default_limit, offset=offset,
                                     output_fields=[self.primary_key_field_name, self.string_field_name],
                                     check_task=CheckTasks.check_search_results,
                                     check_items={"nq": nq,
                                                  "ids": self.primary_keys,
                                                  "limit": default_limit,
                                                  "pk_name": self.primary_key_field_name,
                                                  "output_fields": [self.primary_key_field_name,
                                                                    self.string_field_name]})[0]
            ids = search_res[0].ids
            for j in range(len(ids)):
                search_res_dict[ids[j]] = 1 / (j + k + 1)
            search_res_dict_array.append(search_res_dict)
        # 4. calculate hybrid search baseline for RRFRanker
        ids_answer, score_answer = cf.get_hybrid_search_base_results_rrf(search_res_dict_array)
        # 5. hybrid search
        hybrid_res = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                        ranker=RRFRanker(k),
                                        limit=default_limit,
                                        offset=offset,
                                        output_fields=[self.primary_key_field_name, self.string_field_name],
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": nq,
                                                     "ids": self.primary_keys,
                                                     "limit": default_limit,
                                                     "pk_name": self.primary_key_field_name,
                                                     "output_fields": [self.primary_key_field_name,
                                                                       self.string_field_name]})[0]
        # 6. compare results through the re-calculated distances
        for i in range(len(score_answer[:default_limit])):
            assert score_answer[i] - hybrid_res[0].distances[i] < hybrid_search_epsilon * 2, f"failed in topk={i}"

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [1, 100, 16384])
    def test_hybrid_search_different_limit_round_decimal(self, limit):
        """
        target: test hybrid search with different valid limit and round decimal
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize client
        client = self._client()
        # 2. extract vector field name
        vector_name_list = [self.sparse_vector_field_name1, self.sparse_vector_field_name2]
        # 3. prepare search params
        req_list = []
        weights = [0.3, 0.7]
        search_res_dict_array = []
        if limit > default_nb:
            limit = default_limit
        metrics = []
        for i in range(len(vector_name_list)):
            search_data = cf.gen_varchar_data(length=10, nb=1, text_mode=True)
            search_res_dict = {}
            search_param = {
                "data": search_data,
                "anns_field": vector_name_list[i],
                "param": {},
                "limit": limit,
            }
            req = AnnSearchRequest(**search_param)
            req_list.append(req)
            metrics.append("BM25")
            # search to get the baseline of hybrid_search
            search_res = self.search(client, self.collection_name, data=search_data,
                                     anns_field=vector_name_list[i],
                                     limit=limit,
                                     check_task=CheckTasks.check_search_results,
                                     check_items={"nq": 1,
                                                  "ids": self.primary_keys,
                                                  "limit": limit,
                                                  "pk_name": self.primary_key_field_name})[0]
            ids = search_res[0].ids
            distance_array = search_res[0].distances
            for j in range(len(ids)):
                search_res_dict[ids[j]] = distance_array[j]
            search_res_dict_array.append(search_res_dict)
        # 4. calculate hybrid search baseline
        ids_answer, score_answer = cf.get_hybrid_search_base_results(search_res_dict_array, weights, metrics, 5)
        # 5. hybrid search
        hybrid_res = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                        ranker=WeightedRanker(*weights),
                                        limit=limit,
                                        check_task=CheckTasks.check_search_results,
                                        check_items={"nq": 1,
                                                     "ids": self.primary_keys,
                                                     "limit": limit,
                                                     "pk_name": self.primary_key_field_name})[0]
        # 6. compare results through the re-calculated distances
        for i in range(len(score_answer[:limit])):
            delta = math.fabs(score_answer[i] - hybrid_res[0].distances[i])
            if delta >= hybrid_search_epsilon:
                # print id and distance for debug
                # answer and hybrid search result
                for i1 in range(len(score_answer)):
                    log.info("answer id: %d, distance: %f" % (ids_answer[i1], score_answer[i1]))
                for i2 in range(len(hybrid_res[0].ids)):
                    log.info(
                        "hybrid search res id: %d, distance: %f" % (hybrid_res[0].ids[i2], hybrid_res[0].distances[i2]))
            assert delta < hybrid_search_epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [1, 5])
    @pytest.mark.parametrize("rerank", [RRFRanker(), WeightedRanker(0.1, 0.9)])
    def test_hybrid_search_offset_inside_outside_params(self, offset, rerank):
        """
        target: test hybrid search with offset inside and outside params
        method: create connection, collection, insert and search.
                Note: here the result check is through comparing the score, the ids could not be compared
                because the high probability of the same score, then the id is not fixed in the range of
                the same score
        expected: hybrid search successfully with limit(topK), and the result should be the same
        """
        client = self._client()
        nq = 1
        req_list = []
        search_data = cf.gen_vectors(nq, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
        for field_name in [self.float_vector_field_name1, self.float_vector_field_name2]:
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": field_name,
                "param": {"offset": offset},
                "limit": ct.default_limit,
            })
            req_list.append(req)
        hybrid_res_inside = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                               ranker=rerank,
                                               limit=ct.default_limit,
                                               output_fields=[self.primary_key_field_name,
                                                              self.string_field_name],
                                               check_task=CheckTasks.check_search_results,
                                               check_items={"nq": nq})[0]
        req_list = []
        for field_name in [self.float_vector_field_name1, self.float_vector_field_name2]:
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": field_name,
                "param": {},
                "limit": ct.default_limit
            })
            req_list.append(req)
        hybrid_res_outside = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                                ranker=rerank,
                                                limit=ct.default_limit,
                                                offset=offset,
                                                output_fields=[self.primary_key_field_name,
                                                               self.string_field_name],
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": nq})[0]
        hybrid_res_no_offset = self.hybrid_search(client, self.collection_name, reqs=req_list,
                                                  ranker=rerank,
                                                  limit=ct.default_limit,
                                                  output_fields=[self.primary_key_field_name,
                                                                 self.string_field_name],
                                                  check_task=CheckTasks.check_search_results,
                                                  check_items={"nq": nq})[0]
        for i in range(nq):
            assert hybrid_res_inside[i].ids[offset:] == \
                   hybrid_res_outside[i].ids[:-offset] == \
                   hybrid_res_no_offset[i].ids[offset:]
            # TODO: verify the offset working, uncomment the assertion below after #45939 fixed
            # assert hybrid_res_inside[i].ids != hybrid_res_no_offset[i]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("ranker", [WeightedRanker(*[0.5, 0.5]), RRFRanker()])
    def test_hybrid_search_empty_reqs(self, ranker):
        """
        Test case: Hybrid search with empty reqs
        Scenario:
            - Create a collection, insert data, and perform hybrid search with an empty request list.
        Expected:
            - Hybrid search failed with error message
        """
        client = self._client()
        err_msg = {"err_code": 65535,
                   "err_msg": "nq [0] is invalid, nq (number of search vector per search request) "
                              "should be in range [1, 16384], but got 0"}
        self.hybrid_search(client, self.collection_name,
                           reqs=[],
                           ranker=ranker,
                           limit=default_limit,
                           output_fields=[self.primary_key_field_name, self.string_field_name],
                           check_task=CheckTasks.err_res,
                           check_items=err_msg)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("ranker_param", [[0.1, 2], [0.2, 0.4, 0.8]])
    def test_hybrid_search_invalid_WeightedRanker_params(self, ranker_param):
        """
        Test case: Hybrid search with invalid WeightedRanker parameters
        Scenario:
            - Create a collection, insert data, and perform hybrid search with invalid WeightedRanker parameters.
        Expected:
            - Hybrid search failed with error message
        """
        client = self._client()
        req_list = []
        nq = 1
        search_data = cf.gen_vectors(nq, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
        for field_name in [self.float_vector_field_name1, self.float_vector_field_name2]:
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": field_name,
                "param": {},
                "limit": ct.default_limit,
            })
            req_list.append(req)

        err_msg = {"err_code": 999, "err_msg": "rank param weight should be in range [0, 1]"}
        if ranker_param == [0.2, 0.4, 0.8]:
            err_msg = {"err_code": 999,
                       "err_msg": "the length of weights param mismatch with ann search requests: "
                                  "invalid parameter[expected=2][actual=3]"}

        ranker = WeightedRanker(*ranker_param)
        self.hybrid_search(client, self.collection_name,
                           reqs=req_list,
                           ranker=ranker,
                           limit=default_limit,
                           output_fields=[self.primary_key_field_name, self.string_field_name],
                           check_task=CheckTasks.err_res,
                           check_items=err_msg)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("k", [0, 16385])
    def test_hybrid_search_RRFRanker_k_out_of_range(self, k):
        """
        Test case: Hybrid search with RRFRanker and k out of range
        Scenario:
            - Create a collection, insert data, and perform hybrid search with RRFRanker and k out of range.
        Expected:
            - Hybrid search failed with error message
        """
        client = self._client()

        req_list = []
        nq = 2
        search_data = cf.gen_vectors(nq, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
        for field_name in [self.float_vector_field_name1, self.float_vector_field_name2]:
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": field_name,
                "param": {},
                "limit": default_limit,
            })
            req_list.append(req)

        ranker = RRFRanker(k)
        # TODO: #29867, the error msg is not good enough, but as it is for now.
        err_msg = {"err_code": 65535,
                   "err_msg": "The rank params k should be in range (0, 16384)"}
        self.hybrid_search(client, self.collection_name,
                           reqs=req_list,
                           ranker=ranker,
                           limit=default_limit,
                           output_fields=[self.primary_key_field_name, self.string_field_name],
                           check_task=CheckTasks.err_res,
                           check_items=err_msg)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [ct.max_nq, ct.max_nq + 1])
    def test_hybrid_search_max_nq(self, nq):
        """
        Test case: Hybrid search with valid and boundary nq values
        Scenario:
            - Create a collection, insert data, and perform hybrid search using different nq values (max allowed and just above max).
        Expected:
            - For valid nq, hybrid search should return the correct results limited by topK.
            - For nq above the supported limit, the appropriate error is returned.
        """
        client = self._client()

        search_data = cf.gen_vectors(nq, self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
        # generate hybrid search request list
        req_list = []
        for field_name in [self.float_vector_field_name1, self.float_vector_field_name2]:
            req = AnnSearchRequest(**{
                "data": search_data,
                "anns_field": field_name,
                "param": {},
                "limit": default_limit,
            })
            req_list.append(req)

        if nq == ct.max_nq + 1:
            check_task = CheckTasks.err_res
            check_items = {"err_code": 65535,
                           "err_msg": "nq (number of search vector per search request) should be in range [1, 16384]"}
        else:
            check_task = CheckTasks.check_search_results
            check_items = {"nq": nq,
                           "ids": self.primary_keys,
                           "limit": default_limit,
                           "pk_name": self.primary_key_field_name,
                           "output_fields": [self.primary_key_field_name, self.string_field_name]}
        self.hybrid_search(client, self.collection_name, reqs=req_list,
                           ranker=WeightedRanker(*[0.6, 0.4]),
                           limit=default_limit,
                           output_fields=[self.primary_key_field_name, self.string_field_name],
                           check_task=check_task,
                           check_items=check_items)


class TestCollectionHybridSearch(TestcaseBase):
    """ Test case of search interface """

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
    @pytest.mark.parametrize("primary_field", [ct.default_string_field_name])
    def test_hybrid_search_normal(self, is_flush, primary_field, vector_data_type):
        """
        target: test hybrid search normal case
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        self._connect()
        nq = 2
        offset = 5
        # create db
        db_name = cf.gen_unique_str("db")
        self.database_wrap.create_database(db_name)
        # using db and create collection
        self.database_wrap.using_database(db_name)

        # 1. initialize collection with data
        dim = 64
        enable_dynamic_field = True
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general("", True, dim=dim, is_flush=is_flush,
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
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name])
    def test_hybrid_search_different_metric_type(self, primary_field, is_flush, metric_type):
        """
        target: test hybrid search for fields with different metric type
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 128
        nq = 3
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general("", True, dim=dim, is_flush=is_flush, is_index=False,
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
                "param": {},
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
    @pytest.mark.parametrize("primary_field", [ct.default_int64_field_name])
    def test_hybrid_search_different_metric_type_each_field(self, primary_field, is_flush, metric_type):
        """
        target: test hybrid search for fields with different metric type
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # 1. initialize collection with data
        dim = 91
        nq = 4
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general("", True, dim=dim, is_flush=is_flush, is_index=False,
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
            "param": {"metric_type": "L2"},
            "limit": default_limit,
            "expr": "int64 > 0"}
        req = AnnSearchRequest(**search_param)
        req_list.append(req)
        search_param = {
            "data": [[random.random() for _ in range(dim)] for _ in range(nq)],
            "anns_field": vector_name_list[1],
            "param": {"metric_type": "IP"},
            "limit": default_limit,
            "expr": "int64 > 0"}
        req = AnnSearchRequest(**search_param)
        req_list.append(req)
        search_param = {
            "data": [[random.random() for _ in range(dim)] for _ in range(nq)],
            "anns_field": vector_name_list[2],
            "param": {"metric_type": "COSINE"},
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
    def test_hybrid_search_WeightedRanker_different_parameters(self):
        """
        target: test hybrid search for fields with different offset
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        # TODO: to be implement
        pass

    @pytest.mark.skip(reason="skip for #45939")
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
            self.init_collection_general("", True, multiple_dim_array=[default_dim, default_dim])[0:5]
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
    @pytest.mark.parametrize("primary_field", [ct.default_string_field_name])
    def test_hybrid_search_is_partition_key(self, primary_field, limit, vector_data_type):
        """
        target: test hybrid search with different valid limit and round decimal
        method: create connection, collection, insert and search
        expected: hybrid search successfully with limit(topK)
        """
        nq = 2
        # 1. initialize collection with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general("", True, primary_field=primary_field,
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

        # 6. calculate hybrid search baseline
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
            self.init_collection_general("", True, nb=nb, multiple_dim_array=[dim, dim * 2],
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
            # search for get the baseline of hybrid_search
            search_res = collection_w.search(vector, vector_name_list[i],
                                             param={},
                                             limit=default_limit
                                             )[0]
            ids = search_res[0].ids
            for j in range(len(ids)):
                search_res_dict[ids[j]] = 1 / (j + k + 1)
            search_res_dict_array.append(search_res_dict)
        # 4. calculate hybrid search baseline for RRFRanker
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_not_support_search_by_pk(self):
        """
        Test case: Hybrid search does not support search by pk
        Scenario:
            - Create connection, collection, and insert data.
            - Perform hybrid search with search requests with different 'limit' parameters.
        Expected:
            - Hybrid search failed with error msg
        """
        nq = 2
        req_limit = 10
        ids_to_search = [0, 1]
        # generate hybrid search request list
        sub_params = {
            "ids": ids_to_search,
            "anns_field": ct.default_float_vec_field_name,
            "param": {},
            "limit": req_limit
        }
        with pytest.raises(TypeError,
                           match="AnnSearchRequest.__init__.*got an unexpected keyword argument 'ids'"):
            req = AnnSearchRequest(**sub_params)
