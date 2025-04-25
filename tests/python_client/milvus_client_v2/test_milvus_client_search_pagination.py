import logging

import numpy as np
from common.constants import *
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_v2_base import TestMilvusClientV2Base
from base.client_base import TestcaseBase
import random
import pytest
import pandas as pd
from faker import Faker
import inspect

Faker.seed(19530)
fake_en = Faker("en_US")
fake_zh = Faker("zh_CN")

# patch faker to generate text with specific distribution
cf.patch_faker_text(fake_en, cf.en_vocabularies_distribution)
cf.patch_faker_text(fake_zh, cf.zh_vocabularies_distribution)

pd.set_option("expand_frame_repr", False)

prefix = "search_collection"
default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "int64 >= 0"
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_json_search_exp = "json_field[\"number\"] >= 0"
perfix_expr = 'varchar like "0%"'
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name
default_json_field_name = ct.default_json_field_name
vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
nq = 1
field_name = default_float_vec_field_name
search_param = {"nprobe": 1}
entity = gen_entities(1, is_normal=True)
entities = gen_entities(default_nb, is_normal=True)
raw_vectors, binary_entities = gen_binary_entities(default_nb)
default_query, _ = gen_search_vectors_params(field_name, entities, default_top_k, nq)
half_nb = ct.default_nb // 2

default_primary_key_field_name = "id"
default_vector_field_name = "vector"


@pytest.mark.xdist_group("TestMilvusClientSearchPagination")
class TestMilvusClientSearchPagination(TestMilvusClientV2Base):
    """Test search with pagination functionality"""

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestMilvusClientSearchPagination" + cf.gen_unique_str("_")
        self.partition_names = ["partition_1", "partition_2"]
        self.float_vector_field_name = "float_vector"
        self.bfloat16_vector_field_name = "bfloat16_vector" 
        self.sparse_vector_field_name = "sparse_vector"
        self.binary_vector_field_name = "binary_vector"
        self.float_vector_dim = 128
        self.bf16_vector_dim = 200
        self.binary_vector_dim = 256    
        self.primary_keys = []
        self.enable_dynamic_field = False
        self.datas = []

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        """
        Initialize collection before test class runs
        """
        # Get client connection
        client = self._client()

        # Create collection
        collection_schema = self.create_schema(client, enable_dynamic_field=self.enable_dynamic_field)[0]
        collection_schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        collection_schema.add_field(self.float_vector_field_name, DataType.FLOAT_VECTOR, dim=128)
        collection_schema.add_field(self.bfloat16_vector_field_name, DataType.BFLOAT16_VECTOR, dim=200)
        collection_schema.add_field(self.sparse_vector_field_name, DataType.SPARSE_FLOAT_VECTOR)
        collection_schema.add_field(self.binary_vector_field_name, DataType.BINARY_VECTOR, dim=256)
        collection_schema.add_field(default_float_field_name, DataType.FLOAT)
        collection_schema.add_field(default_string_field_name, DataType.VARCHAR, max_length=256)
        collection_schema.add_field(default_int64_field_name, DataType.INT64)
        self.create_collection(client, self.collection_name, schema=collection_schema, force_teardown=False)
        for partition_name in self.partition_names:
            self.create_partition(client, self.collection_name, partition_name=partition_name)

        # Define number of insert iterations
        insert_times = 10
        
        # Generate vectors for each type and store in self
        float_vectors = cf.gen_vectors(default_nb * insert_times, dim=self.float_vector_dim,
                                       vector_data_type=DataType.FLOAT_VECTOR)
        bfloat16_vectors = cf.gen_vectors(default_nb * insert_times, dim=self.bf16_vector_dim,
                                          vector_data_type=DataType.BFLOAT16_VECTOR)
        sparse_vectors = cf.gen_sparse_vectors(default_nb * insert_times, empty_percentage=2)
        _, binary_vectors = cf.gen_binary_vectors(default_nb * insert_times, dim=self.binary_vector_dim)

        # Insert data multiple times with non-duplicated primary keys
        for j in range(insert_times):
            # Group rows by partition based on primary key mod 3
            default_rows = []
            partition1_rows = []
            partition2_rows = []
            
            for i in range(default_nb):
                pk = i + j * default_nb
                row = {
                    default_primary_key_field_name: pk,
                    self.float_vector_field_name: list(float_vectors[pk]),
                    self.bfloat16_vector_field_name: bfloat16_vectors[pk],
                    self.sparse_vector_field_name: sparse_vectors[pk], 
                    self.binary_vector_field_name: binary_vectors[pk],
                    default_float_field_name: pk * 1.0,
                    default_string_field_name: str(pk),
                    default_int64_field_name: pk
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
        index_params.add_index(field_name=self.float_vector_field_name,
                               metric_type="COSINE",
                               index_type="IVF_FLAT",
                               params={"nlist": 128})
        index_params.add_index(field_name=self.bfloat16_vector_field_name,
                               metric_type="L2",
                               index_type="DISKANN",
                               params={})
        index_params.add_index(field_name=self.sparse_vector_field_name,
                               metric_type="IP",
                               index_type="SPARSE_INVERTED_INDEX",
                               params={})
        index_params.add_index(field_name=self.binary_vector_field_name,
                               metric_type="JACCARD",
                               index_type="BIN_IVF_FLAT",
                               params={"nlist": 128})
        self.create_index(client, self.collection_name, index_params=index_params)

        # Load collection
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(), self.collection_name)

        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_float_vectors_with_pagination_default(self):
        """
        target: test search float vectors with pagination
        method: 1. connect and create a collection
                2. search float vectors with pagination
                3. search with offset+limit
                4. compare with the search results whose corresponding ids should be the same
        expected: search successfully and ids is correct
        """
        client = self._client()
        # 1. Create collection with schema
        collection_name = self.collection_name

        # 2. Search with pagination for 10 pages
        limit = 100
        pages = 10
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)
        all_pages_results = []
        for page in range(pages):
            offset = page * limit
            search_params = {"metric_type": "COSINE", "params": {"nprobe": 100}, "offset": offset}
            search_res_with_offset, _ = self.search(
                client,
                collection_name,
                vectors_to_search[:default_nq],
                anns_field=self.float_vector_field_name,
                search_params=search_params,
                limit=limit,
                check_task=CheckTasks.check_search_results,
                check_items={"enable_milvus_client_api": True,
                             "nq": default_nq,
                             "limit": limit,
                             "metric": "COSINE",
                             "vector_nq": vectors_to_search[:default_nq],
                             "original_vectors": [self.datas[i][self.float_vector_field_name] for i in range(len(self.datas))]
                             }
            )
            all_pages_results.append(search_res_with_offset)

        # 3. Search without pagination
        search_params_full = {"metric_type": "COSINE", "params": {"nprobe": 100}}
        search_res_full, _ = self.search(
            client,
            collection_name,
            vectors_to_search[:default_nq],
            anns_field=self.float_vector_field_name,
            search_params=search_params_full,
            limit=limit * pages
        )

        # 4. Compare results - verify pagination results equal the results in full search with offsets
        for p in range(pages):
            page_res = all_pages_results[p]
            for i in range(default_nq):
                page_ids = [page_res[i][j].get('id') for j in range(limit)]
                ids_in_full = [search_res_full[i][p * limit:p * limit + limit][j].get('id') for j in range(limit)]
                assert page_ids == ids_in_full

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_bfloat16_with_pagination_default(self):
        """
        target: test search bfloat16 vectors with pagination
        method: 1. connect and create a collection
                2. search bfloat16 vectors with pagination
                3. search with offset+limit
                4. compare with the search results whose corresponding ids should be the same
        expected: search successfully and ids is correct
        """
        client = self._client()
        # 1. Create collection with schema
        collection_name = self.collection_name

        # 2. Search with pagination for 10 pages
        limit = 100
        pages = 10
        vectors_to_search = cf.gen_vectors(default_nq, self.bf16_vector_dim, vector_data_type=DataType.BFLOAT16_VECTOR)
        all_pages_results = []
        for page in range(pages):
            offset = page * limit
            search_params = {"offset": offset}
            search_res_with_offset, _ = self.search(
                client,
                collection_name,
                vectors_to_search[:default_nq],
                anns_field=self.bfloat16_vector_field_name,
                search_params=search_params,
                limit=limit,
                check_task=CheckTasks.check_search_results,
                check_items={"enable_milvus_client_api": True,
                             "nq": default_nq,
                             "limit": limit
                             }
            )
            all_pages_results.append(search_res_with_offset)

        # 3. Search without pagination
        search_params_full = {}
        search_res_full, _ = self.search(
            client,
            collection_name,
            vectors_to_search[:default_nq],
            anns_field=self.bfloat16_vector_field_name,
            search_params=search_params_full,
            limit=limit * pages
        )

        # 4. Compare results - verify pagination results equal the results in full search with offsets
        for p in range(pages):
            page_res = all_pages_results[p]
            for i in range(default_nq):
                page_ids = [page_res[i][j].get('id') for j in range(limit)]
                ids_in_full = [search_res_full[i][p * limit:p * limit + limit][j].get('id') for j in range(limit)]
                intersection_ids = set(ids_in_full).intersection(set(page_ids))
                log.debug(f"page[{p}], nq[{i}], intersection_ids: {len(intersection_ids)}")

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_sparse_with_pagination_default(self):
        """
        target: test search sparse vectors with pagination
        method: 1. connect and create a collection
                2. search sparse vectors with pagination
                3. search with offset+limit
                4. compare with the search results whose corresponding ids should be the same
        expected: search successfully and ids is correct
        """
        client = self._client()
        # 1. Create collection with schema
        collection_name = self.collection_name

        # 2. Search with pagination for 10 pages
        limit = 100
        pages = 10
        vectors_to_search = cf.gen_sparse_vectors(default_nq, empty_percentage=2)
        all_pages_results = []
        for page in range(pages):
            offset = page * limit
            search_params = {"params": {"drop_ratio_search": "0.2"}, "offset": offset}
            search_res_with_offset, _ = self.search(
                client,
                collection_name,
                vectors_to_search[:default_nq],
                anns_field=self.sparse_vector_field_name,
                search_params=search_params,
                limit=limit,
                check_task=CheckTasks.check_search_results,
                check_items={"enable_milvus_client_api": True,
                             "nq": default_nq,
                             "limit": limit
                             }
            )
            all_pages_results.append(search_res_with_offset)

        # 3. Search without pagination
        search_params_full = {"params": {"drop_ratio_search": "0.2"}}
        search_res_full, _ = self.search(
            client,
            collection_name,
            vectors_to_search[:default_nq],
            anns_field=self.sparse_vector_field_name,
            search_params=search_params_full,
            limit=limit * pages
        )

        # 4. Compare results - verify pagination results equal the results in full search with offsets
        for p in range(pages):
            page_res = all_pages_results[p]
            for i in range(default_nq):
                page_ids = [page_res[i][j].get('id') for j in range(limit)]
                ids_in_full = [search_res_full[i][p * limit:p * limit + limit][j].get('id') for j in range(limit)]
                assert page_ids == ids_in_full

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_binary_with_pagination_default(self):
        """
        target: test search binary vectors with pagination
        method: 1. connect and create a collection
                2. search binary vectors with pagination
                3. search with offset+limit
                4. compare with the search results whose corresponding ids should be the same
        expected: search successfully and ids is correct
        """
        client = self._client()
        # 1. Create collection with schema
        collection_name = self.collection_name

        # 2. Search with pagination for 10 pages
        limit = 100
        pages = 10
        vectors_to_search = cf.gen_binary_vectors(default_nq, dim=self.binary_vector_dim)[1]
        all_pages_results = []
        for page in range(pages):
            offset = page * limit
            search_params = {"params": {"nprobe": 32}, "offset": offset}
            search_res_with_offset, _ = self.search(
                client,
                collection_name,
                vectors_to_search[:default_nq],
                anns_field=self.binary_vector_field_name,
                search_params=search_params,
                limit=limit,
                check_task=CheckTasks.check_search_results,
                check_items={"enable_milvus_client_api": True,
                             "nq": default_nq,
                             "limit": limit
                             }
            )
            all_pages_results.append(search_res_with_offset)

        # 3. Search without pagination
        search_params_full = {"params": {"nprobe": 32}}
        search_res_full, _ = self.search(
            client,
            collection_name,
            vectors_to_search[:default_nq],
            anns_field=self.binary_vector_field_name,
            search_params=search_params_full,
            limit=limit * pages
        )

        # 4. Compare results - verify pagination results equal the results in full search with offsets
        for p in range(pages):
            page_res = all_pages_results[p]
            for i in range(default_nq):
                page_ids = [page_res[i][j].get('id') for j in range(limit)]
                ids_in_full = [search_res_full[i][p * limit:p * limit + limit][j].get('id') for j in range(limit)]

                # Calculate intersection between paginated results and baseline full results
                common_ids = set(page_ids) & set(ids_in_full) 
                # Calculate overlap ratio using full results as baseline
                overlap_ratio = len(common_ids) / len(ids_in_full) * 100
                assert overlap_ratio >= 80, f"Only {overlap_ratio}% overlap with baseline results, expected >= 80%"

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("limit", [100, 3000, 10000])
    def test_search_with_pagination_topk(self, limit):
        """
        target: Test search pagination when limit + offset equals topK
        method: 1. Get client connection
                2. Calculate offset as topK - limit 
                3. Perform search with calculated offset and limit
                4. Verify search results are returned correctly
        expected: Search should complete successfully with correct number of results
                 based on the specified limit and offset
        """
        client = self._client()
        # 1. Create collection with schema
        collection_name = self.collection_name

        # 2. Search with pagination 
        topK=16384
        offset = topK - limit
        search_param = {"nprobe": 10, "offset": offset}
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)
        client.search(collection_name, vectors_to_search[:default_nq], anns_field=self.float_vector_field_name,
                      search_params=search_param, limit=limit, check_task=CheckTasks.check_search_results,
                      check_items={"enable_milvus_client_api": True,
                                   "nq": default_nq,
                                   "limit": limit}) 
    
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [0, 100])
    def test_search_pagination_with_expression(self, offset):
        """
        target: Test search pagination functionality with filtering expressions
        method: 1. Create collection and insert test data
                2. Search with pagination offset and expression filter
                3. Search with full limit and expression filter 
                4. Compare paginated results match full results with offset
        expected: Paginated search results should match corresponding subset of full search results
        """
        client = self._client()
        collection_name = self.collection_name

        # filter result with expression in collection
        total_datas = self.datas
        for expressions in cf.gen_normal_expressions_and_templates():
            log.debug(f"search with expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, _id in enumerate(self.primary_keys):
                int64 = total_datas[i][ct.default_int64_field_name]
                float = total_datas[i][ct.default_float_field_name]
                if not expr or eval(expr):
                    filter_ids.append(_id)
            # 2. search
            limit = min(default_limit, len(filter_ids))
            if offset >= len(filter_ids):
                limit = 0
            elif len(filter_ids) - offset < default_limit:
                limit = len(filter_ids) - offset
            # 3. search with a high nprobe for better accuracy
            search_params = {"metric_type": "COSINE", "params": {"nprobe": 128}, "offset": offset} 
            vectors_to_search = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
            search_res_with_offset, _ = self.search(
                client,
                collection_name,
                vectors_to_search[:default_nq],
                anns_field=self.float_vector_field_name,
                search_params=search_params,
                limit=default_limit,
                filter=expr,
                check_task=CheckTasks.check_search_results,
                check_items={"enable_milvus_client_api": True,
                             "nq": default_nq,
                             "limit": limit}
            )

            # 4. search with offset+limit
            search_params_full = {"metric_type": "COSINE", "params": {"nprobe": 128}}
            search_res_full, _ = self.search(
                client,
                collection_name,
                vectors_to_search[:default_nq],
                anns_field=self.float_vector_field_name,
                search_params=search_params_full,
                limit=default_limit + offset,
                filter=expr
            )

            # 5. Compare results
            filter_ids_set = set(filter_ids)
            for hits in search_res_with_offset:
                ids = [hit.get('id') for hit in hits]
                assert set(ids).issubset(filter_ids_set)
            
            # Compare pagination results with full results
            page_ids = [search_res_with_offset[0][j].get('id') for j in range(limit)]
            ids_in_full = [search_res_full[0][offset:offset + limit][j].get('id') for j in range(limit)]
            assert page_ids == ids_in_full

            # 6. search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res_with_offset, _ = self.search(
                client,
                collection_name,
                vectors_to_search[:default_nq],
                anns_field=self.float_vector_field_name,
                search_params=search_params,
                limit=default_limit,
                filter=expr,
                filter_params=expr_params,
                check_task=CheckTasks.check_search_results,
                check_items={"enable_milvus_client_api": True,
                             "nq": default_nq,
                             "limit": limit}
            )

            # 7. search with offset+limit
            search_res_full, _ = self.search(
                client,
                collection_name,
                vectors_to_search[:default_nq],
                anns_field=self.float_vector_field_name,
                search_params=search_params_full,
                limit=default_limit + offset,
                filter=expr,
                filter_params=expr_params
            )

            # 8. Compare results
            filter_ids_set = set(filter_ids)
            for hits in search_res_with_offset:
                ids = [hit.get('id') for hit in hits]
                assert set(ids).issubset(filter_ids_set)
            
            # Compare pagination results with full results
            page_ids = [search_res_with_offset[0][j].get('id') for j in range(limit)]
            ids_in_full = [search_res_full[0][offset:offset + limit][j].get('id') for j in range(limit)]
            assert page_ids == ids_in_full
    
    @pytest.mark.tags(CaseLabel.L1)
    def test_search_pagination_in_partitions(self):
        """
        target: test search pagination in partitions
        method: 1. create collection and insert data
                2. search with pagination in partitions
                3. compare with the search results whose corresponding ids should be the same
        """
        client = self._client() 
        collection_name = self.collection_name
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)
        # search with pagination in partition_1
        limit = 50
        pages = 10
        for page in range(pages):
            offset = page * limit
            search_params = {"offset": offset}
            search_res_with_offset, _ = self.search(
                client,
                collection_name,
                vectors_to_search[:default_nq],
                partition_names=[self.partition_names[0]],
                anns_field=self.float_vector_field_name,
                search_params=search_params,
                limit=limit,
                check_task=CheckTasks.check_search_results,
                check_items={"enable_milvus_client_api": True,
                             "nq": default_nq, "limit": limit})
            
            # assert every id in search_res_with_offset %3 ==1
            for hits in search_res_with_offset:
                for hit in hits:
                    assert hit.get('id') % 3 == 1

        # search with pagination in partition_1 and partition_2
        for page in range(pages):
            offset = page * limit
            search_params = {"offset": offset}
            search_res_with_offset, _ = self.search(
                client,
                collection_name,
                vectors_to_search[:default_nq],
                partition_names=self.partition_names,
                anns_field=self.float_vector_field_name,
                search_params=search_params,
                limit=limit,
                check_task=CheckTasks.check_search_results,
                check_items={"enable_milvus_client_api": True,
                             "nq": default_nq, "limit": limit})

            # assert every id in search_res_with_offset %3 ==1 or ==2
            for hits in search_res_with_offset:
                for hit in hits:
                    assert hit.get('id') % 3 == 1 or hit.get('id') % 3 == 2

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_pagination_with_different_offset(self):
        """
        target: test search pagination with different offset
        method: 1. create collection and insert data
                2. search with different offset, including offset > limit, offset = 0
                3. compare with the search results whose corresponding ids should be the same
        """
        client = self._client()
        collection_name = self.collection_name
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)
        # search with offset > limit
        offset = default_limit + 10
        search_params = {"offset": offset}
        self.search(client, collection_name, vectors_to_search[:default_nq],
                    anns_field=self.float_vector_field_name,
                    search_params=search_params, limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq, "limit": default_limit})
        # search with offset = 0
        offset = 0
        search_params = {"offset": offset}
        self.search(client, collection_name, vectors_to_search[:default_nq],
                    anns_field=self.float_vector_field_name,
                    search_params=search_params, limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": default_nq, "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [0, 20, 100, 200])
    def test_search_offset_different_position(self, offset):
        """
        target: test search offset param in different position
        method: create connection, collection, insert data, search with offset in different position
        expected: search successfully
        """
        client = self._client()
        collection_name = self.collection_name
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)

        # 1. search with offset in search_params
        limit = 100
        search_params = {"offset": offset}
        res1, _ = self.search(client, collection_name, vectors_to_search[:default_nq],
                              anns_field=self.float_vector_field_name,
                              search_params=search_params,
                              limit=limit,
                              check_task=CheckTasks.check_search_results,
                              check_items={"enable_milvus_client_api": True,
                                           "nq": default_nq, "limit": limit})

        # 2. search with offset in search 
        search_params = {}
        res2, _ = self.search(client, collection_name, vectors_to_search[:default_nq],
                              anns_field=self.float_vector_field_name,
                              search_params=search_params,
                              offset=offset,
                              limit=limit,
                              check_task=CheckTasks.check_search_results,
                              check_items={"enable_milvus_client_api": True,
                                           "nq": default_nq, "limit": limit})
        # 3. compare results
        assert res1 == res2

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_pagination_empty_list(self):
        """
        target: test search pagination with empty list of vectors
        method: create connection, collection, insert data, search with offset
        expected: search successfully
        """
        client = self._client()
        collection_name = self.collection_name
        vectors_to_search = []
        offset = 10
        limit = 100
        search_params = {"offset": offset}
        error ={"err_code": 1, "err_msg": "list index out of range"}
        self.search(client, collection_name, vectors_to_search,
                    anns_field=self.float_vector_field_name,
                    search_params=search_params,
                    limit=limit,
                    check_task=CheckTasks.err_res,
                    check_items=error)
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("offset", [" ", 1.0, [1, 2], {1}, "12 s"])
    def test_search_pagination_with_invalid_offset_type(self, offset):
        """
        target: test search pagination with invalid offset type
        method: create connection, collection, insert and search with invalid offset type
        expected: raise exception
        """
        client = self._client()
        collection_name = self.collection_name
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)

        search_params = {"offset": offset}
        error = {"err_code": 1, "err_msg": "wrong type for offset, expect int"}
        self.search(
            client,
            collection_name,
            vectors_to_search[:default_nq],
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.err_res,
            check_items=error)
    

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("offset", [-1, 16385])
    def test_search_pagination_with_invalid_offset_value(self, offset):
        """
        target: test search pagination with invalid offset value
        method: create connection, collection, insert and search with invalid offset value
        expected: raise exception
        """
        client = self._client()
        collection_name = self.collection_name
        vectors_to_search = cf.gen_vectors(default_nq, self.float_vector_dim)
        search_params = {"offset": offset}
        error = {"err_code": 1, "err_msg": f"offset [{offset}] is invalid, it should be in range [1, 16384]"}
        self.search(
            client,
            collection_name,
            vectors_to_search[:default_nq],
            anns_field=self.float_vector_field_name,
            search_params=search_params,
            limit=default_limit,
            check_task=CheckTasks.err_res,
            check_items=error
            )


class TestSearchPaginationIndependent(TestMilvusClientV2Base):
    """ Test case of search pagination with independent collection """

    def do_search_pagination_and_assert(self, client, collection_name,
                                        limit=10, pages=10,
                                        dim=default_dim,
                                        vector_dtype=DataType.FLOAT_VECTOR,
                                        index=ct.L0_index_types[0],
                                        metric_type=ct.default_L0_metric,
                                        expected_overlap_ratio=80):
        # 2. Search with pagination for 5 pages
        vectors_to_search = cf.gen_vectors(default_nq, dim, vector_data_type=vector_dtype)
        all_pages_results = []
        for page in range(pages):
            offset = page * limit
            search_params = {"offset": offset}
            search_res_with_offset, _ = self.search(
                client,
                collection_name,
                vectors_to_search[:default_nq],
                anns_field=default_vector_field_name,
                search_params=search_params,
                limit=limit,
                check_task=CheckTasks.check_search_results,
                check_items={"enable_milvus_client_api": True,
                             "nq": default_nq,
                             "limit": limit,
                             "metric": metric_type,
                             }
            )
            all_pages_results.append(search_res_with_offset)

        # 3. Search without pagination
        search_params_full = {}
        search_res_full, _ = self.search(
            client,
            collection_name,
            vectors_to_search[:default_nq],
            anns_field=default_vector_field_name,
            search_params=search_params_full,
            limit=limit * pages
        )

        # 4. Compare results - verify pagination results equal the results in full search with offsets
        for p in range(pages):
            page_res = all_pages_results[p]
            for i in range(default_nq):
                page_ids = [page_res[i][j].get('id') for j in range(limit)]
                ids_in_full = [search_res_full[i][p * limit:p * limit + limit][j].get('id') for j in range(limit)]
                # Calculate intersection between paginated results and baseline full results
                common_ids = set(page_ids) & set(ids_in_full)
                # Calculate overlap ratio using full results as baseline
                overlap_ratio = len(common_ids) / len(ids_in_full) * 100
                log.debug(
                    f"range search {vector_dtype.name} {index} {metric_type} results overlap {overlap_ratio}")
                assert overlap_ratio >= expected_overlap_ratio, \
                    f"Only {overlap_ratio}% overlap with baseline results, expected >= {expected_overlap_ratio}%"

    """
    ******************************************************************
    #  The following are invalid cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize('vector_dtype', ct.all_dense_vector_types)
    @pytest.mark.parametrize('index', ct.all_index_types[:7])
    @pytest.mark.parametrize('metric_type', ct.dense_metrics)
    def test_search_pagination_dense_vectors_indices_metrics_growing(self, vector_dtype, index, metric_type):
        """
        target: test search pagination with growing data
        method: create connection, collection, insert data and search
                check the results by searching with limit+offset
        expected: searched successfully
        """
        client = self._client()

        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(default_primary_key_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, datatype=vector_dtype, dim=default_dim)
        schema.add_field(default_float_field_name, datatype=DataType.FLOAT)
        schema.add_field(default_string_field_name, datatype=DataType.VARCHAR, max_length=100)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data in 3 batches with unique primary keys using a loop
        insert_times = 3
        random_vectors = list(cf.gen_vectors(default_nb*insert_times, default_dim, vector_data_type=vector_dtype)) \
            if vector_dtype == DataType.FLOAT_VECTOR \
            else cf.gen_vectors(default_nb*insert_times, default_dim, vector_data_type=vector_dtype)
        for j in range(insert_times):
            start_pk = j * default_nb
            rows = [{
                default_primary_key_field_name: i + start_pk,
                default_vector_field_name: random_vectors[i + start_pk],
                default_float_field_name: (i + start_pk) * 1.0,
                default_string_field_name: str(i + start_pk)
            } for i in range(default_nb)]
            self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # build index
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(default_vector_field_name, index_type=index,
                               metric_type=metric_type,
                               params=cf.get_index_params_params(index_type=index))
        self.create_index(client, collection_name, index_params=index_params)

        # load the collection with index
        assert self.wait_for_index_ready(client, collection_name, default_vector_field_name, timeout=120)
        self.load_collection(client, collection_name)

        # search and assert
        limit = 50
        pages = 5
        expected_overlap_ratio = 20
        self.do_search_pagination_and_assert(client, collection_name, limit=limit, pages=pages, dim=default_dim,
                                             vector_dtype=vector_dtype, index=index, metric_type=metric_type,
                                             expected_overlap_ratio=expected_overlap_ratio)

        # insert additional data without flush
        random_vectors = list(cf.gen_vectors(default_nb, default_dim, vector_data_type=vector_dtype)) \
            if vector_dtype == DataType.FLOAT_VECTOR \
            else cf.gen_vectors(default_nb, default_dim, vector_data_type=vector_dtype)
        start_pk = default_nb * insert_times
        rows = [{
            default_primary_key_field_name: i + start_pk,
            default_vector_field_name: random_vectors[i],
            default_float_field_name: (i + start_pk) * 1.0,
            default_string_field_name: str(i + start_pk)
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)

        # search and assert
        self.do_search_pagination_and_assert(client, collection_name, limit=limit, pages=pages, dim=default_dim,
                                             vector_dtype=vector_dtype, index=index, metric_type=metric_type,
                                             expected_overlap_ratio=expected_overlap_ratio)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize('index', ct.binary_supported_index_types)
    @pytest.mark.parametrize('metric_type', ct.binary_metrics[:2])
    def test_search_pagination_binary_index_growing(self, index, metric_type):
        """
        target: test search pagination with binary index
        method: create connection, collection, insert data, create index and search
        expected: searched successfully
        """

        vector_dtype = DataType.BINARY_VECTOR
        client = self._client()

        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(default_primary_key_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, datatype=vector_dtype, dim=default_dim)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data in 3 batches with unique primary keys using a loop
        insert_times = 3
        random_vectors = list(cf.gen_vectors(default_nb * insert_times, default_dim, vector_data_type=vector_dtype)) \
            if vector_dtype == DataType.FLOAT_VECTOR \
            else cf.gen_vectors(default_nb * insert_times, default_dim, vector_data_type=vector_dtype)
        for j in range(insert_times):
            start_pk = j * default_nb
            rows = [{
                default_primary_key_field_name: i + start_pk,
                default_vector_field_name: random_vectors[i + start_pk]
            } for i in range(default_nb)]
            self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # build index
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(default_vector_field_name, index_type=index,
                               metric_type=metric_type,
                               params=cf.get_index_params_params(index_type=index))
        self.create_index(client, collection_name, index_params=index_params)

        # load the collection with index
        assert self.wait_for_index_ready(client, collection_name, default_vector_field_name, timeout=120)
        self.load_collection(client, collection_name)

        # search and assert
        limit = 50
        pages = 5
        expected_overlap_ratio = 20
        self.do_search_pagination_and_assert(client, collection_name, limit=limit, pages=pages, dim=default_dim,
                                             vector_dtype=vector_dtype, index=index, metric_type=metric_type,
                                             expected_overlap_ratio=expected_overlap_ratio)

        # insert additional data without flush
        random_vectors = list(cf.gen_vectors(default_nb, default_dim, vector_data_type=vector_dtype)) \
            if vector_dtype == DataType.FLOAT_VECTOR \
            else cf.gen_vectors(default_nb, default_dim, vector_data_type=vector_dtype)
        start_pk = default_nb * insert_times
        rows = [{
            default_primary_key_field_name: i + start_pk,
            default_vector_field_name: random_vectors[i]
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)

        # search and assert
        self.do_search_pagination_and_assert(client, collection_name, limit=limit, pages=pages, dim=default_dim,
                                             vector_dtype=vector_dtype, index=index, metric_type=metric_type,
                                             expected_overlap_ratio=expected_overlap_ratio)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize('index', ct.sparse_supported_index_types)
    @pytest.mark.parametrize('metric_type', ["IP"])
    def test_search_pagination_sparse_index_growing(self, index, metric_type):
        """
        target: test search pagination with sparse index
        method: create connection, collection, insert data, create index and search
        expected: searched successfully
        """
        vector_dtype = DataType.SPARSE_FLOAT_VECTOR
        client = self._client()

        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(default_primary_key_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_vector_field_name, datatype=vector_dtype)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data in 3 batches with unique primary keys using a loop
        insert_times = 3
        random_vectors = list(cf.gen_vectors(default_nb * insert_times, default_dim, vector_data_type=vector_dtype)) \
            if vector_dtype == DataType.FLOAT_VECTOR \
            else cf.gen_vectors(default_nb * insert_times, default_dim, vector_data_type=vector_dtype)
        for j in range(insert_times):
            start_pk = j * default_nb
            rows = [{
                default_primary_key_field_name: i + start_pk,
                default_vector_field_name: random_vectors[i + start_pk]
            } for i in range(default_nb)]
            self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # build index
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(default_vector_field_name, index_type=index,
                               metric_type=metric_type,
                               params=cf.get_index_params_params(index_type=index))
        self.create_index(client, collection_name, index_params=index_params)

        # load the collection with index
        assert self.wait_for_index_ready(client, collection_name, default_vector_field_name, timeout=120)
        self.load_collection(client, collection_name)

        # search and assert
        limit = 50
        pages = 5
        expected_overlap_ratio = 20
        self.do_search_pagination_and_assert(client, collection_name, limit=limit, pages=pages, dim=default_dim,
                                             vector_dtype=vector_dtype, index=index, metric_type=metric_type,
                                             expected_overlap_ratio=expected_overlap_ratio)

        # insert additional data without flush
        random_vectors = list(cf.gen_vectors(default_nb, default_dim, vector_data_type=vector_dtype)) \
            if vector_dtype == DataType.FLOAT_VECTOR \
            else cf.gen_vectors(default_nb, default_dim, vector_data_type=vector_dtype)
        start_pk = default_nb * insert_times
        rows = [{
            default_primary_key_field_name: i + start_pk,
            default_vector_field_name: random_vectors[i]
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)

        # search and assert
        self.do_search_pagination_and_assert(client, collection_name, limit=limit, pages=pages, dim=default_dim,
                                             vector_dtype=vector_dtype, index=index, metric_type=metric_type,
                                             expected_overlap_ratio=expected_overlap_ratio)
