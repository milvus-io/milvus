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

        # Define number of insert iterations
        insert_times = 10
        
        # Generate vectors for each type and store in self
        float_vectors = cf.gen_vectors(default_nb * insert_times, dim=self.float_vector_dim, vector_data_type='FLOAT_VECTOR')
        bfloat16_vectors = cf.gen_vectors(default_nb * insert_times, dim=self.bf16_vector_dim, vector_data_type='BFLOAT16_VECTOR')
        sparse_vectors = cf.gen_sparse_vectors(default_nb * insert_times, empty_percentage=2)
        _, binary_vectors = cf.gen_binary_vectors(default_nb * insert_times, dim=self.binary_vector_dim)

        # Insert data multiple times with non-duplicated primary keys
        for j in range(insert_times):
            rows = [{
                default_primary_key_field_name: i + j * default_nb,
                self.float_vector_field_name: list(float_vectors[i + j * default_nb]),
                self.bfloat16_vector_field_name: bfloat16_vectors[i + j * default_nb],
                self.sparse_vector_field_name: sparse_vectors[i + j * default_nb],
                self.binary_vector_field_name: binary_vectors[i + j * default_nb],
                default_float_field_name: (i + j * default_nb) * 1.0,
                default_string_field_name: str(i + j * default_nb),
                default_int64_field_name: i + j * default_nb
            }
                for i in range(default_nb)]
            self.datas.extend(rows)
            self.primary_keys.extend([i + j * default_nb for i in range(default_nb)])
            self.insert(client, self.collection_name, data=rows)
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
        vectors_to_search = cf.gen_vectors(default_nq, default_dim)
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
        vectors_to_search = cf.gen_vectors(default_nq, self.bf16_vector_dim, vector_data_type='BFLOAT16_VECTOR')
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
                # Calculate percentage of matching items
                matching_items = sum(1 for x, y in zip(page_ids, ids_in_full) if x == y)
                match_percentage = (matching_items / len(page_ids)) * 100
                assert match_percentage >= 80, f"Only {match_percentage}% items matched, expected >= 80%"
    
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
        vectors_to_search = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
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

            # 3. search with offset+limit
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

            # 4. Compare results
            filter_ids_set = set(filter_ids)
            for hits in search_res_with_offset:
                ids = [hit.get('id') for hit in hits]
                assert set(ids).issubset(filter_ids_set)
            
            # Compare pagination results with full results
            page_ids = [search_res_with_offset[0][j].get('id') for j in range(limit)]
            ids_in_full = [search_res_full[0][offset:offset + limit][j].get('id') for j in range(limit)]
            assert page_ids == ids_in_full

            # 5. search again with expression template
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

            # 6. search with offset+limit
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

            # Compare results
            filter_ids_set = set(filter_ids)
            for hits in search_res_with_offset:
                ids = [hit.get('id') for hit in hits]
                assert set(ids).issubset(filter_ids_set)
            
            # Compare pagination results with full results
            page_ids = [search_res_with_offset[0][j].get('id') for j in range(limit)]
            ids_in_full = [search_res_full[0][offset:offset + limit][j].get('id') for j in range(limit)]
            assert page_ids == ids_in_full


class TestSearchPagination(TestcaseBase):
    """ Test case of search pagination """

    @pytest.fixture(scope="function", params=[0, 10, 100])
    def offset(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def auto_id(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def _async(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def enable_dynamic_field(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["FLOAT_VECTOR", "FLOAT16_VECTOR", "BFLOAT16_VECTOR"])
    def vector_data_type(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_pagination_with_index_partition(self, offset, _async):
        """
        target: test search pagination with index and partition
        method: create connection, collection, insert data, create index and search
        expected: searched successfully
        """
        # 1. initialize with data
        auto_id = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      partition_num=1,
                                                                      auto_id=auto_id,
                                                                      is_index=False)[0:4]
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        # 2. create index
        default_index = {"index_type": "IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search through partitions
        par = collection_w.partitions
        limit = 100
        search_params = {"metric_type": "L2",
                         "params": {"nprobe": 10}, "offset": offset}
        search_res = collection_w.search(vectors[:default_nq], default_search_field,
                                         search_params, limit, default_search_exp,
                                         [par[0].name, par[1].name], _async=_async,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": default_nq,
                                                      "ids": insert_ids,
                                                      "limit": limit,
                                                      "_async": _async})[0]
        # 3. search through partitions with offset+limit
        search_params = {"metric_type": "L2"}
        res = collection_w.search(vectors[:default_nq], default_search_field, search_params,
                                  limit + offset, default_search_exp,
                                  [par[0].name, par[1].name], _async=_async)[0]
        if _async:
            search_res.done()
            search_res = search_res.result()
            res.done()
            res = res.result()
        res_distance = res[0].distances[offset:]
        # assert cf.sort_search_distance(search_res[0].distances) == cf.sort_search_distance(res_distance)
        assert set(search_res[0].ids) == set(res[0].ids[offset:])

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_pagination_with_inserted_data(self, offset, _async):
        """
        target: test search pagination with inserted data
        method: create connection, collection, insert data and search
                check the results by searching with limit+offset
        expected: searched successfully
        """
        # 1. create collection
        collection_w = self.init_collection_general(
            prefix, False, dim=default_dim)[0]
        # 2. insert data
        data = cf.gen_default_dataframe_data(dim=default_dim)
        collection_w.insert(data)
        collection_w.load()
        # 3. search
        search_params = {"offset": offset}
        search_res = collection_w.search(vectors[:default_nq], default_search_field,
                                         search_params, default_limit,
                                         default_search_exp, _async=_async,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": default_nq,
                                                      "limit": default_limit,
                                                      "_async": _async})[0]
        # 4. search through partitions with offset+limit
        search_params = {}
        res = collection_w.search(vectors[:default_nq], default_search_field, search_params,
                                  default_limit + offset, default_search_exp, _async=_async)[0]
        if _async:
            search_res.done()
            search_res = search_res.result()
            res.done()
            res = res.result()
        res_distance = res[0].distances[offset:]
        assert sorted(search_res[0].distances) == sorted(res_distance)
        assert set(search_res[0].ids) == set(res[0].ids[offset:])

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_pagination_empty(self, offset, _async):
        """
        target: test search pagination empty
        method: connect, create collection, insert data and search
        expected: search successfully
        """
        # 1. initialize without data
        auto_id = False
        collection_w = self.init_collection_general(
            prefix, True, auto_id=auto_id, dim=default_dim)[0]
        # 2. search collection without data
        search_param = {"metric_type": "COSINE",
                        "params": {"nprobe": 10}, "offset": offset}
        search_res = collection_w.search([], default_search_field, search_param,
                                         default_limit, default_search_exp, _async=_async,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": 0,
                                                      "_async": _async})[0]
        if _async:
            search_res.done()
            search_res = search_res.result()
        assert len(search_res) == 0

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [3000, 5000])
    def test_search_pagination_with_offset_over_num_entities(self, offset):
        """
        target: test search pagination with offset over num_entities
        method: create connection, collection, insert 3000 entities and search with offset over 3000
        expected: return an empty list
        """
        # 1. initialize
        collection_w = self.init_collection_general(
            prefix, True, dim=default_dim)[0]
        # 2. search
        search_param = {"metric_type": "COSINE",
                        "params": {"nprobe": 10}, "offset": offset}
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        res = collection_w.search(vectors[:default_nq], default_search_field,
                                  search_param, default_limit,
                                  default_search_exp,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": default_nq,
                                               "limit": 0})[0]
        assert res[0].ids == []

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_pagination_after_different_index(self, index, offset, _async):
        """
        target: test search pagination after different index
        method: test search pagination after different index and corresponding search params
        expected: search successfully
        """
        # 1. initialize with data
        dim = 128
        auto_id = True
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 1000,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim, is_index=False)[0:5]
        # 2. create index and load
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search
        search_params = cf.gen_search_param(index)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            res = collection_w.search(vectors[:default_nq], default_search_field, search_param,
                                      default_limit + offset, default_search_exp, _async=_async)[0]
            search_param["offset"] = offset
            log.info("Searching with search params: {}".format(search_param))
            search_res = collection_w.search(vectors[:default_nq], default_search_field,
                                             search_param, default_limit,
                                             default_search_exp, _async=_async,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": default_nq,
                                                          "ids": insert_ids,
                                                          "limit": default_limit,
                                                          "_async": _async})[0]
            if _async:
                search_res.done()
                search_res = search_res.result()
                res.done()
                res = res.result()
            res_distance = res[0].distances[offset:]
            # assert sorted(search_res[0].distances, key=numpy.float32) == sorted(res_distance, key=numpy.float32)
            assert set(search_res[0].ids) == set(res[0].ids[offset:])

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [100, default_nb // 2])
    def test_search_offset_different_position(self, offset):
        """
        target: test search pagination with offset in different position
        method: create connection, collection, insert entities and search with offset
        expected: search successfully
        """
        # 1. initialize
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. search with offset in params
        search_params = {"metric_type": "COSINE",
                         "params": {"nprobe": 10}, "offset": offset}
        res1 = collection_w.search(vectors[:default_nq], default_search_field,
                                   search_params, default_limit)[0]

        # 3. search with offset outside params
        res2 = collection_w.search(vectors[:default_nq], default_search_field, default_search_params,
                                   default_limit, offset=offset)[0]
        assert res1[0].ids == res2[0].ids

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [1, 5, 20])
    def test_search_sparse_with_pagination(self, offset):
        """
        target: test search sparse with pagination
        method: 1. connect and create a collection
                2. search pagination with offset
                3. search with offset+limit
                4. compare with the search results whose corresponding ids should be the same
        expected: search successfully and ids is correct
        """
        # 1. create a collection
        auto_id = False
        collection_w, _, _, insert_ids = \
            self.init_collection_general(
                prefix, True, auto_id=auto_id, vector_data_type=ct.sparse_vector)[0:4]
        # 2. search with offset+limit
        search_param = {"metric_type": "IP", "params": {"drop_ratio_search": "0.2"}, "offset": offset}
        search_vectors = cf.gen_default_list_sparse_data()[-1][-2:]
        search_res = collection_w.search(search_vectors, ct.default_sparse_vec_field_name,
                                         search_param, default_limit)[0]
        # 3. search
        _search_param = {"metric_type": "IP", "params": {"drop_ratio_search": "0.2"}}
        res = collection_w.search(search_vectors[:default_nq], ct.default_sparse_vec_field_name, _search_param,
                                  default_limit + offset)[0]
        assert len(search_res[0].ids) == len(res[0].ids[offset:])
        assert sorted(search_res[0].distances, key=np.float32) == sorted(
            res[0].distances[offset:], key=np.float32)


class TestSearchPaginationInvalid(TestMilvusClientV2Base):
    """ Test case of search pagination """
    """
    ******************************************************************
    #  The following are invalid cases
    ******************************************************************
    """
   
    @pytest.mark.tags(CaseLabel.L1)
    def test_search_pagination_with_invalid_offset_type(self):
        """
        target: test search pagination with invalid offset type
        method: create connection, collection, insert and search with invalid offset type
        expected: raise exception
        """
        client = self._client()

        # 1. Create collection with schema
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)

        # Insert data
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Search with invalid offset types
        vectors_to_search = cf.gen_vectors(default_nq, default_dim)
        invalid_offsets = [" ", [1, 2], {1}, "12 s"]

        for offset in invalid_offsets:
            log.debug(f"assert search error if offset={offset}")
            search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}, "offset": offset}
            self.search(
                client,
                collection_name,
                vectors_to_search[:default_nq],
                anns_field=default_vector_field_name,
                search_params=search_params,
                limit=default_limit,
                check_task=CheckTasks.err_res,
                check_items={
                    "err_code": 1,
                    "err_msg": "wrong type for offset, expect int"
                }
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_pagination_with_invalid_offset_value(self):
        """
        target: test search pagination with invalid offset value
        method: create connection, collection, insert and search with invalid offset value
        expected: raise exception
        """
        client = self._client()

        # 1. Create collection with schema
        collection_name = cf.gen_collection_name_by_testcase_name() 
        self.create_collection(client, collection_name, default_dim)

        # Insert data
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(cf.gen_vectors(1, default_dim)[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Search with invalid offset values
        vectors_to_search = cf.gen_vectors(default_nq, default_dim)
        invalid_offsets = [-1, 16385]

        for offset in invalid_offsets:
            log.debug(f"assert search error if offset={offset}")
            search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}, "offset": offset}
            self.search(
                client,
                collection_name,
                vectors_to_search[:default_nq],
                anns_field=default_vector_field_name,
                search_params=search_params,
                limit=default_limit,
                check_task=CheckTasks.err_res,
                check_items={
                    "err_code": 1,
                    "err_msg": f"offset [{offset}] is invalid, it should be in range [1, 16384]"
                }
            )