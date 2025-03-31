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
range_search_supported_indexes = ct.all_index_types[:7]
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


class TestSearchBase(TestcaseBase):
    @pytest.fixture(
        scope="function",
        params=[1, 10]
    )
    def get_top_k(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=[1, 10, 1100]
    )
    def get_nq(self, request):
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_flat_top_k(self, get_nq):
        """
        target: test basic search function, all the search params is correct, change top-k value
        method: search with the given vectors, check the result
        expected: the length of the result is top_k
        """
        top_k = 16385  # max top k is 16384
        nq = get_nq
        collection_w, data, _, insert_ids = self.init_collection_general(prefix, insert_data=True, nb=nq)[0:4]
        collection_w.load()
        if top_k <= max_top_k:
            res, _ = collection_w.search(vectors[:nq], default_search_field, default_search_params, top_k)
            assert len(res[0]) <= top_k
        else:
            collection_w.search(vectors[:nq], default_search_field, default_search_params, top_k,
                                check_task=CheckTasks.err_res,
                                check_items={"err_code": 65535,
                                             "err_msg": f"topk [{top_k}] is invalid, it should be in range"
                                                        f" [1, 16384], but got {top_k}"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_index_empty_partition(self, index):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: add vectors into collection, search with the given vectors, check the result
        expected: the length of the result is top_k, search collection with partition tag return empty
        """
        top_k = ct.default_top_k
        nq = ct.default_nq
        dim = ct.default_dim
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nq,
                                                                                  partition_num=1,
                                                                                  dim=dim, is_index=False)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create partition
        partition_name = "search_partition_empty"
        collection_w.create_partition(partition_name=partition_name, description="search partition empty")
        par = collection_w.partitions
        # collection_w.load()
        # 3. create different index
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()

        # 4. search
        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     default_search_params, top_k,
                                     default_search_exp)

        assert len(res[0]) <= top_k

        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, top_k,
                            default_search_exp, [partition_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_index_partitions(self, index, get_top_k):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: search collection with the given vectors and tags, check the result
        expected: the length of the result is top_k
        """
        top_k = get_top_k
        nq = ct.default_nq
        dim = ct.default_dim
        # 1. initialize with data in 2 partitions
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True,
                                                                                  partition_num=1,
                                                                                  dim=dim, is_index=False)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create different index
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        collection_w.create_index("float_vector", default_index)

        # 3. load and search
        collection_w.load()
        par = collection_w.partitions
        collection_w.search(vectors[:nq], default_search_field,
                            ct.default_search_params, top_k,
                            default_search_exp, [par[0].name, par[1].name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "limit": top_k,
                                         "ids": insert_ids})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_ip_flat(self, get_top_k):
        """
        target: test basic search function, all the search params are correct, change top-k value
        method: search with the given vectors, check the result
        expected: the length of the result is top_k
        """
        top_k = get_top_k
        nq = ct.default_nq
        dim = ct.default_dim
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nq,
                                                                                  dim=dim, is_index=False)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create ip index
        default_index = {"index_type": "IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        search_params = {"metric_type": "IP", "params": {"nprobe": 10}}
        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     search_params, top_k,
                                     default_search_exp)
        assert len(res[0]) <= top_k

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_ip_after_index(self, index):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: search with the given vectors, check the result
        expected: the length of the result is top_k
        """
        top_k = ct.default_top_k
        nq = ct.default_nq
        dim = ct.default_dim

        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nq,
                                                                                  dim=dim, is_index=False)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create ip index
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        search_params = {"metric_type": "IP", "params": {"nprobe": 10}}
        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     search_params, top_k,
                                     default_search_exp)
        assert len(res[0]) <= top_k

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [2, 128, 768])
    @pytest.mark.parametrize("nb", [1, 2, 10, 100])
    def test_search_ip_brute_force(self, nb, dim):
        """
        target: https://github.com/milvus-io/milvus/issues/17378. Ensure the logic of IP distances won't be changed.
        method: search with the given vectors, check the result
        expected: The inner product of vector themselves should be positive.
        """
        top_k = 1

        # 1. initialize with data
        collection_w, insert_entities, _, insert_ids, _ = \
            self.init_collection_general(prefix, True, nb, is_binary=False,
                                         is_index=False, dim=dim)[0:5]
        flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "IP"}
        collection_w.create_index(ct.default_float_vec_field_name, flat_index)
        insert_vectors = insert_entities[0][default_search_field].tolist()

        # 2. load collection.
        collection_w.load()

        # 3. search and then check if the distances are expected.
        res, _ = collection_w.search(insert_vectors[:nb], default_search_field,
                                     ct.default_search_ip_params, top_k,
                                     default_search_exp)
        for i, v in enumerate(insert_vectors):
            assert len(res[i]) == 1
            ref = ip(v, v)
            got = res[i][0].distance
            assert abs(got - ref) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_ip_index_empty_partition(self, index):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: add vectors into collection, search with the given vectors, check the result
        expected: the length of the result is top_k, search collection with partition tag return empty
        """
        top_k = ct.default_top_k
        nq = ct.default_nq
        dim = ct.default_dim
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nq,
                                                                                  partition_num=1,
                                                                                  dim=dim, is_index=False)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create partition
        partition_name = "search_partition_empty"
        collection_w.create_partition(partition_name=partition_name, description="search partition empty")
        par = collection_w.partitions
        # 3. create different index
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()

        # 4. search
        search_params = {"metric_type": "IP", "params": {"nprobe": 10}}
        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     search_params, top_k,
                                     default_search_exp)

        assert len(res[0]) <= top_k

        collection_w.search(vectors[:nq], default_search_field,
                            search_params, top_k,
                            default_search_exp, [partition_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_ip_index_partitions(self, index):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: search collection with the given vectors and tags, check the result
        expected: the length of the result is top_k
        """
        top_k = ct.default_top_k
        nq = ct.default_nq
        dim = ct.default_dim
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nq,
                                                                                  partition_num=1,
                                                                                  dim=dim, is_index=False)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create partition
        par_name = collection_w.partitions[0].name
        # 3. create different index
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()

        # 4. search
        search_params = {"metric_type": "IP", "params": {"nprobe": 10}}
        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     search_params, top_k,
                                     default_search_exp, [par_name])

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_cosine_all_indexes(self, index):
        """
        target: test basic search function, all the search params are correct, test all index params, and build
        method: search collection with the given vectors and tags, check the result
        expected: the length of the result is top_k
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True,
                                                                                  is_index=False)[0:5]
        # 2. create index
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()

        # 3. search
        search_params = {"metric_type": "COSINE"}
        res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                     search_params, default_limit, default_search_exp,
                                     check_task=CheckTasks.check_search_results,
                                     check_items={"nq": default_nq,
                                                  "ids": insert_ids,
                                                  "limit": default_limit})

        # 4. check cosine distance
        for i in range(default_nq):
            for distance in res[i].distances:
                assert 1 >= distance >= -1

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_cosine_results_same_as_l2(self):
        """
        target: test search results of l2 and cosine keep the same
        method: 1. search L2
                2. search cosine
                3. compare the results
        expected: raise no exception
        """
        nb = ct.default_nb
        # 1. prepare original data and normalized data
        original_vec = [[random.random() for _ in range(ct.default_dim)] for _ in range(nb)]
        normalize_vec = preprocessing.normalize(original_vec, axis=1, norm='l2')
        normalize_vec = normalize_vec.tolist()
        data = cf.gen_default_dataframe_data()

        # 2. create L2 collection and insert normalized data
        collection_w1 = self.init_collection_general(prefix, is_index=False)[0]
        data[ct.default_float_vec_field_name] = normalize_vec
        collection_w1.insert(data)

        # 2. create index L2
        default_index = {"index_type": "IVF_SQ8", "params": {"nlist": 64}, "metric_type": "L2"}
        collection_w1.create_index("float_vector", default_index)
        collection_w1.load()

        # 3. search L2
        search_params = {"params": {"nprobe": 10}, "metric_type": "L2"}
        res_l2, _ = collection_w1.search(vectors[:default_nq], default_search_field,
                                         search_params, default_limit, default_search_exp)

        # 4. create cosine collection and insert original data
        collection_w2 = self.init_collection_general(prefix, is_index=False)[0]
        data[ct.default_float_vec_field_name] = original_vec
        collection_w2.insert(data)

        # 5. create index cosine
        default_index = {"index_type": "IVF_SQ8", "params": {"nlist": 64}, "metric_type": "COSINE"}
        collection_w2.create_index("float_vector", default_index)
        collection_w2.load()

        # 6. search cosine
        search_params = {"params": {"nprobe": 10}, "metric_type": "COSINE"}
        res_cosine, _ = collection_w2.search(vectors[:default_nq], default_search_field,
                                             search_params, default_limit, default_search_exp)

        # 7. check the search results
        for i in range(default_nq):
            assert res_l2[i].ids == res_cosine[i].ids

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_cosine_results_same_as_ip(self):
        """
        target: test search results of ip and cosine keep the same
        method: 1. search IP
                2. search cosine
                3. compare the results
        expected: raise no exception
        """
        # 1. create collection and insert data
        collection_w = self.init_collection_general(prefix, True, is_index=False)[0]

        # 2. search IP
        default_index = {"index_type": "IVF_SQ8", "params": {"nlist": 64}, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        search_params = {"params": {"nprobe": 10}, "metric_type": "IP"}
        res_ip, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                        search_params, default_limit, default_search_exp)

        # 3. search cosine
        collection_w.release()
        collection_w.drop_index()
        default_index = {"index_type": "IVF_SQ8", "params": {"nlist": 64}, "metric_type": "COSINE"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        search_params = {"params": {"nprobe": 10}, "metric_type": "COSINE"}
        res_cosine, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            search_params, default_limit, default_search_exp)

        # 4. check the search results
        for i in range(default_nq):
            assert res_ip[i].ids == res_cosine[i].ids
            log.info(res_cosine[i].distances)
            log.info(res_ip[i].distances)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_without_connect(self):
        """
        target: test search vectors without connection
        method: use disconnected instance, call search method and check if search successfully
        expected: raise exception
        """
        self._connect()

        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True,
                                                                                  ct.default_nq)[0:5]
        vectors = [[random.random() for _ in range(ct.default_dim)]
                   for _ in range(nq)]

        collection_w.load()
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list

        res, _ = collection_w.search(vectors[:nq], default_search_field,
                                     ct.default_search_params, ct.default_top_k,
                                     default_search_exp,
                                     check_task=CheckTasks.err_res,
                                     check_items={"err_code": 1,
                                                  "err_msg": "should create connection first"})

    @pytest.mark.tags(CaseLabel.L2)
    # @pytest.mark.timeout(300)
    def test_search_concurrent_multithreads_single_connection(self, _async):
        """
        target: test concurrent search with multi processes
        method: search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        """
        threads_num = 10
        threads = []
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(
            prefix, True, ct.default_nb)[0:5]

        def search(collection_w):
            vectors = [[random.random() for _ in range(ct.default_dim)]
                       for _ in range(nq)]
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

        # 2. search with multi-processes
        log.info("test_search_concurrent_multithreads_single_connection: searching with %s processes" % threads_num)
        for i in range(threads_num):
            t = threading.Thread(target=search, args=(collection_w,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_multi_collections(self):
        """
        target: test search multi collections of L2
        method: add vectors into 10 collections, and search
        expected: search status ok, the length of result
        """
        num = 10
        top_k = 10
        nq = 20

        for i in range(num):
            collection = gen_unique_str(uid + str(i))
            collection_w, _, _, insert_ids, time_stamp = \
                self.init_collection_general(
                    collection, True, ct.default_nb)[0:5]
            assert len(insert_ids) == default_nb
            vectors = [[random.random() for _ in range(ct.default_dim)]
                       for _ in range(nq)]
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, top_k,
                                default_search_exp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": top_k})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[:6])
    def test_each_index_with_mmap_enabled_search(self, index):
        """
        target: test each index with mmap enabled search
        method: test each index with mmap enabled search
        expected: search success
        """
        self._connect()
        nb = 2000
        dim = 32
        collection_w = self.init_collection_general(prefix, True, nb, dim=dim, is_index=False)[0]
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index(field_name, default_index, index_name="mmap_index")
        # mmap index
        collection_w.alter_index("mmap_index", {'mmap.enabled': True})
        # search
        collection_w.load()
        search_params = cf.gen_search_param(index)[0]
        vector = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        collection_w.search(vector, default_search_field, search_params, ct.default_limit,
                            output_fields=["*"],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": ct.default_limit})
        # enable mmap
        collection_w.release()
        collection_w.alter_index("mmap_index", {'mmap.enabled': False})
        collection_w.load()
        collection_w.search(vector, default_search_field, search_params, ct.default_limit,
                            output_fields=["*"],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": ct.default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[7:9])
    def test_enable_mmap_search_for_binary_indexes(self, index):
        """
        target: enable mmap for binary indexes
        method: enable mmap for binary indexes
        expected: search success
        """
        self._connect()
        dim = 64
        nb = 2000
        collection_w = self.init_collection_general(prefix, True, nb, dim=dim, is_index=False, is_binary=True)[0]
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index,
                         "params": params, "metric_type": "JACCARD"}
        collection_w.create_index(ct.default_binary_vec_field_name, default_index, index_name="binary_idx_name")
        collection_w.alter_index("binary_idx_name", {'mmap.enabled': True})
        collection_w.set_properties({'mmap.enabled': True})
        collection_w.load()
        pro = collection_w.describe()[0].get("properties")
        assert pro["mmap.enabled"] == 'True'
        assert collection_w.index()[0].params["mmap.enabled"] == 'True'
        # search
        binary_vectors = cf.gen_binary_vectors(default_nq, dim)[1]
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        output_fields = ["*"]
        collection_w.search(binary_vectors, ct.default_binary_vec_field_name, search_params,
                            default_limit, default_search_string_exp, output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit})


class TestCollectionSearch(TestcaseBase):
    """ Test case of search interface """

    @pytest.fixture(scope="function", params=[default_nb_medium])
    def nb(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[200])
    def nq(self, request):
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

    @pytest.fixture(scope="function", params=["FLOAT_VECTOR", "FLOAT16_VECTOR", "BFLOAT16_VECTOR"])
    def vector_data_type(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=["STL_SORT", "INVERTED"])
    def scalar_index(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[0, 0.5, 1])
    def null_data_percent(self, request):
        yield request.param

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_normal(self, nq, dim, auto_id, is_flush, enable_dynamic_field, vector_data_type):
        """
        target: test search normal case
        method: create connection, collection, insert and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_flush=is_flush,
                                         enable_dynamic_field=enable_dynamic_field,
                                         vector_data_type=vector_data_type)[0:5]
        # 2. generate search data
        vectors = cf.gen_vectors_based_on_vector_type(nq, dim, vector_data_type)
        # 3. search after insert
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            guarantee_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_normal_without_specify_metric_type(self):
        """
        target: test search without specify metric type
        method: create connection, collection, insert and search
        expected: 1. search successfully with limit(topK)
        """
        nq = 2
        dim = 32
        auto_id = True
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(
                prefix, True, auto_id=auto_id, dim=dim, is_flush=True)[0:5]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        search_params = {"params": {"nprobe": 10}}
        # 2. search after insert
        collection_w.search(vectors[:nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp,
                            guarantee_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_normal_without_specify_anns_field(self):
        """
        target: test search normal case
        method: create connection, collection, insert and search
        expected: 1. search successfully with limit(topK)
        """
        nq = 2
        dim = 32
        auto_id = True
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(
                prefix, True, auto_id=auto_id, dim=dim, is_flush=True)[0:5]
        # 2. search after insert
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], "",
                            default_search_params, default_limit,
                            default_search_exp,
                            guarantee_timestamp=0,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_hit_vectors(self, nq):
        """
        target: test search with vectors in collections
        method: create connections,collection insert and search vectors in collections
        expected: search successfully with limit(topK) and can be hit at top 1 (min distance is 0)
        """
        dim = 64
        auto_id = False
        enable_dynamic_field = True
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim,
                                         enable_dynamic_field=enable_dynamic_field)[0:4]
        # get vectors that inserted into collection
        vectors = []
        if enable_dynamic_field:
            for vector in _vectors[0]:
                vector = vector[ct.default_float_vec_field_name]
                vectors.append(vector)
        else:
            vectors = np.array(_vectors[0]).tolist()
            vectors = [vectors[i][-1] for i in range(nq)]
        log.info("test_search_with_hit_vectors: searching collection %s" %
                 collection_w.name)
        search_res, _ = collection_w.search(vectors[:nq], default_search_field,
                                            default_search_params, default_limit,
                                            default_search_exp,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": nq,
                                                         "ids": insert_ids,
                                                         "limit": default_limit})
        log.info("test_search_with_hit_vectors: checking the distance of top 1")
        for hits in search_res:
            # verify that top 1 hit is itself,so min distance is 0
            assert 1.0 - hits.distances[0] <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_multi_vector_fields(self, nq, is_flush, vector_data_type):
        """
        target: test search normal case
        method: create connection, collection, insert and search
        expected: 1. search successfully with limit(topK)
        """
        # 1. initialize with data
        dim = 64
        auto_id = True
        enable_dynamic_field = False
        multiple_dim_array = [dim, dim]
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_flush=is_flush,
                                         enable_dynamic_field=enable_dynamic_field,
                                         multiple_dim_array=multiple_dim_array,
                                         vector_data_type=vector_data_type,
                                         nullable_fields={ct.default_string_field_name: 1},
                                         default_value_fields={ct.default_float_field_name: np.float32(10.0)})[0:5]
        # 2. generate search data
        vectors = cf.gen_vectors_based_on_vector_type(nq, dim, vector_data_type)
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(default_search_field)
        # 3. search after insert
        for search_field in vector_name_list:
            collection_w.search(vectors[:nq], search_field,
                                default_search_params, default_limit,
                                default_search_exp,
                                output_fields=[ct.default_float_field_name, ct.default_string_field_name],
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "output_fields": [ct.default_float_field_name,
                                                               ct.default_string_field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_random_primary_key(self, random_primary_key):
        """
        target: test search for collection with random primary keys
        method: create connection, collection, insert and search
        expected: Search without errors and data consistency
        """
        # 1. initialize collection with random primary key
        collection_w, _vectors, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, 10, random_primary_key=random_primary_key,
                                         language="Russian")[0:5]
        # 2. search
        log.info("test_search_random_primary_key: searching collection %s" % collection_w.name)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp,
                            output_fields=[default_int64_field_name,
                                           default_float_field_name,
                                           default_json_field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": 10,
                                         "original_entities": _vectors,
                                         "output_fields": [default_int64_field_name,
                                                           default_float_field_name,
                                                           default_json_field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dup_times", [1, 2, 3])
    def test_search_with_dup_primary_key(self, _async, dup_times):
        """
        target: test search with duplicate primary key
        method: 1.insert same data twice
                2.search
        expected: search results are de-duplicated
        """
        # initialize with data
        nb = ct.default_nb
        nq = ct.default_nq
        dim = 128
        auto_id = True
        collection_w, insert_data, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                                auto_id=auto_id,
                                                                                dim=dim)[0:4]
        # insert dup data multi times
        for i in range(dup_times):
            insert_res, _ = collection_w.insert(insert_data[0])
            insert_ids.extend(insert_res.primary_keys)
        # search
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        search_res, _ = collection_w.search(vectors[:nq], default_search_field,
                                            default_search_params, default_limit,
                                            default_search_exp, _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": nq,
                                                         "ids": insert_ids,
                                                         "limit": default_limit,
                                                         "_async": _async})
        if _async:
            search_res.done()
            search_res = search_res.result()
        # assert that search results are de-duplicated
        for hits in search_res:
            ids = hits.ids
            assert sorted(list(set(ids))) == sorted(ids)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("search_params", [{}, {"metric_type": "COSINE"}])
    def test_search_with_default_search_params(self, _async, search_params):
        """
        target: test search with default search params
        method: search with default search params
        expected: search successfully
        """
        # initialize with data
        collection_w, insert_data, _, insert_ids = self.init_collection_general(prefix, True)[0:4]
        # search
        collection_w.search(vectors[:nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_accurate_search_with_multi_segments(self):
        """
        target: search collection with multi segments accurately
        method: insert and flush twice
        expect: result pk should be [19,9,18]
        """
        # 1. create a collection, insert data and flush
        nb = 10
        dim = 64
        collection_w = self.init_collection_general(
            prefix, True, nb, dim=dim, is_index=False)[0]

        # 2. insert data and flush again for two segments
        data = cf.gen_default_dataframe_data(nb=nb, dim=dim, start=nb)
        collection_w.insert(data)
        collection_w.flush()

        # 3. create index and load
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()

        # 4. get inserted original data
        inserted_vectors = collection_w.query(expr="int64 >= 0", output_fields=[
            ct.default_float_vec_field_name])
        original_vectors = []
        for single in inserted_vectors[0]:
            single_vector = single[ct.default_float_vec_field_name]
            original_vectors.append(single_vector)

        # 5. Calculate the searched ids
        limit = 2 * nb
        vectors = [[random.random() for _ in range(dim)] for _ in range(1)]
        distances = []
        for original_vector in original_vectors:
            distance = cf.cosine(vectors, original_vector)
            distances.append(distance)
        distances_max = heapq.nlargest(limit, distances)
        distances_index_max = map(distances.index, distances_max)

        # 6. search
        collection_w.search(vectors, default_search_field,
                            default_search_params, limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={
                                "nq": 1,
                                "limit": limit,
                                "ids": list(distances_index_max)
                            })

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_empty_vectors(self, _async):
        """
        target: test search with empty query vector
        method: search using empty query vector
        expected: search successfully with 0 results
        """
        # 1. initialize without data
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w = self.init_collection_general(prefix, True,
                                                    auto_id=auto_id, dim=dim,
                                                    enable_dynamic_field=enable_dynamic_field)[0]
        # 2. search collection without data
        log.info("test_search_with_empty_vectors: Searching collection %s "
                 "using empty vector" % collection_w.name)
        collection_w.search([], default_search_field, default_search_params,
                            default_limit, default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 0,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_ndarray(self, _async):
        """
        target: test search with ndarray
        method: search using ndarray data
        expected: search successfully
        """
        # 1. initialize without data
        dim = 64
        auto_id = True
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      auto_id=auto_id,
                                                                      dim=dim,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search collection without data
        log.info("test_search_with_ndarray: Searching collection %s "
                 "using ndarray" % collection_w.name)
        vectors = np.random.randn(default_nq, dim)
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("search_params", [{}, {"params": {}}, {"params": {"nprobe": 10}}])
    def test_search_normal_default_params(self, search_params, _async):
        """
        target: test search normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim,
                                         enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. rename collection
        new_collection_name = cf.gen_unique_str(prefix + "new")
        self.utility_wrap.rename_collection(
            collection_w.name, new_collection_name)
        collection_w = self.init_collection_general(auto_id=auto_id, dim=dim, name=new_collection_name,
                                                    enable_dynamic_field=enable_dynamic_field)[0]
        # 3. search
        log.info("test_search_normal_default_params: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="partition load and release constraints")
    def test_search_before_after_delete(self, nq, _async):
        """
        target: test search function before and after deletion
        method: 1. search the collection
                2. delete a partition
                3. search the collection
        expected: the deleted entities should not be searched
        """
        # 1. initialize with data
        dim = 64
        auto_id = False
        nb = 1000
        limit = 1000
        partition_num = 1
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      partition_num,
                                                                      auto_id=auto_id, dim=dim)[0:4]
        # 2. search all the partitions before partition deletion
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        log.info(
            "test_search_before_after_delete: searching before deleting partitions")
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": limit,
                                         "_async": _async})
        # 3. delete partitions
        log.info("test_search_before_after_delete: deleting a partition")
        par = collection_w.partitions
        deleted_entity_num = par[partition_num].num_entities
        print(deleted_entity_num)
        entity_num = nb - deleted_entity_num
        collection_w.drop_partition(par[partition_num].name)
        log.info("test_search_before_after_delete: deleted a partition")
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        # 4. search non-deleted part after delete partitions
        log.info(
            "test_search_before_after_delete: searching after deleting partitions")
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids[:entity_num],
                                         "limit": limit - deleted_entity_num,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_collection_after_release_load(self, nq, _async):
        """
        target: search the pre-released collection after load
        method: 1. create collection
                2. release collection
                3. load collection
                4. search the pre-released collection
        expected: search successfully
        """
        # 1. initialize without data
        nb = 2000
        dim = 64
        auto_id = True
        enable_dynamic_field = True
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, nb, 1, auto_id=auto_id, dim=dim,
                                         enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. release collection
        log.info("test_search_collection_after_release_load: releasing collection %s" %
                 collection_w.name)
        collection_w.release()
        log.info("test_search_collection_after_release_load: released collection %s" %
                 collection_w.name)
        # 3. Search the pre-released collection after load
        log.info("test_search_collection_after_release_load: loading collection %s" %
                 collection_w.name)
        collection_w.load()
        log.info("test_search_collection_after_release_load: searching after load")
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_load_flush_load(self, nq, _async):
        """
        target: test search when load before flush
        method: 1. insert data and load
                2. flush, and load
                3. search the collection
        expected: search success with limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w = self.init_collection_general(prefix, auto_id=auto_id, dim=dim,
                                                    enable_dynamic_field=enable_dynamic_field)[0]
        # 2. insert data
        insert_ids = cf.insert_data(collection_w, nb, auto_id=auto_id, dim=dim,
                                    enable_dynamic_field=enable_dynamic_field)[3]
        # 3. load data
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        # 4. flush and load
        collection_w.num_entities
        collection_w.load()
        # 5. search
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.skip("enable this later using session/strong consistency")
    @pytest.mark.tags(CaseLabel.L1)
    def test_search_new_data(self, nq, _async):
        """
        target: test search new inserted data without load
        method: 1. search the collection
                2. insert new data
                3. search the collection without load again
                4. Use guarantee_timestamp to guarantee data consistency
        expected: new data should be searched
        """
        # 1. initialize with data
        dim = 128
        auto_id = False
        limit = 1000
        nb_old = 500
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nb_old,
                                                                                  auto_id=auto_id,
                                                                                  dim=dim)[0:5]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        log.info("test_search_new_data: searching for original data after load")
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})
        # 3. insert new data
        nb_new = 300
        _, _, _, insert_ids_new, time_stamp = cf.insert_data(collection_w, nb_new,
                                                             auto_id=auto_id, dim=dim,
                                                             insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)
        # 4. search for new data without load
        # Using bounded staleness, maybe we could not search the "inserted" entities,
        # since the search requests arrived query nodes earlier than query nodes consume the insert requests.
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            guarantee_timestamp=time_stamp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old + nb_new,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_different_data_distribution_with_index(self, auto_id, _async):
        """
        target: test search different data distribution with index
        method: 1. connect milvus
                2. create a collection
                3. insert data
                4. create an index
                5. Load and search
        expected: Search successfully
        """
        # 1. connect, create collection and insert data
        dim = 64
        self._connect()
        collection_w = self.init_collection_general(
            prefix, False, dim=dim, is_index=False)[0]
        dataframe = cf.gen_default_dataframe_data(dim=dim, start=-1500)
        collection_w.insert(dataframe)

        # 2. create index
        index_param = {"index_type": "IVF_FLAT",
                       "metric_type": "COSINE", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)

        # 3. load and search
        collection_w.load()
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_max_dim(self, _async):
        """
        target: test search with max configuration
        method: create connection, collection, insert and search with max dim
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        auto_id = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, 100,
                                                                      auto_id=auto_id,
                                                                      dim=max_dim)[0:4]
        # 2. search
        nq = 2
        log.info("test_search_max_dim: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(max_dim)]
                   for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, nq,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nq,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_min_dim(self, _async):
        """
        target: test search with min configuration
        method: create connection, collection, insert and search with dim=1
        expected: search successfully
        """
        # 1. initialize with data
        auto_id = True
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, 100,
                                                                      auto_id=auto_id, dim=min_dim,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search
        nq = 2
        log.info("test_search_min_dim: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(min_dim)]
                   for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, nq,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nq,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [1, 20, 100, 8000, 16384])
    def test_search_different_nq(self, nq):
        """
        target: test search with different nq
        method: create collection, insert, load and search with different nq ∈ [1, 16384]
        expected: search successfully with different nq
        """
        collection_w, _, _, insert_ids = self.init_collection_general(
            prefix, True, nb=20000)[0:4]
        log.info("test_search_max_nq: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("shards_num", [-256, 0, ct.max_shards_num // 2, ct.max_shards_num])
    def test_search_with_non_default_shard_nums(self, shards_num, _async):
        """
        target: test search with non_default shards_num
        method: connect milvus, create collection with several shard numbers , insert, load and search
        expected: search successfully with the non_default shards_num
        """
        auto_id = False
        self._connect()
        # 1. create collection
        name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(
            name=name, shards_num=shards_num)
        # 2. rename collection
        new_collection_name = cf.gen_unique_str(prefix + "new")
        self.utility_wrap.rename_collection(
            collection_w.name, new_collection_name)
        collection_w = self.init_collection_wrap(
            name=new_collection_name, shards_num=shards_num)
        # 3. insert
        dataframe = cf.gen_default_dataframe_data()
        collection_w.insert(dataframe)
        # 4. create index and load
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        # 5. search
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("M", [4, 64])
    @pytest.mark.parametrize("efConstruction", [8, 512])
    def test_search_HNSW_index_with_max_ef(self, M, efConstruction, _async):
        """
        target: test search HNSW index with max ef
        method: connect milvus, create collection , insert, create index, load and search
        expected: search successfully
        """
        dim = M * 4
        auto_id = True
        enable_dynamic_field = False
        self._connect()
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, partition_num=1, auto_id=auto_id,
                                         dim=dim, is_index=False, enable_dynamic_field=enable_dynamic_field)[0:5]
        HNSW_index_params = {"M": M, "efConstruction": efConstruction}
        HNSW_index = {"index_type": "HNSW",
                      "params": HNSW_index_params, "metric_type": "L2"}
        collection_w.create_index("float_vector", HNSW_index)
        collection_w.load()
        search_param = {"metric_type": "L2", "params": {"ef": 32768}}
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_param, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("M", [4, 64])
    @pytest.mark.parametrize("efConstruction", [8, 512])
    def test_search_HNSW_index_with_redundant_param(self, M, efConstruction, _async):
        """
        target: test search HNSW index with redundant param
        method: connect milvus, create collection , insert, create index, load and search
        expected: search successfully
        """
        dim = M * 4
        auto_id = False
        enable_dynamic_field = False
        self._connect()
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, partition_num=1, auto_id=auto_id,
                                         dim=dim, is_index=False, enable_dynamic_field=enable_dynamic_field)[0:5]
        # nlist is of no use
        HNSW_index_params = {
            "M": M, "efConstruction": efConstruction, "nlist": 100}
        HNSW_index = {"index_type": "HNSW",
                      "params": HNSW_index_params, "metric_type": "L2"}
        collection_w.create_index("float_vector", HNSW_index)
        collection_w.load()
        search_param = {"metric_type": "L2", "params": {
            "ef": 32768, "nprobe": 10}}  # nprobe is of no use
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_param, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("M", [4, 64])
    @pytest.mark.parametrize("efConstruction", [8, 512])
    @pytest.mark.parametrize("limit", [1, 10, 3000])
    def test_search_HNSW_index_with_min_ef(self, M, efConstruction, limit, _async):
        """
        target: test search HNSW index with min ef
        method: connect milvus, create collection , insert, create index, load and search
        expected: search successfully
        """
        dim = M * 4
        ef = limit
        auto_id = True
        enable_dynamic_field = True
        self._connect()
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, 5000, partition_num=1,
                                         auto_id=auto_id, dim=dim, is_index=False,
                                         enable_dynamic_field=enable_dynamic_field)[0:5]
        HNSW_index_params = {"M": M, "efConstruction": efConstruction}
        HNSW_index = {"index_type": "HNSW",
                      "params": HNSW_index_params, "metric_type": "L2"}
        collection_w.create_index("float_vector", HNSW_index)
        collection_w.load()
        search_param = {"metric_type": "L2", "params": {"ef": ef}}
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_param, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.tags(CaseLabel.GPU)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_after_different_index_with_params(self, index, _async, scalar_index):
        """
        target: test search after different index
        method: test search after different index and corresponding search params
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, 5000, partition_num=1, is_all_data_type=True,
                                         auto_id=auto_id, dim=dim, is_index=False,
                                         enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. create index on vector field and load
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, default_index)
        # 3. create index on scalar field
        scalar_index_params = {"index_type": scalar_index, "params": {}}
        collection_w.create_index(ct.default_int64_field_name, scalar_index_params)
        collection_w.load()
        # 4. search
        search_params = cf.gen_search_param(index, "COSINE")
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            limit = default_limit
            if index == "HNSW":
                limit = search_param["params"]["ef"]
                if limit > max_limit:
                    limit = default_nb
            if index == "DISKANN":
                limit = search_param["params"]["search_list"]
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.tags(CaseLabel.GPU)
    @pytest.mark.skip(reason="waiting for the address of bf16 data generation slow problem")
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_after_different_index_with_params_all_vector_type_multiple_vectors(self, index,
                                                                                       _async,
                                                                                       scalar_index):
        """
        target: test search after different index
        method: test search after different index and corresponding search params
        expected: search successfully with limit(topK)
        """
        auto_id = False
        enable_dynamic_field = False
        if index == "DISKANN":
            pytest.skip("https://github.com/milvus-io/milvus/issues/30793")
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp =\
            self.init_collection_general(prefix, True, 5000, partition_num=1, is_all_data_type=True,
                                         auto_id=auto_id, dim=default_dim, is_index=False,
                                         enable_dynamic_field=enable_dynamic_field,
                                         multiple_dim_array=[default_dim, default_dim])[0:5]
        # 2. create index on vector field and load
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, default_index)
        # 3. create index on scalar field
        scalar_index_params = {"index_type": scalar_index, "params": {}}
        collection_w.create_index(ct.default_int64_field_name, scalar_index_params)
        collection_w.load()
        # 4. search
        search_params = cf.gen_search_param(index, "COSINE")
        vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            limit = default_limit
            if index == "HNSW":
                limit = search_param["params"]["ef"]
                if limit > max_limit:
                    limit = default_nb
            if index == "DISKANN":
                limit = search_param["params"]["search_list"]
            collection_w.search(vectors[:default_nq], vector_name_list[0],
                                search_param, limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.GPU)
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    def test_search_after_different_index_with_params_gpu(self, index, _async):
        """
        target: test search after different index
        method: test search after different index and corresponding search params
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, 5000, partition_num=1, auto_id=auto_id,
                                         dim=dim, is_index=False,
                                         enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. create index and load
        params = cf.get_index_params_params(index)
        if params.get("m"):
            if (dim % params["m"]) != 0:
                params["m"] = dim // 4
        if params.get("PQM"):
            if (dim % params["PQM"]) != 0:
                params["PQM"] = dim // 4
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search
        search_params = cf.gen_search_param(index)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("search_params", cf.gen_autoindex_search_params())
    @pytest.mark.skip("issue #24533 #24555")
    def test_search_default_search_params_fit_for_autoindex(self, search_params, _async):
        """
        target: test search using autoindex
        method: test search using autoindex and its corresponding search params
        expected: search successfully
        """
        # 1. initialize with data
        auto_id = True
        collection_w = self.init_collection_general(
            prefix, True, auto_id=auto_id, is_index=False)[0]
        # 2. create index and load
        collection_w.create_index("float_vector", {})
        collection_w.load()
        # 3. search
        log.info("Searching with search params: {}".format(search_params))
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.tags(CaseLabel.GPU)
    @pytest.mark.skip("issue #27252")
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_after_different_index_with_min_dim(self, index, _async):
        """
        target: test search after different index with min dim
        method: test search after different index and corresponding search params with dim = 1
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        auto_id = False
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  dim=min_dim, is_index=False)[0:5]
        # 2. create index and load
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search
        search_params = cf.gen_search_param(index)
        vectors = [[random.random() for _ in range(min_dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.GPU)
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    def test_search_after_different_index_with_min_dim_gpu(self, index, _async):
        """
        target: test search after different index with min dim
        method: test search after different index and corresponding search params with dim = 1
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        auto_id = False
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  dim=min_dim, is_index=False)[0:5]
        # 2. create index and load
        params = cf.get_index_params_params(index)
        if params.get("m"):
            params["m"] = min_dim
        if params.get("PQM"):
            params["PQM"] = min_dim
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search
        search_params = cf.gen_search_param(index)
        vectors = [[random.random() for _ in range(min_dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.tags(CaseLabel.GPU)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_after_index_different_metric_type(self, index, _async, metric_type):
        """
        target: test search with different metric type
        method: test search with different metric type
        expected: searched successfully
        """
        # 1. initialize with data
        dim = 64
        auto_id = True
        enable_dynamic_field = True
        collection_w, _vectors, _, insert_ids, time_stamp =\
            self.init_collection_general(prefix, True, 5000, partition_num=1, auto_id=auto_id,
                                         dim=dim, is_index=False, enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. get vectors that inserted into collection
        original_vectors = []
        if enable_dynamic_field:
            for vector in _vectors[0]:
                vector = vector[ct.default_float_vec_field_name]
                original_vectors.append(vector)
        else:
            for _vector in _vectors:
                vectors_tmp = np.array(_vector).tolist()
                vectors_single = [vectors_tmp[i][-1] for i in range(2500)]
                original_vectors.append(vectors_single)
        log.info(len(original_vectors))
        # 3. create different index
        params = cf.get_index_params_params(index)
        if params.get("m"):
            if (dim % params["m"]) != 0:
                params["m"] = dim // 4
        if params.get("PQM"):
            if (dim % params["PQM"]) != 0:
                params["PQM"] = dim // 4
        log.info("test_search_after_index_different_metric_type: Creating index-%s" % index)
        default_index = {"index_type": index, "params": params, "metric_type": metric_type}
        collection_w.create_index("float_vector", default_index)
        log.info("test_search_after_index_different_metric_type: Created index-%s" % index)
        collection_w.load()
        # 4. search
        search_params = cf.gen_search_param(index, metric_type)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            limit = default_limit
            if index == "HNSW":
                limit = search_param["params"]["ef"]
                if limit > max_limit:
                    limit = default_nb
            if index == "DISKANN":
                limit = search_param["params"]["search_list"]
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": limit,
                                             "_async": _async,
                                             "metric": metric_type,
                                             "vector_nq": vectors[:default_nq],
                                             "original_vectors": original_vectors})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue 24957")
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    def test_search_after_release_recreate_index(self, index, _async, metric_type):
        """
        target: test search after new metric with different metric type
        method: test search after new metric with different metric type
        expected: searched successfully
        """
        # 1. initialize with data
        dim = 64
        auto_id = True
        enable_dynamic_field = False
        collection_w, _vectors, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, 5000, partition_num=1, auto_id=auto_id,
                                         dim=dim, is_index=False, enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. get vectors that inserted into collection
        original_vectors = []
        if enable_dynamic_field:
            for vector in _vectors[0]:
                vector = vector[ct.default_float_vec_field_name]
                original_vectors.append(vector)
        else:
            for _vector in _vectors:
                vectors_tmp = np.array(_vector).tolist()
                vectors_single = [vectors_tmp[i][-1] for i in range(2500)]
                original_vectors.append(vectors_single)
        # 3. create different index
        params = cf.get_index_params_params(index)
        if params.get("m"):
            if (dim % params["m"]) != 0:
                params["m"] = dim // 4
        if params.get("PQM"):
            if (dim % params["PQM"]) != 0:
                params["PQM"] = dim // 4
        log.info("test_search_after_release_recreate_index: Creating index-%s" % index)
        default_index = {"index_type": index, "params": params, "metric_type": "COSINE"}
        collection_w.create_index("float_vector", default_index)
        log.info("test_search_after_release_recreate_index: Created index-%s" % index)
        collection_w.load()
        # 4. search
        search_params = cf.gen_search_param(index, "COSINE")
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp, _async=_async)
        # 5. re-create index
        collection_w.release()
        collection_w.drop_index()
        default_index = {"index_type": index, "params": params, "metric_type": metric_type}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async,
                                             "metric": metric_type,
                                             "vector_nq": vectors[:default_nq],
                                             "original_vectors": original_vectors})

    @pytest.mark.tags(CaseLabel.GPU)
    @pytest.mark.parametrize("index", ct.all_index_types[9:11])
    def test_search_after_index_different_metric_type_gpu(self, index, _async):
        """
        target: test search with different metric type
        method: test search with different metric type
        expected: searched successfully
        """
        # 1. initialize with data
        dim = 64
        auto_id = True
        enable_dynamic_field = False
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, 5000, partition_num=1, auto_id=auto_id,
                                         dim=dim, is_index=False, enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. create different index
        params = cf.get_index_params_params(index)
        if params.get("m"):
            if (dim % params["m"]) != 0:
                params["m"] = dim // 4
        if params.get("PQM"):
            if (dim % params["PQM"]) != 0:
                params["PQM"] = dim // 4
        log.info("test_search_after_index_different_metric_type: Creating index-%s" % index)
        default_index = {"index_type": index, "params": params, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        log.info("test_search_after_index_different_metric_type: Created index-%s" % index)
        collection_w.load()
        # 3. search
        search_params = cf.gen_search_param(index, "IP")
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_collection_multiple_times(self, nq, _async):
        """
        target: test search for multiple times
        method: search for multiple times
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 1000
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      auto_id=auto_id, dim=dim,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search for multiple times
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        for i in range(search_num):
            log.info(
                "test_search_collection_multiple_times: searching round %d" % (i + 1))
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_sync_async_multiple_times(self, nq):
        """
        target: test async search after sync search case
        method: create connection, collection, insert,
                sync search and async search
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        dim = 64
        auto_id = True
        enable_dynamic_field = False
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, nb, auto_id=auto_id, dim=dim,
                                         enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. search
        log.info("test_search_sync_async_multiple_times: searching collection %s" %
                 collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        for i in range(search_num):
            log.info(
                "test_search_sync_async_multiple_times: searching round %d" % (i + 1))
            for _async in [False, True]:
                collection_w.search(vectors[:nq], default_search_field,
                                    default_search_params, default_limit,
                                    default_search_exp, _async=_async,
                                    check_task=CheckTasks.check_search_results,
                                    check_items={"nq": nq,
                                                 "ids": insert_ids,
                                                 "limit": default_limit,
                                                 "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue #12680")
    # TODO: add one more for binary vectors
    # @pytest.mark.parametrize("vec_fields", [[cf.gen_float_vec_field(name="test_vector1")],
    #                                         [cf.gen_binary_vec_field(name="test_vector1")],
    #                                         [cf.gen_binary_vec_field(), cf.gen_binary_vec_field("test_vector1")]])
    def test_search_multiple_vectors_with_one_indexed(self):
        """
        target: test indexing on one vector fields when there are multi float vec fields
        method: 1. create collection with multiple float vector fields
                2. insert data and build index on one of float vector fields
                3. load collection and search
        expected: load and search successfully
        """
        vec_fields = [cf.gen_float_vec_field(name="test_vector1")]
        schema = cf.gen_schema_multi_vector_fields(vec_fields)
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema)
        df = cf.gen_dataframe_multi_vec_fields(vec_fields=vec_fields)
        collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        _index = {"index_type": "IVF_FLAT", "params": {
            "nlist": 128}, "metric_type": "L2"}
        res, ch = collection_w.create_index(
            field_name="test_vector1", index_params=_index)
        assert ch is True
        collection_w.load()
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(2)]
        search_params = {"metric_type": "L2", "params": {"nprobe": 16}}
        res_1, _ = collection_w.search(data=vectors, anns_field="test_vector1",
                                       param=search_params, limit=1)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_index_one_partition(self, _async):
        """
        target: test search from partition
        method: search from one partition
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 1200
        auto_id = False
        enable_dynamic_field = True
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, nb, partition_num=1, auto_id=auto_id,
                                         is_index=False, enable_dynamic_field=enable_dynamic_field)[0:5]

        # 2. create index
        default_index = {"index_type": "IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search in one partition
        log.info(
            "test_search_index_one_partition: searching (1000 entities) through one partition")
        limit = 1000
        par = collection_w.partitions
        if limit > par[1].num_entities:
            limit_check = par[1].num_entities
        else:
            limit_check = limit
        search_params = {"metric_type": "L2", "params": {"nprobe": 128}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            search_params, limit, default_search_exp,
                            [par[1].name], _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids[par[0].num_entities:],
                                         "limit": limit_check,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_index_partitions(self, nq, _async):
        """
        target: test search from partitions
        method: search from partitions
        expected: searched successfully
        """
        # 1. initialize with data
        dim = 64
        nb = 1000
        auto_id = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      partition_num=1, auto_id=auto_id,
                                                                      dim=dim, is_index=False)[0:4]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create index
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search through partitions
        log.info("test_search_index_partitions: searching (1000 entities) through partitions")
        par = collection_w.partitions
        log.info("test_search_index_partitions: partitions: %s" % par)
        search_params = {"metric_type": "L2", "params": {"nprobe": 64}}
        collection_w.search(vectors[:nq], default_search_field,
                            search_params, ct.default_limit, default_search_exp,
                            [par[0].name, par[1].name], _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": ct.default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_names", [["(.*)"], ["search(.*)"]])
    def test_search_index_partitions_fuzzy(self, partition_names):
        """
        target: test search from partitions
        method: search from partitions with fuzzy
                partition name
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 2000
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb, partition_num=1,
                                                                      auto_id=auto_id, dim=dim, is_index=False,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        nq = 2
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create index
        nlist = 128
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": nlist}, "metric_type": "COSINE"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search through partitions
        limit = 100
        search_params = {"metric_type": "COSINE", "params": {"nprobe": nlist}}
        collection_w.search(vectors[:nq], default_search_field,
                            search_params, limit=limit, expr=default_search_exp,
                            partition_names=partition_names,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 65535,
                                         ct.err_msg: f"partition name {partition_names[0]} not found"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_index_partition_empty(self, nq, _async):
        """
        target: test search the empty partition
        method: search from the empty partition
        expected: searched successfully with 0 results
        """
        # 1. initialize with data
        dim = 64
        auto_id = True
        collection_w = self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_index=False)[0]
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # 2. create empty partition
        partition_name = "search_partition_empty"
        collection_w.create_partition(
            partition_name=partition_name, description="search partition empty")
        par = collection_w.partitions
        log.info("test_search_index_partition_empty: partitions: %s" % par)
        # 3. create index
        default_index = {"index_type": "IVF_FLAT", "params": {
            "nlist": 128}, "metric_type": "COSINE"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 4. search the empty partition
        log.info("test_search_index_partition_empty: searching %s "
                 "entities through empty partition" % default_limit)
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, [partition_name],
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": [],
                                         "limit": 0,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_search_binary_jaccard_flat_index(self, nq, _async, index, is_flush):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with JACCARD
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        dim = 64
        auto_id = False
        collection_w, _, binary_raw_vector, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, 2, is_binary=True, auto_id=auto_id,
                                         dim=dim, is_index=False, is_flush=is_flush)[0:5]
        # 2. create index on sclalar and vector field
        default_index = {"index_type": "INVERTED", "params": {}}
        collection_w.create_index(ct.default_float_field_name, default_index)
        default_index = {"index_type": index, "params": {
            "nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, dim)
        distance_0 = cf.jaccard(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.jaccard(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": 2,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert abs(res[0].distances[0] -
                   min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_search_binary_hamming_flat_index(self, nq, _async, index, is_flush):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with HAMMING
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        dim = 64
        auto_id = False
        collection_w, _, binary_raw_vector, insert_ids = \
            self.init_collection_general(prefix, True, 2, is_binary=True, auto_id=auto_id,
                                         dim=dim, is_index=False, is_flush=is_flush)[0:4]
        # 2. create index
        default_index = {"index_type": index, "params": {
            "nlist": 128}, "metric_type": "HAMMING"}
        collection_w.create_index("binary_vector", default_index)
        # 3. compute the distance
        collection_w.load()
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, dim)
        distance_0 = cf.hamming(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.hamming(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "HAMMING", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": 2,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert abs(res[0].distances[0] -
                   min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("tanimoto obsolete")
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_search_binary_tanimoto_flat_index(self, nq, _async, index, is_flush):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with TANIMOTO
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        dim = 64
        auto_id = False
        collection_w, _, binary_raw_vector, insert_ids = \
            self.init_collection_general(prefix, True, 2, is_binary=True, auto_id=auto_id,
                                         dim=dim, is_index=False, is_flush=is_flush)[0:4]
        log.info("auto_id= %s, _async= %s" % (auto_id, _async))
        # 2. create index
        default_index = {"index_type": index, "params": {
            "nlist": 128}, "metric_type": "TANIMOTO"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, dim)
        distance_0 = cf.tanimoto(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.tanimoto(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "TANIMOTO", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": 2,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert abs(res[0].distances[0] -
                   min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT"])
    def test_search_binary_substructure_flat_index(self, _async, index, is_flush):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with SUBSTRUCTURE.
                (1) The returned limit(topK) are impacted by dimension (dim) of data
                (2) Searched topK is smaller than set limit when dim is large
                (3) It does not support "BIN_IVF_FLAT" index
                (4) Only two values for distance: 0 and 1, 0 means hits, 1 means not
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        nq = 1
        dim = 8
        auto_id = True
        collection_w, _, binary_raw_vector, insert_ids, time_stamp \
            = self.init_collection_general(prefix, True, default_nb, is_binary=True, auto_id=auto_id,
                                           dim=dim, is_index=False, is_flush=is_flush)[0:5]
        # 2. create index
        default_index = {"index_type": index, "params": {"nlist": 128}, "metric_type": "SUBSTRUCTURE"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. generate search vectors
        _, binary_vectors = cf.gen_binary_vectors(nq, dim)
        # 4. search and compare the distance
        search_params = {"metric_type": "SUBSTRUCTURE", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async)[0]
        if _async:
            res.done()
            res = res.result()
        assert res[0].distances[0] == 0.0
        assert len(res) <= default_limit

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT"])
    def test_search_binary_superstructure_flat_index(self, _async, index, is_flush):
        """
        target: search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with SUPERSTRUCTURE
                (1) The returned limit(topK) are impacted by dimension (dim) of data
                (2) Searched topK is smaller than set limit when dim is large
                (3) It does not support "BIN_IVF_FLAT" index
                (4) Only two values for distance: 0 and 1, 0 means hits, 1 means not
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        nq = 1
        dim = 8
        auto_id = True
        collection_w, _, binary_raw_vector, insert_ids, time_stamp \
            = self.init_collection_general(prefix, True, default_nb, is_binary=True, auto_id=auto_id,
                                           dim=dim, is_index=False, is_flush=is_flush)[0:5]
        # 2. create index
        default_index = {"index_type": index, "params": {"nlist": 128}, "metric_type": "SUPERSTRUCTURE"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. generate search vectors
        _, binary_vectors = cf.gen_binary_vectors(nq, dim)
        # 4. search and compare the distance
        search_params = {"metric_type": "SUPERSTRUCTURE", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:nq], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async)[0]
        if _async:
            res.done()
            res = res.result()
        assert len(res[0]) <= default_limit
        assert res[0].distances[0] == 0.0

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_binary_without_flush(self, metrics):
        """
        target: test search without flush for binary data (no index)
        method: create connection, collection, insert, load and search
        expected: search successfully with limit(topK)
        """
        # 1. initialize a collection without data
        auto_id = True
        collection_w = self.init_collection_general(
            prefix, is_binary=True, auto_id=auto_id, is_index=False)[0]
        # 2. insert data
        insert_ids = cf.insert_data(
            collection_w, default_nb, is_binary=True, auto_id=auto_id)[3]
        # 3. load data
        index_params = {"index_type": "BIN_FLAT", "params": {
            "nlist": 128}, "metric_type": metrics}
        collection_w.create_index("binary_vector", index_params)
        collection_w.load()
        # 4. search
        log.info("test_search_binary_without_flush: searching collection %s" %
                 collection_w.name)
        binary_vectors = cf.gen_binary_vectors(default_nq, default_dim)[1]
        search_params = {"metric_type": metrics, "params": {"nprobe": 10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression(self, null_data_percent):
        """
        target: test search with different expressions
        method: test search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 2000
        dim = 64
        enable_dynamic_field = False
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, nb, dim=dim, is_index=False,
                                         enable_dynamic_field=enable_dynamic_field,
                                         nullable_fields={ct.default_float_field_name: null_data_percent})[0:4]
        # 2. create index
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # filter result with expression in collection
        _vectors = _vectors[0]
        for _async in [False, True]:
            for expressions in cf.gen_normal_expressions_and_templates():
                log.debug(f"test_search_with_expression: {expressions}")
                expr = expressions[0].replace("&&", "and").replace("||", "or")
                filter_ids = []
                for i, _id in enumerate(insert_ids):
                    if enable_dynamic_field:
                        int64 = _vectors[i][ct.default_int64_field_name]
                        float = _vectors[i][ct.default_float_field_name]
                    else:
                        int64 = _vectors.int64[i]
                        float = _vectors.float[i]
                    if float is None and "float <=" in expr:
                        continue
                    if null_data_percent == 1 and "and float" in expr:
                        continue
                    if not expr or eval(expr):
                        filter_ids.append(_id)

                # 3. search with expression
                vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
                search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                                    default_search_params, nb,
                                                    expr=expr, _async=_async,
                                                    check_task=CheckTasks.check_search_results,
                                                    check_items={"nq": default_nq,
                                                                 "ids": insert_ids,
                                                                 "limit": min(nb, len(filter_ids)),
                                                                 "_async": _async})
                if _async:
                    search_res.done()
                    search_res = search_res.result()
                filter_ids_set = set(filter_ids)
                for hits in search_res:
                    ids = hits.ids
                    assert set(ids).issubset(filter_ids_set)

                # 4. search again with expression template
                expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
                expr_params = cf.get_expr_params_from_template(expressions[1])
                search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                                    default_search_params, nb,
                                                    expr=expr, expr_params=expr_params, _async=_async,
                                                    check_task=CheckTasks.check_search_results,
                                                    check_items={"nq": default_nq,
                                                                 "ids": insert_ids,
                                                                 "limit": min(nb, len(filter_ids)),
                                                                 "_async": _async})
                if _async:
                    search_res.done()
                    search_res = search_res.result()
                filter_ids_set = set(filter_ids)
                for hits in search_res:
                    ids = hits.ids
                    assert set(ids).issubset(filter_ids_set)

                # 5. search again with expression template and search hints
                search_param = default_search_params.copy()
                search_param.update({"hints": "iterative_filter"})
                search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                                    search_param, nb,
                                                    expr=expr, expr_params=expr_params, _async=_async,
                                                    check_task=CheckTasks.check_search_results,
                                                    check_items={"nq": default_nq,
                                                                 "ids": insert_ids,
                                                                 "limit": min(nb, len(filter_ids)),
                                                                 "_async": _async})
                if _async:
                    search_res.done()
                    search_res = search_res.result()
                filter_ids_set = set(filter_ids)
                for hits in search_res:
                    ids = hits.ids
                    assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("bool_type", [True, False, "true", "false"])
    def test_search_with_expression_bool(self, _async, bool_type, null_data_percent):
        """
        target: test search with different bool expressions
        method: search with different bool expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        dim = 64
        auto_id = True
        enable_dynamic_field = False
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, nb, is_all_data_type=True, auto_id=auto_id,
                                         dim=dim, is_index=False, enable_dynamic_field=enable_dynamic_field,
                                         nullable_fields={ct.default_bool_field_name: null_data_percent})[0:4]
        # 2. create index and load
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {"nlist": 100}}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, index_param)
        collection_w.load()

        # 3. filter result with expression in collection
        filter_ids = []
        bool_type_cmp = bool_type
        if bool_type == "true":
            bool_type_cmp = True
        if bool_type == "false":
            bool_type_cmp = False
        if enable_dynamic_field:
            for i, _id in enumerate(insert_ids):
                if _vectors[0][i][f"{ct.default_bool_field_name}"] == bool_type_cmp:
                    filter_ids.append(_id)
        else:
            for i in range(len(_vectors[0])):
                if _vectors[0][i].dtype == bool:
                    num = i
                    break
            for i, _id in enumerate(insert_ids):
                if _vectors[0][num][i] == bool_type_cmp:
                    filter_ids.append(_id)

        # 4. search with different expressions
        expression = f"{default_bool_field_name} == {bool_type}"
        log.info("test_search_with_expression_bool: searching with bool expression: %s" % expression)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]

        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, nb, expression,
                                            _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "ids": insert_ids,
                                                         "limit": min(nb, len(filter_ids)),
                                                         "_async": _async})
        if _async:
            search_res.done()
            search_res = search_res.result()

        filter_ids_set = set(filter_ids)
        for hits in search_res:
            ids = hits.ids
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_expression_array(self, null_data_percent):
        """
        target: test search with different expressions
        method: test search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        enable_dynamic_field = False
        # 1. create a collection
        nb = ct.default_nb
        schema = cf.gen_array_collection_schema()
        collection_w = self.init_collection_wrap(schema=schema, enable_dynamic_field=enable_dynamic_field)

        # 2. insert data
        array_length = 10
        data = []
        for i in range(int(nb * (1 - null_data_percent))):
            arr = {ct.default_int64_field_name: i,
                   ct.default_float_vec_field_name: cf.gen_vectors(1, ct.default_dim)[0],
                   ct.default_int32_array_field_name: [np.int32(i) for i in range(array_length)],
                   ct.default_float_array_field_name: [np.float32(i) for i in range(array_length)],
                   ct.default_string_array_field_name: [str(i) for i in range(array_length)]}
            data.append(arr)
        for i in range(int(nb * (1 - null_data_percent)), nb):
            arr = {ct.default_int64_field_name: i,
                   ct.default_float_vec_field_name: cf.gen_vectors(1, ct.default_dim)[0],
                   ct.default_int32_array_field_name: [np.int32(i) for i in range(array_length)],
                   ct.default_float_array_field_name: [np.float32(i) for i in range(array_length)],
                   ct.default_string_array_field_name: None}
            data.append(arr)
        collection_w.insert(data)

        # 3. create index
        collection_w.create_index("float_vector", ct.default_index)
        collection_w.load()

        # 4. filter result with expression in collection
        for _async in [False, True]:
            for expressions in cf.gen_array_field_expressions_and_templates():
                log.debug(f"search with expression: {expressions} with async={_async}")
                expr = expressions[0].replace("&&", "and").replace("||", "or")
                filter_ids = []
                for i in range(nb):
                    int32_array = data[i][ct.default_int32_array_field_name]
                    float_array = data[i][ct.default_float_array_field_name]
                    string_array = data[i][ct.default_string_array_field_name]
                    if ct.default_string_array_field_name in expr and string_array is None:
                        continue
                    if not expr or eval(expr):
                        filter_ids.append(i)

                # 5. search with expression
                search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                                    default_search_params, limit=nb,
                                                    expr=expr, _async=_async)
                if _async:
                    search_res.done()
                    search_res = search_res.result()
                for hits in search_res:
                    ids = hits.ids
                    assert set(ids) == set(filter_ids)

                # 6. search again with expression template
                expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
                expr_params = cf.get_expr_params_from_template(expressions[1])
                search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                                    default_search_params, limit=nb,
                                                    expr=expr, expr_params=expr_params,
                                                    _async=_async)
                if _async:
                    search_res.done()
                    search_res = search_res.result()
                for hits in search_res:
                    ids = hits.ids
                    assert set(ids) == set(filter_ids)

                # 7. search again with expression template and hints
                search_params = default_search_params.copy()
                search_params.update({"hints": "iterative_filter"})
                search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                                    search_params, limit=nb,
                                                    expr=expr, expr_params=expr_params,
                                                    _async=_async)
                if _async:
                    search_res.done()
                    search_res = search_res.result()
                for hits in search_res:
                    ids = hits.ids
                    assert set(ids) == set(filter_ids)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("exists", ["exists"])
    @pytest.mark.parametrize("json_field_name", ["json_field", "json_field['number']", "json_field['name']",
                                                 "float_array", "not_exist_field", "new_added_field"])
    def test_search_with_expression_exists(self, exists, json_field_name, _async):
        """
        target: test search with different expressions
        method: test search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        enable_dynamic_field = True
        if not enable_dynamic_field:
            pytest.skip("not allowed")
        # 1. initialize with data
        nb = 100
        schema = cf.gen_array_collection_schema(with_json=True, enable_dynamic_field=enable_dynamic_field)
        collection_w = self.init_collection_wrap(schema=schema, enable_dynamic_field=enable_dynamic_field)
        log.info(schema.fields)
        if enable_dynamic_field:
            data = cf.gen_row_data_by_schema(nb, schema=schema)
            for i in range(nb):
                data[i]["new_added_field"] = i
            log.info(data[0])
        else:
            data = cf.gen_array_dataframe_data(nb, with_json=True)
            log.info(data.head(1))
        collection_w.insert(data)

        # 2. create index
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        expression = exists + " " + json_field_name
        if enable_dynamic_field:
            limit = nb if json_field_name in data[0].keys() else 0
        else:
            limit = nb if json_field_name in data.columns.to_list() else 0
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, nb, expression,
                                            _async=_async,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "limit": limit,
                                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_expression_auto_id(self, _async):
        """
        target: test search with different expressions
        method: test search with different expressions with auto id
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 1000
        dim = 64
        enable_dynamic_field = True
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, nb, auto_id=True, dim=dim,
                                         is_index=False, enable_dynamic_field=enable_dynamic_field)[0:4]

        # 2. create index
        index_param = {"index_type": "IVF_FLAT",
                       "metric_type": "COSINE", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # filter result with expression in collection
        search_vectors = [[random.random() for _ in range(dim)]
                          for _ in range(default_nq)]
        _vectors = _vectors[0]
        for expressions in cf.gen_normal_expressions_and_templates_field(default_float_field_name):
            log.debug(f"search with expression: {expressions}")
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            for i, _id in enumerate(insert_ids):
                if enable_dynamic_field:
                    exec(
                        f"{default_float_field_name} = _vectors[i][f'{default_float_field_name}']")
                else:
                    exec(
                        f"{default_float_field_name} = _vectors.{default_float_field_name}[i]")
                if not expr or eval(expr):
                    filter_ids.append(_id)
            # 3. search expressions
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                default_search_params,
                                                limit=nb, expr=expr,
                                                _async=_async,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids)),
                                                             "_async": _async})
            if _async:
                search_res.done()
                search_res = search_res.result()
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                assert set(ids).issubset(filter_ids_set)

            # search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                default_search_params,
                                                limit=nb, expr=expr, expr_params=expr_params,
                                                _async=_async,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids)),
                                                             "_async": _async})
            if _async:
                search_res.done()
                search_res = search_res.result()
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_expr_json_field(self):
        """
        target: test delete entities using normal expression
        method: delete using normal expression
        expected: delete successfully
        """
        # init collection with nb default data
        nb = 2000
        dim = 64
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, nb=nb, dim=dim, enable_dynamic_field=True)[0:4]

        # filter result with expression in collection
        search_vectors = [[random.random() for _ in range(dim)]
                          for _ in range(default_nq)]
        _vectors = _vectors[0]
        for expressions in cf.gen_json_field_expressions_and_templates():
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            filter_ids = []
            json_field = {}
            for i, _id in enumerate(insert_ids):
                json_field['number'] = _vectors[i][ct.default_json_field_name]['number']
                json_field['float'] = _vectors[i][ct.default_json_field_name]['float']
                if not expr or eval(expr):
                    filter_ids.append(_id)

            # 3. search expressions
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                default_search_params,
                                                limit=nb, expr=expr,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids))})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                assert set(ids).issubset(filter_ids_set)

            # 4. search again with expression template
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                default_search_params,
                                                limit=nb, expr=expr, expr_params=expr_params,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids))})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                assert set(ids).issubset(filter_ids_set)

            # 5. search again with expression template and hint
            search_params = default_search_params.copy()
            search_params.update({"hints": "iterative_filter"})
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                search_params,
                                                limit=nb, expr=expr, expr_params=expr_params,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids))})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                assert set(ids).issubset(filter_ids_set)
                 # 6. search again with expression template and hint
            search_params = default_search_params.copy()
            search_params.update({"hints": "iterative_filter"})
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                search_params,
                                                limit=nb, expr=expr, expr_params=expr_params,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids))})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                log.info("binbin2")
                log.info(ids)
                log.info(filter_ids_set)
                assert set(ids).issubset(filter_ids_set)
            # 7. create json index
            default_json_path_index = {"index_type": "INVERTED", "params": {"json_cast_type": DataType.DOUBLE,
                                                                            "json_path": f"{ct.default_json_field_name}['number']"}}
            collection_w.create_index(ct.default_json_field_name, default_json_path_index, index_name = f"{ct.default_json_field_name}_0")
            default_json_path_index = {"index_type": "INVERTED", "params": {"json_cast_type": DataType.DOUBLE,
                                                                            "json_path": f"{ct.default_json_field_name}['float']"}}
            collection_w.create_index(ct.default_json_field_name, default_json_path_index, index_name = f"{ct.default_json_field_name}_1")
            # 8. release and load to make sure the new index is loaded
            collection_w.release()
            collection_w.load()
            # 9. search expressions after json path index
            expr = expressions[0].replace("&&", "and").replace("||", "or")
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                default_search_params,
                                                limit=nb, expr=expr,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids))})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                assert set(ids).issubset(filter_ids_set)

            # 10. search again with expression template after json path index
            expr = cf.get_expr_from_template(expressions[1]).replace("&&", "and").replace("||", "or")
            expr_params = cf.get_expr_params_from_template(expressions[1])
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                default_search_params,
                                                limit=nb, expr=expr, expr_params=expr_params,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids))})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                assert set(ids).issubset(filter_ids_set)

            # 11. search again with expression template and hint after json path index
            search_params = default_search_params.copy()
            search_params.update({"hints": "iterative_filter"})
            log.info("binbin")
            log.info(expr)
            log.info(expr_params)
            log.info(search_params)
            search_res, _ = collection_w.search(search_vectors[:default_nq], default_search_field,
                                                search_params,
                                                limit=nb, expr=expr, expr_params=expr_params,
                                                check_task=CheckTasks.check_search_results,
                                                check_items={"nq": default_nq,
                                                             "ids": insert_ids,
                                                             "limit": min(nb, len(filter_ids))})
            filter_ids_set = set(filter_ids)
            for hits in search_res:
                ids = hits.ids
                log.info(ids)
                log.info(filter_ids_set)
                assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_all_data_type(self, nq, _async, null_data_percent):
        """
        target: test search using all supported data types
        method: search using different supported data types
        expected: search success
        """
        # 1. initialize with data
        nb = 3000
        dim = 64
        auto_id = False
        nullable_fields = {ct.default_int32_field_name: null_data_percent,
                           ct.default_int16_field_name: null_data_percent,
                           ct.default_int8_field_name: null_data_percent,
                           ct.default_bool_field_name: null_data_percent,
                           ct.default_float_field_name: null_data_percent,
                           ct.default_double_field_name: null_data_percent,
                           ct.default_string_field_name: null_data_percent}
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, nb, is_all_data_type=True,
                                         auto_id=auto_id, dim=dim, multiple_dim_array=[dim, dim],
                                         nullable_fields=nullable_fields)[0:4]
        # 2. search
        log.info("test_search_expression_all_data_type: Searching collection %s" %
                 collection_w.name)
        search_exp = "int64 >= 0 && int32 >= 0 && int16 >= 0 " \
                     "&& int8 >= 0 && float >= 0 && double >= 0"
        limit = default_limit
        if null_data_percent == 1:
            limit = 0
            insert_ids = []
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        for search_field in vector_name_list:
            vector_data_type = search_field.lstrip("multiple_vector_")
            vectors = cf.gen_vectors_based_on_vector_type(nq, dim, vector_data_type)
            res = collection_w.search(vectors[:nq], search_field,
                                      default_search_params, default_limit,
                                      search_exp, _async=_async,
                                      output_fields=[default_int64_field_name,
                                                     default_float_field_name,
                                                     default_bool_field_name],
                                      check_task=CheckTasks.check_search_results,
                                      check_items={"nq": nq,
                                                   "ids": insert_ids,
                                                   "limit": limit,
                                                   "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        if limit:
            assert (default_int64_field_name and default_float_field_name
                    and default_bool_field_name) in res[0][0].fields

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("field", ct.all_scalar_data_types[:3])
    def test_search_expression_different_data_type(self, field, null_data_percent):
        """
        target: test search expression using different supported data types
        method: search using different supported data types
        expected: search success
        """
        # 1. initialize with data
        num = int(field[3:])
        offset = 2 ** (num - 1)
        nullable_fields = {field: null_data_percent}
        default_schema = cf.gen_collection_schema_all_datatype(nullable_fields=nullable_fields)
        collection_w = self.init_collection_wrap(schema=default_schema)
        collection_w = cf.insert_data(collection_w, is_all_data_type=True, insert_offset=offset - 1000,
                                      nullable_fields=nullable_fields)[0]

        # 2. create index and load
        vector_name_list = cf.extract_vector_field_name_list(collection_w)
        vector_name_list.append(ct.default_float_vec_field_name)
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {"nlist": 100}}
        for vector_name in vector_name_list:
            collection_w.create_index(vector_name, index_param)
        collection_w.load()

        # 3. search using expression which field value is out of bound
        log.info("test_search_expression_different_data_type: Searching collection %s" % collection_w.name)
        expression = f"{field} >= {offset}"
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, expression, output_fields=[field],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": 0})[0]
        # 4. search normal using all the scalar type as output fields
        collection_w.search(vectors, default_search_field, default_search_params,
                            default_limit, output_fields=[field],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "output_fields": [field]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_comparative_expression(self, _async):
        """
        target: test search with expression comparing two fields
        method: create a collection, insert data and search with comparative expression
        expected: search successfully
        """
        # 1. create a collection
        nb = 10
        dim = 2
        fields = [cf.gen_int64_field("int64_1"), cf.gen_int64_field("int64_2"),
                  cf.gen_float_vec_field(dim=dim)]
        schema = cf.gen_collection_schema(fields=fields, primary_field="int64_1")
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str("comparison"), schema=schema)

        # 2. inset data
        values = pd.Series(data=[i for i in range(0, nb)])
        dataframe = pd.DataFrame({"int64_1": values, "int64_2": values,
                                  ct.default_float_vec_field_name: cf.gen_vectors(nb, dim)})
        insert_res = collection_w.insert(dataframe)[0]

        insert_ids = []
        filter_ids = []
        insert_ids.extend(insert_res.primary_keys)
        for _id in enumerate(insert_ids):
            filter_ids.extend(_id)

        # 3. search with expression
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        expression = "int64_1 <= int64_2"
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        res = collection_w.search(vectors[:nq], default_search_field,
                                  default_search_params, default_limit,
                                  expression, _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "ids": insert_ids,
                                               "limit": default_limit,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        filter_ids_set = set(filter_ids)
        for hits in res:
            ids = hits.ids
            assert set(ids).issubset(filter_ids_set)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_expression_with_double_quotes(self):
        """
        target: test search with expressions with double quotes
        method: test search with expressions with double quotes
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix)[0]
        string_value = [(f"'{cf.gen_str_by_length(3)}'{cf.gen_str_by_length(3)}\""
                         f"{cf.gen_str_by_length(3)}\"") for _ in range(default_nb)]
        data = cf.gen_default_dataframe_data()
        data[default_string_field_name] = string_value
        insert_ids = data[default_int64_field_name]
        collection_w.insert(data)

        # 2. create index
        index_param = {"index_type": "FLAT", "metric_type": "COSINE", "params": {}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        _id = random.randint(0, default_nb)
        string_value[_id] = string_value[_id].replace("\"", "\\\"")
        expression = f"{default_string_field_name} == \"{string_value[_id]}\""
        log.info("test_search_with_expression: searching with expression: %s" % expression)
        search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                            default_search_params, default_limit, expression,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": default_nq,
                                                         "ids": insert_ids,
                                                         "limit": 1})
        assert search_res[0].ids == [_id]

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_output_fields_empty(self, nq, _async):
        """
        target: test search with output fields
        method: search with empty output_field
        expected: search success
        """
        # 1. initialize with data
        nb = 1500
        dim = 32
        auto_id = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search
        log.info("test_search_with_output_fields_empty: Searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            output_fields=[],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async,
                                         "output_fields": []})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_output_field(self, _async):
        """
        target: test search with output fields
        method: search with one output_field
        expected: search success
        """
        # 1. initialize with data
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      auto_id=auto_id,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search
        log.info("test_search_with_output_field: Searching collection %s" % collection_w.name)

        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            output_fields=[default_int64_field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async,
                                         "output_fields": [default_int64_field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_output_vector_field(self, _async):
        """
        target: test search with output fields
        method: search with one output_field
        expected: search success
        """
        # 1. initialize with data
        auto_id = True
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, auto_id=auto_id, enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search
        log.info("test_search_with_output_field: Searching collection %s" % collection_w.name)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            output_fields=[field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq, "ids": insert_ids,
                                         "limit": default_limit, "_async": _async,
                                         "output_fields": [field_name]})[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_output_fields(self, _async):
        """
        target: test search with output fields
        method: search with multiple output_field
        expected: search success
        """
        # 1. initialize with data
        nb = 2000
        dim = 64
        auto_id = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      is_all_data_type=True,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search
        log.info("test_search_with_output_fields: Searching collection %s" % collection_w.name)
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        output_fields = [default_int64_field_name, default_float_field_name]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async,
                                         "output_fields": output_fields})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_output_array_field(self, enable_dynamic_field):
        """
        target: test search output array field
        method: create connection, collection, insert and search
        expected: search successfully
        """
        # 1. create a collection
        auto_id = True
        schema = cf.gen_array_collection_schema(auto_id=auto_id)
        collection_w = self.init_collection_wrap(schema=schema)

        # 2. insert data
        if enable_dynamic_field:
            data = cf.gen_row_data_by_schema(schema=schema)
        else:
            data = cf.gen_array_dataframe_data(auto_id=auto_id)

        collection_w.insert(data)

        # 3. create index and load
        collection_w.create_index(default_search_field)
        collection_w.load()

        # 4. search output array field, check
        output_fields = [ct.default_int64_field_name, ct.default_int32_array_field_name,
                         ct.default_float_array_field_name]
        collection_w.search(vectors[:default_nq], default_search_field, {}, default_limit,
                            output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "output_fields": output_fields})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index", ct.all_index_types[:7])
    @pytest.mark.parametrize("metrics", ct.float_metrics)
    @pytest.mark.parametrize("limit", [20, 1200])
    def test_search_output_field_vector_after_different_index_metrics(self, index, metrics, limit):
        """
        target: test search with output vector field after different index
        method: 1. create a collection and insert data
                2. create index and load
                3. search with output field vector
                4. check the result vectors should be equal to the inserted
        expected: search success
        """
        collection_w, _vectors = self.init_collection_general(prefix, True, is_index=False)[:2]

        # 2. create index and load
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": metrics}
        collection_w.create_index(field_name, default_index)
        collection_w.load()

        # 3. search with output field vector
        search_params = cf.gen_search_param(index, metrics)
        for search_param in search_params:
            log.info(search_param)
            if index == "HNSW":
                limit = search_param["params"]["ef"]
                if limit > max_limit:
                    limit = default_nb
            if index == "DISKANN":
                limit = search_param["params"]["search_list"]
            collection_w.search(vectors[:1], default_search_field,
                                search_param, limit, default_search_exp,
                                output_fields=[field_name],
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": 1,
                                             "limit": limit,
                                             "original_entities": _vectors,
                                             "output_fields": [field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("metrics", ct.binary_metrics[:2])
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_search_output_field_vector_after_binary_index(self, metrics, index):
        """
        target: test search with output vector field after binary index
        method: 1. create a collection and insert data
                2. create index and load
                3. search with output field vector
                4. check the result vectors should be equal to the inserted
        expected: search success
        """
        # 1. create a collection and insert data
        collection_w = self.init_collection_general(prefix, is_binary=True, is_index=False)[0]
        data = cf.gen_default_binary_dataframe_data()[0]
        collection_w.insert(data)

        # 2. create index and load
        params = {"M": 48, "efConstruction": 500} if index == "HNSW" else {"nlist": 128}
        default_index = {"index_type": index, "metric_type": metrics, "params": params}
        collection_w.create_index(binary_field_name, default_index)
        collection_w.load()

        # 3. search with output field vector
        search_params = cf.gen_search_param(index, metrics)
        binary_vectors = cf.gen_binary_vectors(1, default_dim)[1]
        for search_param in search_params:
            res = collection_w.search(binary_vectors, binary_field_name,
                                      search_param, 2, default_search_exp,
                                      output_fields=[binary_field_name])[0]

            # 4. check the result vectors should be equal to the inserted
            assert res[0][0].entity.binary_vector == data[binary_field_name][res[0][0].id]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metrics", ct.structure_metrics)
    @pytest.mark.parametrize("index", ["BIN_FLAT"])
    def test_search_output_field_vector_after_structure_metrics(self, metrics, index):
        """
        target: test search with output vector field after binary index
        method: 1. create a collection and insert data
                2. create index and load
                3. search with output field vector
                4. check the result vectors should be equal to the inserted
        expected: search success
        """
        dim = 8
        # 1. create a collection and insert data
        collection_w = self.init_collection_general(prefix, dim=dim, is_binary=True, is_index=False)[0]
        data = cf.gen_default_binary_dataframe_data(dim=dim)[0]
        collection_w.insert(data)

        # 2. create index and load
        default_index = {"index_type": index, "metric_type": metrics, "params": {"nlist": 128}}
        collection_w.create_index(binary_field_name, default_index)
        collection_w.load()

        # 3. search with output field vector
        search_params = {"metric_type": metrics, "params": {"nprobe": 10}}
        binary_vectors = cf.gen_binary_vectors(ct.default_nq, dim)[1]
        res = collection_w.search(binary_vectors, binary_field_name,
                                  search_params, 2, default_search_exp,
                                  output_fields=[binary_field_name])[0]

        # 4. check the result vectors should be equal to the inserted
        assert res[0][0].entity.binary_vector == data[binary_field_name][res[0][0].id]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim", [32, 77, 768])
    def test_search_output_field_vector_with_different_dim(self, dim):
        """
        target: test search with output vector field after binary index
        method: 1. create a collection and insert data
                2. create index and load
                3. search with output field vector
                4. check the result vectors should be equal to the inserted
        expected: search success
        """
        # 1. create a collection and insert data
        collection_w, _vectors = self.init_collection_general(prefix, True, dim=dim)[:2]

        # 2. search with output field vector
        vectors = cf.gen_vectors(default_nq, dim=dim)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            output_fields=[field_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "original_entities": _vectors,
                                         "output_fields": [field_name]})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_output_vector_field_and_scalar_field(self, enable_dynamic_field):
        """
        target: test search with output vector field and scalar field
        method: 1. initialize a collection
                2. search with output field vector
                3. check no field missing
        expected: search success
        """
        # 1. initialize a collection
        collection_w, _vectors = self.init_collection_general(prefix, True,
                                                              enable_dynamic_field=enable_dynamic_field)[:2]

        # search with output field vector
        output_fields = [default_float_field_name, default_string_field_name,
                         default_json_field_name, default_search_field]
        original_entities = []
        if enable_dynamic_field:
            entities = []
            for vector in _vectors[0]:
                entities.append({default_int64_field_name: vector[default_int64_field_name],
                                 default_float_field_name: vector[default_float_field_name],
                                 default_string_field_name: vector[default_string_field_name],
                                 default_json_field_name: vector[default_json_field_name],
                                 default_search_field: vector[default_search_field]})
            original_entities.append(pd.DataFrame(entities))
        else:
            original_entities = _vectors
        collection_w.search(vectors[:1], default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1,
                                         "limit": default_limit,
                                         "original_entities": original_entities,
                                         "output_fields": output_fields})
        if enable_dynamic_field:
            collection_w.search(vectors[:1], default_search_field,
                                default_search_params, default_limit, default_search_exp,
                                output_fields=["$meta", default_search_field],
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": 1,
                                             "limit": default_limit,
                                             "original_entities": original_entities,
                                             "output_fields": output_fields})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_output_vector_field_and_pk_field(self, enable_dynamic_field):
        """
        target: test search with output vector field and pk field
        method: 1. initialize a collection
                2. search with output field vector
                3. check no field missing
        expected: search success
        """
        # 1. initialize a collection
        collection_w = self.init_collection_general(prefix, True,
                                                    enable_dynamic_field=enable_dynamic_field)[0]

        # 2. search with output field vector
        output_fields = [default_int64_field_name, default_string_field_name, default_search_field]
        collection_w.search(vectors[:1], default_search_field,
                            default_search_params, default_limit, default_search_exp,
                            output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1,
                                         "limit": default_limit,
                                         "output_fields": output_fields})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_output_field_vector_with_partition(self):
        """
        target: test search with output vector field
        method: 1. create a collection and insert data
                2. create index and load
                3. search with output field vector
                4. check the result vectors should be equal to the inserted
        expected: search success
        """
        # 1. create a collection and insert data
        collection_w = self.init_collection_general(prefix, is_index=False)[0]
        partition_w = self.init_partition_wrap(collection_w)
        data = cf.gen_default_dataframe_data()
        partition_w.insert(data)

        # 2. create index and load
        collection_w.create_index(field_name, default_index_params)
        collection_w.load()

        # 3. search with output field vector
        partition_w.search(vectors[:1], default_search_field,
                           default_search_params, default_limit, default_search_exp,
                           output_fields=[field_name],
                           check_task=CheckTasks.check_search_results,
                           check_items={"nq": 1,
                                        "limit": default_limit,
                                        "original_entities": [data],
                                        "output_fields": [field_name]})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("wildcard_output_fields", [["*"], ["*", default_int64_field_name],
                                                        ["*", default_search_field]])
    def test_search_with_output_field_wildcard(self, wildcard_output_fields, _async):
        """
        target: test search with output fields using wildcard
        method: search with one output_field (wildcard)
        expected: search success
        """
        # 1. initialize with data
        auto_id = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      auto_id=auto_id)[0:4]
        # 2. search
        log.info("test_search_with_output_field_wildcard: Searching collection %s" % collection_w.name)
        output_fields = cf.get_wildcard_output_field_names(collection_w, wildcard_output_fields)
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            default_search_exp, _async=_async,
                            output_fields=wildcard_output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async,
                                         "output_fields": output_fields})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_invalid_output_fields(self):
        """
        target: test search with output fields using wildcard
        method: search with one output_field (wildcard)
        expected: search success
        """
        # 1. initialize with data
        invalid_output_fields = [["%"], [""], ["-"]]
        auto_id = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, auto_id=auto_id)[0:4]
        # 2. search
        for field in invalid_output_fields:
            error1 = {ct.err_code: 999, ct.err_msg: "field %s not exist" % field[0]}
            error2 = {ct.err_code: 999, ct.err_msg: "`output_fields` value %s is illegal" % field}
            error = error2 if field == [""] else error1
            collection_w.search(vectors[:default_nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp,
                                output_fields=field,
                                check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_multi_collections(self, nq, _async):
        """
        target: test search multi collections of L2
        method: add vectors into 10 collections, and search
        expected: search status ok, the length of result
        """
        nb = 1000
        dim = 64
        auto_id = True
        self._connect()
        collection_num = 10
        for i in range(collection_num):
            # 1. initialize with data
            log.info("test_search_multi_collections: search round %d" % (i + 1))
            collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                          auto_id=auto_id,
                                                                          dim=dim)[0:4]
            # 2. search
            vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
            log.info("test_search_multi_collections: searching %s entities (nq = %s) from collection %s" %
                     (default_limit, nq, collection_w.name))
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_concurrent_multi_threads(self, nq, _async, null_data_percent):
        """
        target: test concurrent search with multi-processes
        method: search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        """
        # 1. initialize with data
        nb = 3000
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        threads_num = 10
        threads = []
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, nb, auto_id=auto_id, dim=dim,
                                         enable_dynamic_field=enable_dynamic_field,
                                         nullable_fields={ct.default_string_field_name: null_data_percent})[0:4]

        def search(collection_w):
            vectors = [[random.random() for _ in range(dim)]
                       for _ in range(nq)]
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

        # 2. search with multi-processes
        log.info("test_search_concurrent_multi_threads: searching with %s processes" % threads_num)
        for i in range(threads_num):
            t = threading.Thread(target=search, args=(collection_w,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue 37113")
    def test_search_concurrent_two_collections_nullable(self, nq, _async):
        """
        target: test concurrent load/search with multi-processes between two collections with null data in json field
        method: concurrent load, and concurrent search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        """
        # 1. initialize with data
        nb = 3000
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        threads_num = 10
        threads = []
        collection_w_1, _, _, insert_ids = \
            self.init_collection_general(prefix, False, nb, auto_id=True, dim=dim,
                                         enable_dynamic_field=enable_dynamic_field,
                                         nullable_fields={ct.default_json_field_name: 1})[0:4]
        collection_w_2, _, _, insert_ids = \
            self.init_collection_general(prefix, False, nb, auto_id=True, dim=dim,
                                         enable_dynamic_field=enable_dynamic_field,
                                         nullable_fields={ct.default_json_field_name: 1})[0:4]
        collection_w_1.release()
        collection_w_2.release()
        # insert data
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nb)]
        data = [[np.float32(i) for i in range(default_nb)], [str(i) for i in range(default_nb)], [], vectors]
        collection_w_1.insert(data)
        collection_w_2.insert(data)
        collection_w_1.num_entities
        collection_w_2.num_entities
        collection_w_1.load(_async=True)
        collection_w_2.load(_async=True)
        res = {'loading_progress': '0%'}
        res_1 = {'loading_progress': '0%'}
        while ((res['loading_progress'] != '100%') or (res_1['loading_progress'] != '100%')):
            res = self.utility_wrap.loading_progress(collection_w_1.name)[0]
            log.info("collection %s: loading progress: %s " % (collection_w_1.name, res))
            res_1 = self.utility_wrap.loading_progress(collection_w_2.name)[0]
            log.info("collection %s: loading progress: %s " % (collection_w_1.name, res_1))

        def search(collection_w):
            vectors = [[random.random() for _ in range(dim)]
                       for _ in range(nq)]
            collection_w.search(vectors[:nq], default_search_field,
                                default_search_params, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

        # 2. search with multi-processes
        log.info("test_search_concurrent_two_collections_nullable: searching with %s processes" % threads_num)
        for i in range(threads_num):
            t = threading.Thread(target=search, args=(collection_w_1))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.skip(reason="Not running for now")
    @pytest.mark.tags(CaseLabel.L2)
    def test_search_insert_in_parallel(self):
        """
        target: test search and insert in parallel
        method: One process do search while other process do insert
        expected: No exception
        """
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name)
        default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index(ct.default_float_vec_field_name, default_index)
        collection_w.load()

        def do_insert():
            df = cf.gen_default_dataframe_data(10000)
            for i in range(11):
                collection_w.insert(df)
                log.info(f'Collection num entities is : {collection_w.num_entities}')

        def do_search():
            while True:
                results, _ = collection_w.search(cf.gen_vectors(nq, ct.default_dim), default_search_field,
                                                 default_search_params, default_limit, default_search_exp, timeout=30)
                ids = []
                for res in results:
                    ids.extend(res.ids)
                expr = f'{ct.default_int64_field_name} in {ids}'
                collection_w.query(expr, output_fields=[ct.default_int64_field_name, ct.default_float_field_name],
                                   timeout=30)

        p_insert = multiprocessing.Process(target=do_insert, args=())
        p_search = multiprocessing.Process(target=do_search, args=(), daemon=True)

        p_insert.start()
        p_search.start()

        p_insert.join()

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("round_decimal", [0, 1, 2, 3, 4, 5, 6])
    def test_search_round_decimal(self, round_decimal):
        """
        target: test search with valid round decimal
        method: search with valid round decimal
        expected: search successfully
        """
        import math
        tmp_nb = 500
        tmp_nq = 1
        tmp_limit = 5
        enable_dynamic_field = False
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True, nb=tmp_nb,
                                                    enable_dynamic_field=enable_dynamic_field)[0]
        # 2. search
        log.info("test_search_round_decimal: Searching collection %s" % collection_w.name)
        res, _ = collection_w.search(vectors[:tmp_nq], default_search_field,
                                     default_search_params, tmp_limit)

        res_round, _ = collection_w.search(vectors[:tmp_nq], default_search_field,
                                           default_search_params, tmp_limit, round_decimal=round_decimal)

        abs_tol = pow(10, 1 - round_decimal)
        for i in range(tmp_limit):
            dis_expect = round(res[0][i].distance, round_decimal)
            dis_actual = res_round[0][i].distance
            # log.debug(f'actual: {dis_actual}, expect: {dis_expect}')
            # abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)
            assert math.isclose(dis_actual, dis_expect, rel_tol=0, abs_tol=abs_tol)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_large(self):
        """
        target: test search with large expression
        method: test search with large expression
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 10000
        dim = 64
        enable_dynamic_field = True
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, nb, dim=dim, is_index=False,
                                         enable_dynamic_field=enable_dynamic_field,
                                         with_json=False)[0:4]

        # 2. create index
        index_param = {"index_type": "IVF_FLAT", "metric_type": "COSINE", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        expression = f"0 < {default_int64_field_name} < 5001"
        log.info("test_search_with_expression: searching with expression: %s" % expression)

        nums = 5000
        vectors = [[random.random() for _ in range(dim)] for _ in range(nums)]
        search_res, _ = collection_w.search(vectors, default_search_field,
                                            default_search_params, default_limit, expression,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": nums,
                                                         "ids": insert_ids,
                                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_expression_large_two(self):
        """
        target: test search with large expression
        method: test one of the collection ids to another collection search for it, with the large expression
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 10000
        dim = 64
        enable_dynamic_field = True
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, nb, dim=dim, is_index=False,
                                         enable_dynamic_field=enable_dynamic_field,
                                         with_json=False)[0:4]

        # 2. create index
        index_param = {"index_type": "IVF_FLAT", "metric_type": "COSINE", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        nums = 5000
        vectors = [[random.random() for _ in range(dim)] for _ in range(nums)]
        vectors_id = [random.randint(0, nums) for _ in range(nums)]
        expression = f"{default_int64_field_name} in {vectors_id}"
        search_res, _ = collection_w.search(vectors, default_search_field,
                                            default_search_params, default_limit, expression,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={
                                                "nq": nums,
                                                "ids": insert_ids,
                                                "limit": default_limit,
                                            })

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_consistency_bounded(self, nq, _async):
        """
        target: test search with different consistency level
        method: 1. create a collection
                2. insert data
                3. search with consistency_level is "bounded"
        expected: searched successfully
        """
        limit = 1000
        nb_old = 500
        dim = 64
        auto_id = True
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = \
            self.init_collection_general(prefix, True, nb_old, auto_id=auto_id,
                                         dim=dim, enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async,
                                         })

        kwargs = {}
        consistency_level = kwargs.get(
            "consistency_level", CONSISTENCY_BOUNDED)
        kwargs.update({"consistency_level": consistency_level})

        nb_new = 400
        _, _, _, insert_ids_new, _ = cf.insert_data(collection_w, nb_new,
                                                    auto_id=auto_id, dim=dim,
                                                    insert_offset=nb_old,
                                                    enable_dynamic_field=enable_dynamic_field)
        insert_ids.extend(insert_ids_new)

        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs,
                            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_consistency_strong(self, nq, _async):
        """
        target: test search with different consistency level
        method: 1. create a collection
                2. insert data
                3. search with consistency_level is "Strong"
        expected: searched successfully
        """
        limit = 1000
        nb_old = 500
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id, dim=dim,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})

        nb_new = 400
        _, _, _, insert_ids_new, _ = cf.insert_data(collection_w, nb_new,
                                                    auto_id=auto_id, dim=dim,
                                                    insert_offset=nb_old,
                                                    enable_dynamic_field=enable_dynamic_field)
        insert_ids.extend(insert_ids_new)
        kwargs = {}
        consistency_level = kwargs.get("consistency_level", CONSISTENCY_STRONG)
        kwargs.update({"consistency_level": consistency_level})

        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old + nb_new,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_consistency_eventually(self, nq, _async):
        """
        target: test search with different consistency level
        method: 1. create a collection
                2. insert data
                3. search with consistency_level is "eventually"
        expected: searched successfully
        """
        limit = 1000
        nb_old = 500
        dim = 64
        auto_id = True
        enable_dynamic_field = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id, dim=dim,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})
        nb_new = 400
        _, _, _, insert_ids_new, _ = cf.insert_data(collection_w, nb_new,
                                                    auto_id=auto_id, dim=dim,
                                                    insert_offset=nb_old,
                                                    enable_dynamic_field=enable_dynamic_field)
        insert_ids.extend(insert_ids_new)
        kwargs = {}
        consistency_level = kwargs.get(
            "consistency_level", CONSISTENCY_EVENTUALLY)
        kwargs.update({"consistency_level": consistency_level})
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_consistency_session(self, nq, _async):
        """
        target: test search with different consistency level
        method: 1. create a collection
                2. insert data
                3. search with consistency_level is "session"
        expected: searched successfully
        """
        limit = 1000
        nb_old = 500
        dim = 64
        auto_id = False
        enable_dynamic_field = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id, dim=dim,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})

        kwargs = {}
        consistency_level = kwargs.get(
            "consistency_level", CONSISTENCY_SESSION)
        kwargs.update({"consistency_level": consistency_level})

        nb_new = 400
        _, _, _, insert_ids_new, _ = cf.insert_data(collection_w, nb_new,
                                                    auto_id=auto_id, dim=dim,
                                                    insert_offset=nb_old,
                                                    enable_dynamic_field=enable_dynamic_field)
        insert_ids.extend(insert_ids_new)
        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old + nb_new,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_ignore_growing(self, nq, _async):
        """
        target: test search ignoring growing segment
        method: 1. create a collection, insert data, create index and load
                2. insert data again
                3. search with param ignore_growing=True
        expected: searched successfully
        """
        # 1. create a collection
        dim = 64
        collection_w = self.init_collection_general(prefix, True, dim=dim)[0]

        # 2. insert data again
        data = cf.gen_default_dataframe_data(dim=dim, start=10000)
        collection_w.insert(data)

        # 3. search with param ignore_growing=True
        search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}, "ignore_growing": True}
        vector = [[random.random() for _ in range(dim)] for _ in range(nq)]
        res = collection_w.search(vector[:nq], default_search_field, search_params, default_limit,
                                  default_search_exp, _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "limit": default_limit,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        for ids in res[0].ids:
            assert ids < 10000

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_ignore_growing_two(self, nq, _async):
        """
        target: test search ignoring growing segment
        method: 1. create a collection, insert data, create index and load
                2. insert data again
                3. search with param ignore_growing=True(outside search_params)
        expected: searched successfully
        """
        # 1. create a collection
        dim = 64
        collection_w = self.init_collection_general(prefix, True, dim=dim)[0]

        # 2. insert data again
        data = cf.gen_default_dataframe_data(dim=dim, start=10000)
        collection_w.insert(data)

        # 3. search with param ignore_growing=True
        search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}}
        vector = [[random.random() for _ in range(dim)] for _ in range(nq)]
        res = collection_w.search(vector[:nq], default_search_field, search_params, default_limit,
                                  default_search_exp, _async=_async,
                                  ignore_growing=True,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq,
                                               "limit": default_limit,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        for ids in res[0].ids:
            assert ids < 10000

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("name", ["_co11ection", "co11_ection"])
    @pytest.mark.parametrize("index_name", ["_1ndeX", "In_0"])
    def test_search_collection_naming_rules(self, name, index_name, _async):
        """
        target: test search collection naming rules
        method: 1. Connect milvus
                2. Create a field with a name which uses all the supported elements in the naming rules
                3. Create a collection with a name which uses all the supported elements in the naming rules
                4. Create an index with a name which uses all the supported elements in the naming rules
                5. Insert data (5000) into collection
                6. Search collection
        expected: searched successfully
        """
        field_name1 = "_1nt"
        field_name2 = "f10at_"
        collection_name = cf.gen_unique_str(name)
        self._connect()
        fields = [cf.gen_int64_field(), cf.gen_int64_field(field_name1),
                  cf.gen_float_vec_field(field_name2, dim=default_dim)]
        schema = cf.gen_collection_schema(
            fields=fields, primary_field=default_int64_field_name)
        collection_w = self.init_collection_wrap(name=collection_name, schema=schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={"name": collection_name, "schema": schema})
        collection_w.create_index(field_name1, index_name=index_name)
        int_values = pd.Series(data=[i for i in range(0, default_nb)])
        float_vec_values = cf.gen_vectors(default_nb, default_dim)
        dataframe = pd.DataFrame({default_int64_field_name: int_values,
                                  field_name1: int_values, field_name2: float_vec_values})
        collection_w.insert(dataframe)
        collection_w.create_index(
            field_name2, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.search(vectors[:default_nq], field_name2, default_search_params,
                            default_limit, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ["_PartiTi0n", "pArt1_ti0n"])
    def test_search_partition_naming_rules_without_index(self, nq, partition_name):
        """
        target: test search collection naming rules
        method: 1. Connect milvus
                2. Create a collection
                3. Create a partition with a name which uses all the supported elements in the naming rules
                4. Insert data into collection
                5. without index with a name which uses all the supported elements in the naming rules
                6. Search partition (should successful)
        expected: searched successfully
        """
        nb = 5000
        dim = 64
        auto_id = False
        enable_dynamic_field = False
        self._connect()
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, False, nb,
                                                                      auto_id=auto_id, dim=dim,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        collection_w.create_partition(partition_name)
        insert_ids = cf.insert_data(collection_w, nb, auto_id=auto_id, dim=dim,
                                    enable_dynamic_field=enable_dynamic_field)[3]
        collection_w.load()
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, [
                                partition_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ["_PartiTi0n", "pArt1_ti0n"])
    @pytest.mark.parametrize("index_name", ["_1ndeX", "In_0"])
    def test_search_partition_naming_rules_with_index(self, nq, partition_name, index_name):
        """
        target: test search collection naming rules
        method: 1. Connect milvus
                2. Create a collection
                3. Create a partition with a name which uses all the supported elements in the naming rules
                4. Insert data into collection
                5. with index with a name which uses all the supported elements in the naming rules
                6. Search partition (should successful)
        expected: searched successfully
        """
        nb = 5000
        dim = 64
        auto_id = False
        enable_dynamic_field = True
        self._connect()
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, False, nb, auto_id=auto_id,
                                                                      dim=dim, is_index=False,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        collection_w.create_partition(partition_name)
        insert_ids = cf.insert_data(collection_w, nb, auto_id=auto_id, dim=dim,
                                    enable_dynamic_field=enable_dynamic_field)[3]
        collection_w.create_index(
            default_search_field, default_index_params, index_name=index_name)
        collection_w.load()
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq], default_search_field, default_search_params,
                            default_limit, default_search_exp, [
                                partition_name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue #22582")
    def test_search_during_upsert(self):
        """
        target: test search during upsert
        method: 1. create a collection and search
                2. search during upsert
                3. compare two search results
        expected: the two search results is the same
        """
        nq = 5
        upsert_nb = 1000
        collection_w = self.init_collection_general(prefix, True)[0]
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(nq)]
        res1 = collection_w.search(
            vectors[:nq], default_search_field, default_search_params, default_limit)[0]

        def do_upsert():
            data = cf.gen_default_data_for_upsert(upsert_nb)[0]
            collection_w.upsert(data=data)

        t = threading.Thread(target=do_upsert, args=())
        t.start()
        res2 = collection_w.search(
            vectors[:nq], default_search_field, default_search_params, default_limit)[0]
        t.join()
        assert [res1[i].ids for i in range(nq)] == [
            res2[i].ids for i in range(nq)]

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_using_all_types_of_default_value(self, auto_id):
        """
        target: test create collection with default_value
        method: create a schema with all fields using default value and search
        expected: search results are as expected
        """
        fields = [
            cf.gen_int64_field(name='pk', is_primary=True),
            cf.gen_float_vec_field(),
            cf.gen_int8_field(default_value=numpy.int8(8)),
            cf.gen_int16_field(default_value=numpy.int16(16)),
            cf.gen_int32_field(default_value=numpy.int32(32)),
            cf.gen_int64_field(default_value=numpy.int64(64)),
            cf.gen_float_field(default_value=numpy.float32(3.14)),
            cf.gen_double_field(default_value=numpy.double(3.1415)),
            cf.gen_bool_field(default_value=False),
            cf.gen_string_field(default_value="abc")
        ]
        schema = cf.gen_collection_schema(fields, auto_id=auto_id)
        collection_w = self.init_collection_wrap(schema=schema)
        data = [
            [i for i in range(ct.default_nb)],
            cf.gen_vectors(ct.default_nb, ct.default_dim),
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            []
        ]
        if auto_id:
            del data[0]
        collection_w.insert(data)
        collection_w.create_index(field_name, default_index_params)
        collection_w.load()
        res = collection_w.search(vectors[:1], default_search_field, default_search_params,
                                  default_limit, default_search_exp,
                                  output_fields=["*"],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": 1,
                                               "limit": default_limit})[0]
        for res in res[0]:
            res = res.entity
            assert res.get(ct.default_int8_field_name) == 8
            assert res.get(ct.default_int16_field_name) == 16
            assert res.get(ct.default_int32_field_name) == 32
            assert res.get(ct.default_int64_field_name) == 64
            assert res.get(ct.default_float_field_name) == numpy.float32(3.14)
            assert res.get(ct.default_double_field_name) == 3.1415
            assert res.get(ct.default_bool_field_name) is False
            assert res.get(ct.default_string_field_name) == "abc"

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.all_index_types[1:4])
    def test_search_repeatedly_ivf_index_different_limit(self, index):
        """
        target: test create collection repeatedly
        method: search twice, check the results is the same
        expected: search results are as expected
        """
        nb = 5000
        limit = random.randint(10, 100)
        # 1. create a collection
        collection_w = self.init_collection_general(prefix, True, nb, is_index=False)[0]

        # 2. insert data again
        params = cf.get_index_params_params(index)
        index_params = {"metric_type": "COSINE", "index_type": index, "params": params}
        collection_w.create_index(default_search_field, index_params)

        # 3. search with param ignore_growing=True
        collection_w.load()
        search_params = cf.gen_search_param(index, "COSINE")[0]
        vector = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
        res1 = collection_w.search(vector, default_search_field, search_params, limit)[0]
        res2 = collection_w.search(vector, default_search_field, search_params, limit * 2)[0]
        for i in range(default_nq):
            assert res1[i].ids == res2[i].ids[:limit]
        # search again with the previous limit
        res3 = collection_w.search(vector, default_search_field, search_params, limit)[0]
        for i in range(default_nq):
            assert res1[i].ids == res3[i].ids

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metrics", ct.binary_metrics[:2])
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    @pytest.mark.parametrize("dim", [32768, 65536, ct.max_binary_vector_dim - 8, ct.max_binary_vector_dim])
    def test_binary_indexed_large_dim_vectors_search(self, dim, metrics, index):
        """
        target: binary vector large dim search
        method: binary vector large dim search
        expected: search success
        """
        # 1. create a collection and insert data
        collection_w = self.init_collection_general(prefix, dim=dim, is_binary=True, is_index=False)[0]
        data = cf.gen_default_binary_dataframe_data(nb=200, dim=dim)[0]
        collection_w.insert(data)

        # 2. create index and load
        params = {"M": 48, "efConstruction": 500} if index == "HNSW" else {"nlist": 128}
        default_index = {"index_type": index, "metric_type": metrics, "params": params}
        collection_w.create_index(binary_field_name, default_index)
        collection_w.load()

        # 3. search with output field vector
        search_params = cf.gen_search_param(index, metrics)
        binary_vectors = cf.gen_binary_vectors(1, dim)[1]
        for search_param in search_params:
            res = collection_w.search(binary_vectors, binary_field_name,
                                      search_param, 2, default_search_exp,
                                      output_fields=[binary_field_name])[0]

            # 4. check the result vectors should be equal to the inserted
            assert res[0][0].entity.binary_vector == data[binary_field_name][res[0][0].id]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim", [ct.max_binary_vector_dim + 1, ct.max_binary_vector_dim + 8])
    def test_binary_indexed_over_max_dim(self, dim):
        """
        target: tests exceeding the maximum binary vector dimension
        method: tests exceeding the maximum binary vector dimension
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        binary_schema = cf.gen_default_binary_collection_schema(dim=dim)
        self.init_collection_wrap(c_name, schema=binary_schema,
                                  check_task=CheckTasks.err_res,
                                  check_items={"err_code": 999,
                                               "err_msg": f"invalid dimension: {dim} of field "
                                                          f"{ct.default_binary_vec_field_name}. "})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="issue #37547")
    def test_search_verify_expr_cache(self, is_flush):
        """
        target: test search case to test expr cache
        method: 1. create collection with a double datatype field
                2. search with expr "doubleField == 0"
                3. drop this collection
                4. create collection with same collection name and same field name but modify the type of double field
                   as varchar datatype
                5. search with expr "doubleField == 0" again
        expected: 1. search successfully with limit(topK) for the first collection
                  2. report error for the second collection with the same name
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, is_flush=is_flush)[0:5]
        collection_name = collection_w.name
        # 2. generate search data
        vectors = cf.gen_vectors_based_on_vector_type(default_nq, default_dim)
        # 3. search with expr "nullableFid == 0"
        search_exp = f"{ct.default_float_field_name} == 0"
        output_fields = [default_int64_field_name, default_float_field_name]
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            search_exp,
                            output_fields=output_fields,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": 1,
                                         "output_fields": output_fields})
        # 4. drop collection
        collection_w.drop()
        # 5. create the same collection name with same field name but varchar field type
        int64_field = cf.gen_int64_field(is_primary=True)
        string_field = cf.gen_string_field(ct.default_float_field_name)
        json_field = cf.gen_json_field()
        float_vector_field = cf.gen_float_vec_field()
        fields = [int64_field, string_field, json_field, float_vector_field]
        schema = cf.gen_collection_schema(fields)
        collection_w = self.init_collection_wrap(name=collection_name, schema=schema)
        int64_values = pd.Series(data=[i for i in range(default_nb)])
        string_values = pd.Series(data=[str(i) for i in range(default_nb)], dtype="string")
        json_values = [{"number": i, "string": str(i), "bool": bool(i),
                        "list": [j for j in range(i, i + ct.default_json_list_length)]} for i in range(default_nb)]
        float_vec_values = cf.gen_vectors(default_nb, default_dim)
        df = pd.DataFrame({
            ct.default_int64_field_name: int64_values,
            ct.default_float_field_name: string_values,
            ct.default_json_field_name: json_values,
            ct.default_float_vec_field_name: float_vec_values
        })
        collection_w.insert(df)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
        collection_w.load()
        collection_w.flush()
        collection_w.search(vectors[:default_nq], default_search_field,
                            default_search_params, default_limit,
                            search_exp,
                            output_fields=output_fields,
                            check_task=CheckTasks.err_res,
                            check_items={"err_code": 1100,
                                         "err_msg": "failed to create query plan: cannot parse expression: float == 0, "
                                                    "error: comparisons between VarChar and Int64 are not supported: "
                                                    "invalid parameter"})


