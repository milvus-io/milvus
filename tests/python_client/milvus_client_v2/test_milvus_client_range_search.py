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


class TestCollectionRangeSearch(TestcaseBase):
    """ Test case of range search interface """

    @pytest.fixture(scope="function", params=ct.all_index_types[:7])
    def index_type(self, request):
        tags = request.config.getoption("--tags")
        if CaseLabel.L2 not in tags:
            if request.param not in ct.L0_index_types:
                pytest.skip(f"skip index type {request.param}")
        yield request.param

    @pytest.fixture(scope="function", params=ct.dense_metrics)
    def metric(self, request):
        tags = request.config.getoption("--tags")
        if CaseLabel.L2 not in tags:
            if request.param != ct.default_L0_metric:
                pytest.skip(f"skip index type {request.param}")
        yield request.param

    @pytest.fixture(scope="function", params=[default_nb, default_nb_medium])
    def nb(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[2, 500])
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

    @pytest.fixture(scope="function", params=["JACCARD", "HAMMING", "TANIMOTO"])
    def metrics(self, request):
        if request.param == "TANIMOTO":
            pytest.skip("TANIMOTO not supported now")
        yield request.param

    @pytest.fixture(scope="function", params=[False, True])
    def is_flush(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[True, False])
    def enable_dynamic_field(self, request):
        yield request.param

    @pytest.fixture(scope="function", params=[0, 0.5, 1])
    def null_data_percent(self, request):
        yield request.param

    """
    ******************************************************************
    #  The followings are valid range search cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("vector_data_type", ct.all_dense_vector_types)
    @pytest.mark.parametrize("with_growing", [False, True])
    @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/32630")
    def test_range_search_default(self, index_type, metric, vector_data_type, with_growing, null_data_percent):
        """
        target: verify the range search returns correct results
        method: 1. create collection, insert 10k vectors,
                2. search with topk=1000
                3. range search from the 30th-330th distance as filter
                4. verified the range search results is same as the search results in the range
        """
        collection_w = self.init_collection_general(prefix, auto_id=True, insert_data=False, is_index=False,
                                                    vector_data_type=vector_data_type, with_json=False,
                                                    nullable_fields={ct.default_float_field_name: null_data_percent})[0]
        nb = 1000
        rounds = 10
        for i in range(rounds):
            data = cf.gen_default_list_data(nb=nb, auto_id=True, vector_data_type=vector_data_type,
                                            with_json=False, start=i * nb)
            collection_w.insert(data)

        collection_w.flush()
        _index_params = {"index_type": "FLAT", "metric_type": metric, "params": {}}
        collection_w.create_index(ct.default_float_vec_field_name, index_params=_index_params)
        collection_w.load()

        if with_growing is True:
            # add some growing segments
            for j in range(rounds // 2):
                data = cf.gen_default_list_data(nb=nb, auto_id=True, vector_data_type=vector_data_type,
                                                with_json=False, start=(rounds + j) * nb)
                collection_w.insert(data)

        search_params = {"params": {}}
        nq = 1
        search_vectors = cf.gen_vectors(nq, ct.default_dim, vector_data_type=vector_data_type)
        search_res = collection_w.search(search_vectors, default_search_field,
                                         search_params, limit=1000)[0]
        assert len(search_res[0].ids) == 1000
        log.debug(f"search topk=1000 returns {len(search_res[0].ids)}")
        check_topk = 300
        check_from = 30
        ids = search_res[0].ids[check_from:check_from + check_topk]
        radius = search_res[0].distances[check_from + check_topk]
        range_filter = search_res[0].distances[check_from]

        # rebuild the collection with test target index
        collection_w.release()
        collection_w.indexes[0].drop()
        _index_params = {"index_type": index_type, "metric_type": metric,
                         "params": cf.get_index_params_params(index_type)}
        collection_w.create_index(ct.default_float_vec_field_name, index_params=_index_params)
        collection_w.load()

        params = cf.get_search_params_params(index_type)
        params.update({"radius": radius, "range_filter": range_filter})
        if index_type == "HNSW":
            params.update({"ef": check_topk + 100})
        if index_type == "IVF_PQ":
            params.update({"max_empty_result_buckets": 100})
        range_search_params = {"params": params}
        range_res = collection_w.search(search_vectors, default_search_field,
                                        range_search_params, limit=check_topk)[0]
        range_ids = range_res[0].ids
        # assert len(range_ids) == check_topk
        log.debug(f"range search radius={radius}, range_filter={range_filter}, range results num: {len(range_ids)}")
        hit_rate = round(len(set(ids).intersection(set(range_ids))) / len(set(ids)), 2)
        log.debug(
            f"{vector_data_type} range search results {index_type} {metric} with_growing {with_growing} hit_rate: {hit_rate}")
        assert hit_rate >= 0.2  # issue #32630 to improve the accuracy

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("range_filter", [1000, 1000.0])
    @pytest.mark.parametrize("radius", [0, 0.0])
    @pytest.mark.skip()
    def test_range_search_multi_vector_fields(self, nq, dim, auto_id, is_flush, radius, range_filter,
                                              enable_dynamic_field):
        """
        target: test range search normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        multiple_dim_array = [dim, dim]
        collection_w, _vectors, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, auto_id=auto_id, dim=dim, is_flush=is_flush,
                                         enable_dynamic_field=enable_dynamic_field,
                                         multiple_dim_array=multiple_dim_array)[0:5]
        # 2. get vectors that inserted into collection
        vectors = []
        if enable_dynamic_field:
            for vector in _vectors[0]:
                vector = vector[ct.default_float_vec_field_name]
                vectors.append(vector)
        else:
            vectors = np.array(_vectors[0]).tolist()
            vectors = [vectors[i][-1] for i in range(nq)]
        # 3. range search
        range_search_params = {"metric_type": "COSINE", "params": {"radius": radius,
                                                                   "range_filter": range_filter}}
        vector_list = cf.extract_vector_field_name_list(collection_w)
        vector_list.append(default_search_field)
        for search_field in vector_list:
            search_res = collection_w.search(vectors[:nq], search_field,
                                             range_search_params, default_limit,
                                             default_search_exp,
                                             check_task=CheckTasks.check_search_results,
                                             check_items={"nq": nq,
                                                          "ids": insert_ids,
                                                          "limit": default_limit})[0]
            log.info("test_range_search_normal: checking the distance of top 1")
            for hits in search_res:
                # verify that top 1 hit is itself,so min distance is 1.0
                assert abs(hits.distances[0] - 1.0) <= epsilon
                # distances_tmp = list(hits.distances)
                # assert distances_tmp.count(1.0) == 1

    @pytest.mark.tags(CaseLabel.L1)
    def test_range_search_cosine(self):
        """
        target: test range search normal case
        method: create connection, collection, insert and search
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        range_filter = random.uniform(0, 1)
        radius = random.uniform(-1, range_filter)

        # 2. range search
        range_search_params = {"metric_type": "COSINE",
                               "params": {"radius": radius, "range_filter": range_filter}}
        search_res = collection_w.search(vectors[:nq], default_search_field,
                                         range_search_params, default_limit,
                                         default_search_exp)[0]

        # 3. check search results
        for hits in search_res:
            for distance in hits.distances:
                assert range_filter >= distance > radius

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_only_range_filter(self):
        """
        target: test range search with only range filter
        method: create connection, collection, insert and search
        expected: range search successfully as normal search
        """
        # 1. initialize with data
        collection_w, _vectors, _, insert_ids, time_stamp = self.init_collection_general(
            prefix, True, nb=10)[0:5]
        # 2. get vectors that inserted into collection
        vectors = np.array(_vectors[0]).tolist()
        vectors = [vectors[i][-1] for i in range(default_nq)]
        # 3. range search with L2
        range_search_params = {"metric_type": "COSINE",
                               "params": {"range_filter": 1}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})
        # 4. range search with IP
        range_search_params = {"metric_type": "IP",
                               "params": {"range_filter": 1}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 65535,
                                         ct.err_msg: "metric type not match: "
                                                     "invalid parameter[expected=COSINE][actual=IP]"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_only_radius(self):
        """
        target: test range search with only radius
        method: create connection, collection, insert and search
        expected: search successfully with filtered limit(topK)
        """
        # 1. initialize with data
        collection_w, _vectors, _, insert_ids, time_stamp = self.init_collection_general(
            prefix, True, nb=10, is_index=False)[0:5]
        collection_w.create_index(field_name, {"metric_type": "L2"})
        collection_w.load()
        # 2. get vectors that inserted into collection
        vectors = np.array(_vectors[0]).tolist()
        vectors = [vectors[i][-1] for i in range(default_nq)]
        # 3. range search with L2
        range_search_params = {"metric_type": "L2", "params": {"radius": 0}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})
        # 4. range search with IP
        range_search_params = {"metric_type": "IP", "params": {"radius": 0}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 65535,
                                         ct.err_msg: "metric type not match: invalid "
                                                     "parameter[expected=L2][actual=IP]"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_radius_range_filter_not_in_params(self):
        """
        target: test range search radius and range filter not in params
        method: create connection, collection, insert and search
        expected: search successfully as normal search
        """
        # 1. initialize with data
        collection_w, _vectors, _, insert_ids, time_stamp = self.init_collection_general(
            prefix, True, nb=10)[0:5]
        # 2. get vectors that inserted into collection
        vectors = np.array(_vectors[0]).tolist()
        vectors = [vectors[i][-1] for i in range(default_nq)]
        # 3. range search with L2
        range_search_params = {"metric_type": "COSINE",
                               "radius": 0, "range_filter": 1}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})
        # 4. range search with IP
        range_search_params = {"metric_type": "IP",
                               "radius": 1, "range_filter": 0}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 65535,
                                         ct.err_msg: "metric type not match: invalid "
                                                     "parameter[expected=COSINE][actual=IP]"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dup_times", [1, 2])
    def test_range_search_with_dup_primary_key(self, auto_id, _async, dup_times):
        """
        target: test range search with duplicate primary key
        method: 1.insert same data twice
                2.range search
        expected: range search results are de-duplicated
        """
        # 1. initialize with data
        collection_w, insert_data, _, insert_ids = self.init_collection_general(prefix, True, default_nb,
                                                                                auto_id=auto_id,
                                                                                dim=default_dim)[0:4]
        # 2. insert dup data multi times
        for i in range(dup_times):
            insert_res, _ = collection_w.insert(insert_data[0])
            insert_ids.extend(insert_res.primary_keys)
        # 3. range search
        vectors = np.array(insert_data[0]).tolist()
        vectors = [vectors[i][-1] for i in range(default_nq)]
        log.info(vectors)
        range_search_params = {"metric_type": "COSINE", "params": {
            "nprobe": 10, "radius": 0, "range_filter": 1000}}
        search_res = collection_w.search(vectors[:default_nq], default_search_field,
                                         range_search_params, default_limit,
                                         default_search_exp, _async=_async,
                                         check_task=CheckTasks.check_search_results,
                                         check_items={"nq": default_nq,
                                                      "ids": insert_ids,
                                                      "limit": default_limit,
                                                      "_async": _async})[0]
        if _async:
            search_res.done()
            search_res = search_res.result()
        # assert that search results are de-duplicated
        for hits in search_res:
            ids = hits.ids
            assert sorted(list(set(ids))) == sorted(ids)

    @pytest.mark.tags(CaseLabel.L2)
    def test_accurate_range_search_with_multi_segments(self):
        """
        target: range search collection with multi segments accurately
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

        # 6. Search
        range_search_params = {"metric_type": "COSINE", "params": {
            "nprobe": 10, "radius": 0, "range_filter": 1}}
        collection_w.search(vectors, default_search_field,
                            range_search_params, limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={
                                "nq": 1,
                                "limit": limit,
                                "ids": list(distances_index_max)
                            })

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_with_empty_vectors(self, _async):
        """
        target: test range search with empty query vector
        method: search using empty query vector
        expected: search successfully with 0 results
        """
        # 1. initialize without data
        collection_w = self.init_collection_general(
            prefix, True, dim=default_dim)[0]
        # 2. search collection without data
        log.info("test_range_search_with_empty_vectors: Range searching collection %s "
                 "using empty vector" % collection_w.name)
        range_search_params = {"metric_type": "COSINE", "params": {
            "nprobe": 10, "radius": 0, "range_filter": 0}}
        collection_w.search([], default_search_field, range_search_params,
                            default_limit, default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 0,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="partition load and release constraints")
    def test_range_search_before_after_delete(self, nq, _async):
        """
        target: test range search before and after deletion
        method: 1. search the collection
                2. delete a partition
                3. search the collection
        expected: the deleted entities should not be searched
        """
        # 1. initialize with data
        nb = 1000
        limit = 1000
        partition_num = 1
        dim = 100
        auto_id = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb,
                                                                      partition_num,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search all the partitions before partition deletion
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        log.info(
            "test_range_search_before_after_delete: searching before deleting partitions")
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": limit,
                                         "_async": _async})
        # 3. delete partitions
        log.info("test_range_search_before_after_delete: deleting a partition")
        par = collection_w.partitions
        deleted_entity_num = par[partition_num].num_entities
        print(deleted_entity_num)
        entity_num = nb - deleted_entity_num
        collection_w.release(par[partition_num].name)
        collection_w.drop_partition(par[partition_num].name)
        log.info("test_range_search_before_after_delete: deleted a partition")
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        # 4. search non-deleted part after delete partitions
        log.info(
            "test_range_search_before_after_delete: searching after deleting partitions")
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids[:entity_num],
                                         "limit": limit - deleted_entity_num,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_collection_after_release_load(self, _async):
        """
        target: range search the pre-released collection after load
        method: 1. create collection
                2. release collection
                3. load collection
                4. range search the pre-released collection
        expected: search successfully
        """
        # 1. initialize without data
        auto_id = True
        enable_dynamic_field = False
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, default_nb, 1, auto_id=auto_id,
                                         dim=default_dim, enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. release collection
        log.info("test_range_search_collection_after_release_load: releasing collection %s" %
                 collection_w.name)
        collection_w.release()
        log.info("test_range_search_collection_after_release_load: released collection %s" %
                 collection_w.name)
        # 3. Search the pre-released collection after load
        log.info("test_range_search_collection_after_release_load: loading collection %s" %
                 collection_w.name)
        collection_w.load()
        log.info(
            "test_range_search_collection_after_release_load: searching after load")
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        collection_w.search(vectors[:default_nq], default_search_field, range_search_params,
                            default_limit, default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_load_flush_load(self, _async):
        """
        target: test range search when load before flush
        method: 1. insert data and load
                2. flush, and load
                3. search the collection
        expected: search success with limit(topK)
        """
        # 1. initialize with data
        dim = 100
        enable_dynamic_field = True
        collection_w = self.init_collection_general(
            prefix, dim=dim, enable_dynamic_field=enable_dynamic_field)[0]
        # 2. insert data
        insert_ids = cf.insert_data(
            collection_w, default_nb, dim=dim, enable_dynamic_field=enable_dynamic_field)[3]
        # 3. load data
        collection_w.create_index(
            ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        # 4. flush and load
        collection_w.num_entities
        collection_w.load()
        # 5. search
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_new_data(self, nq):
        """
        target: test search new inserted data without load
        method: 1. search the collection
                2. insert new data
                3. search the collection without load again
                4. Use guarantee_timestamp to guarantee data consistency
        expected: new data should be range searched
        """
        # 1. initialize with data
        limit = 1000
        nb_old = 500
        dim = 111
        enable_dynamic_field = False
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, nb_old, dim=dim,
                                         enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"radius": 0,
                                                                   "range_filter": 1000}}
        log.info("test_range_search_new_data: searching for original data after load")
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old})
        # 3. insert new data
        nb_new = 300
        _, _, _, insert_ids_new, time_stamp = cf.insert_data(collection_w, nb_new, dim=dim,
                                                             enable_dynamic_field=enable_dynamic_field,
                                                             insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)
        # 4. search for new data without load
        # Using bounded staleness, maybe we could not search the "inserted" entities,
        # since the search requests arrived query nodes earlier than query nodes consume the insert requests.
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp,
                            guarantee_timestamp=time_stamp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old + nb_new})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_different_data_distribution_with_index(self, _async):
        """
        target: test search different data distribution with index
        method: 1. connect to milvus
                2. create a collection
                3. insert data
                4. create an index
                5. Load and search
        expected: Range search successfully
        """
        # 1. connect, create collection and insert data
        dim = 100
        self._connect()
        collection_w = self.init_collection_general(
            prefix, False, dim=dim, is_index=False)[0]
        dataframe = cf.gen_default_dataframe_data(dim=dim, start=-1500)
        collection_w.insert(dataframe)

        # 2. create index
        index_param = {"index_type": "IVF_FLAT",
                       "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)

        # 3. load and range search
        collection_w.load()
        vectors = [[random.random() for _ in range(dim)]
                   for _ in range(default_nq)]
        range_search_params = {"metric_type": "L2", "params": {"radius": 1000,
                                                               "range_filter": 0}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("not fixed yet")
    @pytest.mark.parametrize("shards_num", [-256, 0, 1, 10, 31, 63])
    def test_range_search_with_non_default_shard_nums(self, shards_num, _async):
        """
        target: test range search with non_default shards_num
        method: connect milvus, create collection with several shard numbers , insert, load and search
        expected: search successfully with the non_default shards_num
        """
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
        # 5. range search
        vectors = [[random.random() for _ in range(default_dim)]
                   for _ in range(default_nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, default_limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "limit": default_limit,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", range_search_supported_indexes)
    def test_range_search_after_different_index_with_params(self, index):
        """
        target: test range search after different index
        method: test range search after different index and corresponding search params
        expected: search successfully with limit(topK)
        """
        # 1. initialize with data
        dim = 96
        enable_dynamic_field = False
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, 5000, partition_num=1,
                                         dim=dim, is_index=False, enable_dynamic_field=enable_dynamic_field)[0:5]
        # 2. create index and load
        params = cf.get_index_params_params(index)
        default_index = {"index_type": index, "params": params, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. range search
        search_params = cf.gen_search_param(index)
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            search_param["params"]["radius"] = 1000
            search_param["params"]["range_filter"] = 0
            if index.startswith("IVF_"):
                search_param["params"].pop("nprobe")
            if index == "SCANN":
                search_param["params"].pop("nprobe")
                search_param["params"].pop("reorder_k")
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", range_search_supported_indexes)
    def test_range_search_after_index_different_metric_type(self, index):
        """
        target: test range search with different metric type
        method: test range search with different metric type
        expected: searched successfully
        """
        if index == "SCANN":
            pytest.skip("https://github.com/milvus-io/milvus/issues/32648")
        # 1. initialize with data
        dim = 208
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, 5000,
                                                                                  partition_num=1,
                                                                                  dim=dim, is_index=False)[0:5]
        # 2. create different index
        params = cf.get_index_params_params(index)
        log.info("test_range_search_after_index_different_metric_type: Creating index-%s" % index)
        default_index = {"index_type": index, "params": params, "metric_type": "IP"}
        collection_w.create_index("float_vector", default_index)
        log.info("test_range_search_after_index_different_metric_type: Created index-%s" % index)
        collection_w.load()
        # 3. search
        search_params = cf.gen_search_param(index, "IP")
        vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
        for search_param in search_params:
            search_param["params"]["radius"] = 0
            search_param["params"]["range_filter"] = 1000
            if index.startswith("IVF_"):
                search_param["params"].pop("nprobe")
            log.info("Searching with search params: {}".format(search_param))
            collection_w.search(vectors[:default_nq], default_search_field,
                                search_param, default_limit,
                                default_search_exp,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": default_nq,
                                             "ids": insert_ids,
                                             "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_index_one_partition(self, _async):
        """
        target: test range search from partition
        method: search from one partition
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 3000
        auto_id = False
        collection_w, _, _, insert_ids, time_stamp = self.init_collection_general(prefix, True, nb,
                                                                                  partition_num=1,
                                                                                  auto_id=auto_id,
                                                                                  is_index=False)[0:5]

        # 2. create index
        default_index = {"index_type": "IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "L2"}
        collection_w.create_index("float_vector", default_index)
        collection_w.load()
        # 3. search in one partition
        log.info(
            "test_range_search_index_one_partition: searching (1000 entities) through one partition")
        limit = 1000
        par = collection_w.partitions
        if limit > par[1].num_entities:
            limit_check = par[1].num_entities
        else:
            limit_check = limit
        range_search_params = {"metric_type": "L2",
                               "params": {"radius": 1000, "range_filter": 0}}
        collection_w.search(vectors[:default_nq], default_search_field,
                            range_search_params, limit, default_search_exp,
                            [par[1].name], _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids[par[0].num_entities:],
                                         "limit": limit_check,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_range_search_binary_jaccard_flat_index(self, nq, _async, index, is_flush):
        """
        target: range search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with JACCARD
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        dim = 48
        auto_id = False
        collection_w, _, binary_raw_vector, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, 2, is_binary=True,
                                         auto_id=auto_id, dim=dim, is_index=False,
                                         is_flush=is_flush)[0:5]
        # 2. create index
        default_index = {"index_type": index, "params": {
            "nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(3000, dim)
        distance_0 = cf.jaccard(query_raw_vector[0], binary_raw_vector[0])
        distance_1 = cf.jaccard(query_raw_vector[0], binary_raw_vector[1])
        # 4. search and compare the distance
        search_params = {"metric_type": "JACCARD",
                         "params": {"radius": 1000, "range_filter": 0}}
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
    def test_range_search_binary_jaccard_invalid_params(self, index):
        """
        target: range search binary_collection with out of range params [0, 1]
        method: range search binary_collection with out of range params
        expected: return empty
        """
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, 2, is_binary=True,
                                         dim=default_dim, is_index=False, )[0:5]
        # 2. create index
        default_index = {"index_type": index, "params": {
            "nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(
            3000, default_dim)
        # 4. range search
        search_params = {"metric_type": "JACCARD",
                         "params": {"radius": -1, "range_filter": -10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})
        # 5. range search
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 10, "radius": 10,
                                                              "range_filter": 2}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_range_search_binary_hamming_flat_index(self, nq, _async, index, is_flush):
        """
        target: range search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with HAMMING
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        dim = 80
        auto_id = True
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
        search_params = {"metric_type": "HAMMING",
                         "params": {"radius": 1000, "range_filter": 0}}
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
    def test_range_search_binary_hamming_invalid_params(self, index):
        """
        target: range search binary_collection with out of range params
        method: range search binary_collection with out of range params
        expected: return empty
        """
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, 2, is_binary=True,
                                         dim=default_dim, is_index=False, )[0:5]
        # 2. create index
        default_index = {"index_type": index, "params": {
            "nlist": 128}, "metric_type": "HAMMING"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(
            3000, default_dim)
        # 4. range search
        search_params = {"metric_type": "HAMMING", "params": {"nprobe": 10, "radius": -1,
                                                              "range_filter": -10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("tanimoto obsolete")
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_range_search_binary_tanimoto_flat_index(self, _async, index, is_flush):
        """
        target: range search binary_collection, and check the result: distance
        method: compare the return distance value with value computed with TANIMOTO
        expected: the return distance equals to the computed value
        """
        # 1. initialize with binary data
        dim = 100
        auto_id = False
        collection_w, _, binary_raw_vector, insert_ids = self.init_collection_general(prefix, True, 2,
                                                                                      is_binary=True,
                                                                                      auto_id=auto_id,
                                                                                      dim=dim,
                                                                                      is_index=False,
                                                                                      is_flush=is_flush)[0:4]
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
        # 4. search
        search_params = {"metric_type": "TANIMOTO", "params": {"nprobe": 10}}
        res = collection_w.search(binary_vectors[:1], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async)[0]
        if _async:
            res.done()
            res = res.result()
        limit = 0
        radius = 1000
        range_filter = 0
        # filter the range search results to be compared
        for distance_single in res[0].distances:
            if radius > distance_single >= range_filter:
                limit += 1
        # 5. range search and compare the distance
        search_params = {"metric_type": "TANIMOTO", "params": {"radius": radius,
                                                               "range_filter": range_filter}}
        res = collection_w.search(binary_vectors[:1], "binary_vector",
                                  search_params, default_limit, "int64 >= 0",
                                  _async=_async,
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": 1,
                                               "ids": insert_ids,
                                               "limit": limit,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert abs(res[0].distances[0] -
                   min(distance_0, distance_1)) <= epsilon

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("tanimoto obsolete")
    @pytest.mark.parametrize("index", ["BIN_FLAT", "BIN_IVF_FLAT"])
    def test_range_search_binary_tanimoto_invalid_params(self, index):
        """
        target: range search binary_collection with out of range params [0,inf)
        method: range search binary_collection with out of range params
        expected: return empty
        """
        # 1. initialize with binary data
        collection_w, _, binary_raw_vector, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, 2, is_binary=True,
                                         dim=default_dim, is_index=False, )[0:5]
        # 2. create index
        default_index = {"index_type": index, "params": {
            "nlist": 128}, "metric_type": "JACCARD"}
        collection_w.create_index("binary_vector", default_index)
        collection_w.load()
        # 3. compute the distance
        query_raw_vector, binary_vectors = cf.gen_binary_vectors(
            3000, default_dim)
        # 4. range search
        search_params = {"metric_type": "JACCARD",
                         "params": {"radius": -1, "range_filter": -10}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": [],
                                         "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_binary_without_flush(self, metrics):
        """
        target: test range search without flush for binary data (no index)
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
        log.info("test_range_search_binary_without_flush: searching collection %s" %
                 collection_w.name)
        binary_vectors = cf.gen_binary_vectors(default_nq, default_dim)[1]
        search_params = {"metric_type": metrics, "params": {"radius": 1000,
                                                            "range_filter": 0}}
        collection_w.search(binary_vectors[:default_nq], "binary_vector",
                            search_params, default_limit,
                            default_search_exp,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": default_nq,
                                         "ids": insert_ids,
                                         "limit": default_limit})

    @pytest.mark.tags(CaseLabel.L1)
    def test_range_search_with_expression(self, enable_dynamic_field):
        """
        target: test range search with different expressions
        method: test range search with different expressions
        expected: searched successfully with correct limit(topK)
        """
        # 1. initialize with data
        nb = 2000
        dim = 200
        collection_w, _vectors, _, insert_ids = \
            self.init_collection_general(prefix, True, nb, dim=dim,
                                         is_index=False,  enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. create index
        index_param = {"index_type": "FLAT", "metric_type": "L2", "params": {}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # filter result with expression in collection
        _vectors = _vectors[0]
        for _async in [False, True]:
            for expressions in cf.gen_normal_expressions_and_templates():
                log.debug(f"test_range_search_with_expression: {expressions} with _async={_async}")
                expr = expressions[0].replace("&&", "and").replace("||", "or")
                filter_ids = []
                for i, _id in enumerate(insert_ids):
                    if enable_dynamic_field:
                        int64 = _vectors[i][ct.default_int64_field_name]
                        float = _vectors[i][ct.default_float_field_name]
                    else:
                        int64 = _vectors.int64[i]
                        float = _vectors.float[i]
                    if not expr or eval(expr):
                        filter_ids.append(_id)

                # 3. search with expression
                vectors = [[random.random() for _ in range(dim)] for _ in range(default_nq)]
                range_search_params = {"metric_type": "L2", "params": {"radius": 1000, "range_filter": 0}}
                search_res, _ = collection_w.search(vectors[:default_nq], default_search_field,
                                                    range_search_params, nb,
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
                                                    range_search_params, nb,
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
    def test_range_search_with_output_field(self, _async, enable_dynamic_field):
        """
        target: test range search with output fields
        method: range search with one output_field
        expected: search success
        """
        # 1. initialize with data
        auto_id = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      auto_id=auto_id,
                                                                      enable_dynamic_field=enable_dynamic_field)[0:4]
        # 2. search
        log.info("test_range_search_with_output_field: Searching collection %s" %
                 collection_w.name)
        range_search_params = {"metric_type": "COSINE", "params": {"radius": 0,
                                                                   "range_filter": 1000}}
        res = collection_w.search(vectors[:default_nq], default_search_field,
                                  range_search_params, default_limit,
                                  default_search_exp, _async=_async,
                                  output_fields=[default_int64_field_name],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": default_nq,
                                               "ids": insert_ids,
                                               "limit": default_limit,
                                               "_async": _async})[0]
        if _async:
            res.done()
            res = res.result()
        assert default_int64_field_name in res[0][0].fields

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_concurrent_multi_threads(self, nq, _async, null_data_percent):
        """
        target: test concurrent range search with multi-processes
        method: search with 10 processes, each process uses dependent connection
        expected: status ok and the returned vectors should be query_records
        """
        # 1. initialize with data
        threads_num = 10
        threads = []
        dim = 66
        auto_id = False
        nb = 4000
        collection_w, _, _, insert_ids, time_stamp = \
            self.init_collection_general(prefix, True, nb,  auto_id=auto_id, dim=dim,
                                         nullable_fields={ct.default_float_field_name: null_data_percent})[0:5]

        def search(collection_w):
            vectors = [[random.random() for _ in range(dim)]
                       for _ in range(nq)]
            range_search_params = {"metric_type": "COSINE", "params": {"radius": 0,
                                                                       "range_filter": 1000}}
            collection_w.search(vectors[:nq], default_search_field,
                                range_search_params, default_limit,
                                default_search_exp, _async=_async,
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": nq,
                                             "ids": insert_ids,
                                             "limit": default_limit,
                                             "_async": _async})

        # 2. search with multi-processes
        log.info("test_range_search_concurrent_multi_threads: searching with %s processes" % threads_num)
        for i in range(threads_num):
            t = threading.Thread(target=search, args=(collection_w,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("round_decimal", [0, 1, 2, 3, 4, 5, 6])
    def test_range_search_round_decimal(self, round_decimal):
        """
        target: test range search with valid round decimal
        method: range search with valid round decimal
        expected: search successfully
        """
        import math
        tmp_nb = 500
        tmp_nq = 1
        tmp_limit = 5
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True, nb=tmp_nb)[0]
        # 2. search
        log.info("test_search_round_decimal: Searching collection %s" %
                 collection_w.name)
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        res = collection_w.search(vectors[:tmp_nq], default_search_field,
                                  range_search_params, tmp_limit)[0]

        res_round = collection_w.search(vectors[:tmp_nq], default_search_field,
                                        range_search_params, tmp_limit, round_decimal=round_decimal)[0]

        abs_tol = pow(10, 1 - round_decimal)
        # log.debug(f'abs_tol: {abs_tol}')
        for i in range(tmp_limit):
            dis_expect = round(res[0][i].distance, round_decimal)
            dis_actual = res_round[0][i].distance
            # log.debug(f'actual: {dis_actual}, expect: {dis_expect}')
            # abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)
            assert math.isclose(dis_actual, dis_expect,
                                rel_tol=0, abs_tol=abs_tol)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("known issue #27518")
    def test_range_search_with_expression_large(self, dim):
        """
        target: test range search with large expression
        method: test range search with large expression
        expected: searched successfully
        """
        # 1. initialize with data
        nb = 10000
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True,
                                                                      nb, dim=dim,
                                                                      is_index=False)[0:4]

        # 2. create index
        index_param = {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 100}}
        collection_w.create_index("float_vector", index_param)
        collection_w.load()

        # 3. search with expression
        expression = f"0 < {default_int64_field_name} < 5001"
        log.info("test_search_with_expression: searching with expression: %s" % expression)

        nums = 5000
        vectors = [[random.random() for _ in range(dim)] for _ in range(nums)]
        # calculate the distance to make sure in range(0, 1000)
        search_params = {"metric_type": "L2"}
        search_res, _ = collection_w.search(vectors, default_search_field,
                                            search_params, 500, expression)
        for i in range(nums):
            if len(search_res[i]) < 10:
                assert False
            for j in range(len(search_res[i])):
                if search_res[i][j].distance < 0 or search_res[i][j].distance >= 1000:
                    assert False
        # range search
        range_search_params = {"metric_type": "L2", "params": {"radius": 1000, "range_filter": 0}}
        search_res, _ = collection_w.search(vectors, default_search_field,
                                            range_search_params, default_limit, expression)
        for i in range(nums):
            log.info(i)
            assert len(search_res[i]) == default_limit

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_with_consistency_bounded(self, nq, _async):
        """
        target: test range search with different consistency level
        method: 1. create a collection
                2. insert data
                3. range search with consistency_level is "bounded"
        expected: searched successfully
        """
        limit = 1000
        nb_old = 500
        dim = 200
        auto_id = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
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
                                                    insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)

        collection_w.search(vectors[:nq], default_search_field,
                            default_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs,
                            )

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_with_consistency_strong(self, nq, _async):
        """
        target: test range search with different consistency level
        method: 1. create a collection
                2. insert data
                3. range search with consistency_level is "Strong"
        expected: searched successfully
        """
        limit = 1000
        nb_old = 500
        dim = 100
        auto_id = True
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})

        nb_new = 400
        _, _, _, insert_ids_new, _ = cf.insert_data(collection_w, nb_new,
                                                    auto_id=auto_id, dim=dim,
                                                    insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)
        kwargs = {}
        consistency_level = kwargs.get("consistency_level", CONSISTENCY_STRONG)
        kwargs.update({"consistency_level": consistency_level})
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old + nb_new,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_with_consistency_eventually(self, nq, _async):
        """
        target: test range search with different consistency level
        method: 1. create a collection
                2. insert data
                3. range search with consistency_level is "eventually"
        expected: searched successfully
        """
        limit = 1000
        nb_old = 500
        dim = 128
        auto_id = False
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        range_search_params = {"metric_type": "COSINE", "params": {
            "nprobe": 10, "radius": 0, "range_filter": 1000}}
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp, _async=_async,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old,
                                         "_async": _async})
        nb_new = 400
        _, _, _, insert_ids_new, _ = cf.insert_data(collection_w, nb_new,
                                                    auto_id=auto_id, dim=dim,
                                                    insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)
        kwargs = {}
        consistency_level = kwargs.get(
            "consistency_level", CONSISTENCY_EVENTUALLY)
        kwargs.update({"consistency_level": consistency_level})
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs
                            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_range_search_with_consistency_session(self, nq, dim, auto_id, _async):
        """
        target: test range search with different consistency level
        method: 1. create a collection
                2. insert data
                3. range search with consistency_level is "session"
        expected: searched successfully
        """
        limit = 1000
        nb_old = 500
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, nb_old,
                                                                      auto_id=auto_id,
                                                                      dim=dim)[0:4]
        # 2. search for original data after load
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        range_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10, "radius": 0,
                                                                   "range_filter": 1000}}
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
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
                                                    insert_offset=nb_old)
        insert_ids.extend(insert_ids_new)
        collection_w.search(vectors[:nq], default_search_field,
                            range_search_params, limit,
                            default_search_exp, _async=_async,
                            **kwargs,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids,
                                         "limit": nb_old + nb_new,
                                         "_async": _async})

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_sparse(self):
        """
        target: test sparse index normal range search
        method: create connection, collection, insert and range search
        expected: range search successfully
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True, nb=5000,
                                                    with_json=True,
                                                    vector_data_type=DataType.SPARSE_FLOAT_VECTOR)[0]
        range_filter = random.uniform(0.5, 1)
        radius = random.uniform(0, 0.5)

        # 2. range search
        range_search_params = {"metric_type": "IP",
                               "params": {"radius": radius, "range_filter": range_filter}}
        d = cf.gen_default_list_sparse_data(nb=1)
        search_res = collection_w.search(d[-1][-1:], ct.default_sparse_vec_field_name,
                                         range_search_params, default_limit,
                                         default_search_exp)[0]

        # 3. check search results
        for hits in search_res:
            for distance in hits.distances:
                assert range_filter >= distance > radius
