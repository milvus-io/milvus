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


class TestCollectionLoadOperation(TestcaseBase):
    """ Test case of search combining load and other functions """

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_load_collection_release_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. delete half data in each partition
                4. load the collection
                5. release one partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # load && release
        collection_w.load()
        partition_w1.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_load_collection_release_collection(self):
        """
        target: test delete load collection release collection
        method: 1. create a collection and 2 partitions
                2. insert data
                3. delete half data in each partition
                4. load the collection
                5. release the collection
                6. load one partition
                7. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # load && release
        collection_w.load()
        collection_w.release()
        partition_w2.load()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_load_partition_release_collection(self):
        """
        target: test delete load partition release collection
        method: 1. create a collection and 2 partitions
                2. insert data
                3. delete half data in each partition
                4. load one partition
                5. release the collection
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # load && release
        partition_w1.load()
        collection_w.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_release_collection_load_partition(self):
        """
        target: test delete load collection release collection
        method: 1. create a collection and 2 partitions
                2. insert data
                3. delete half data in each partition
                4. load one partition
                5. release the collection
                6. load the other partition
                7. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # load && release
        partition_w1.load()
        collection_w.release()
        partition_w2.load()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})

    @pytest.mark.tags(CaseLabel.L2)
    def test_delete_load_partition_drop_partition(self):
        """
        target: test delete load partition drop partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. delete half data in each partition
                4. load one partition
                5. release the collection
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, default_index_params)
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # load && release
        partition_w2.load()
        partition_w2.release()
        partition_w2.drop()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not found'})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_collection_delete_release_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the collection
                4. delete half data in each partition
                5. release one partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load
        collection_w.load()
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # release
        partition_w1.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[
                                partition_w1.name, partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_partition_delete_release_collection(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. delete half data in each partition
                5. release the collection and load one partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, default_index_params)
        # load
        partition_w1.load()
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # release
        collection_w.release()
        partition_w1.load()
        # search on collection, partition1, partition2
        collection_w.query(expr='', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": [{ct.default_count_output: 50}]})
        partition_w1.query(expr='', output_fields=[ct.default_count_output],
                           check_task=CheckTasks.check_query_results,
                           check_items={"exp_res": [{ct.default_count_output: 50}]})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_delete_drop_partition(self):
        """
        target: test load partition delete drop partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. delete half data in each partition
                5. drop the non-loaded partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load
        partition_w1.load()
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # release
        partition_w2.drop()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not found'})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_collection_release_partition_delete(self):
        """
        target: test load collection release partition delete
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the collection
                4. release one partition
                5. delete half data in each partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load && release
        collection_w.load()
        partition_w1.release()
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_release_collection_delete(self):
        """
        target: test load partition release collection delete
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. release the collection
                5. delete half data in each partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load && release
        partition_w1.load()
        collection_w.release()
        # delete data
        delete_ids = [i for i in range(50, 150)]
        collection_w.delete(f"int64 in {delete_ids}")
        collection_w.load()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[
                                partition_w1.name, partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 50})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_drop_partition_delete(self):
        """
        target: test load partition drop partition delete
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. release and drop the partition
                5. delete half data in each partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_wrap(name=prefix)
        p1_name = cf.gen_unique_str("par1")
        partition_w1 = self.init_partition_wrap(collection_w, name=p1_name)
        p2_name = cf.gen_unique_str("par2")
        partition_w2 = self.init_partition_wrap(collection_w, name=p2_name)
        collection_w.create_index(default_search_field, default_index_params)
        # load && release
        partition_w2.load()
        partition_w2.release()
        partition_w2.drop()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 10,
                            partition_names=[partition_w1.name, partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 999, ct.err_msg: f'partition name {partition_w2.name} not found'})
        collection_w.search(vectors[:1], field_name, default_search_params, 10,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 999, ct.err_msg: 'failed to search: collection not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 10,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 999, ct.err_msg: f'partition name {partition_w2.name} not found'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_load_collection_release_partition(self):
        """
        target: test compact load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. compact
                4. load the collection
                5. release one partition
                6. search
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix, partition_num=1)[0]
        partition_w1, partition_w2 = collection_w.partitions
        df = cf.gen_default_dataframe_data()
        # insert data
        partition_w1.insert(df[:100])
        partition_w1.insert(df[100:200])
        partition_w2.insert(df[200:300])
        # compact
        collection_w.compact()
        collection_w.get_compaction_state()
        # load && release
        collection_w.load()
        partition_w1.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_load_collection_release_collection(self):
        """
        target: test compact load collection release collection
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. compact
                4. load the collection
                5. release the collection
                6. load one partition
                7. search
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix, partition_num=1)[0]
        partition_w1, partition_w2 = collection_w.partitions
        df = cf.gen_default_dataframe_data()
        # insert data
        partition_w1.insert(df[:100])
        partition_w1.insert(df[100:200])
        partition_w2.insert(df[200:300])
        # compact
        collection_w.compact()
        collection_w.get_compaction_state()
        # load && release
        collection_w.load()
        collection_w.release()
        partition_w1.load()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[
                                partition_w1.name, partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 200})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_load_partition_release_collection(self):
        """
        target: test compact load partition release collection
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. compact
                4. load one partition
                5. release the collection
                6. search
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix, partition_num=1)[0]
        partition_w1, partition_w2 = collection_w.partitions
        df = cf.gen_default_dataframe_data()
        # insert data
        partition_w1.insert(df[:100])
        partition_w1.insert(df[100:200])
        partition_w2.insert(df[200:300])
        # compact
        collection_w.compact()
        collection_w.get_compaction_state()
        # load && release
        partition_w2.load()
        collection_w.release()
        partition_w1.load()
        partition_w2.load()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 300})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 200})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_compact_drop_partition(self):
        """
        target: test load collection compact drop partition
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. load the collection
                4. compact
                5. release one partition and drop
                6. search
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix, partition_num=1)[0]
        partition_w1, partition_w2 = collection_w.partitions
        df = cf.gen_default_dataframe_data()
        # insert data
        partition_w1.insert(df[:100])
        partition_w1.insert(df[100:200])
        partition_w2.insert(df[200:300])
        # load
        collection_w.load()
        # compact
        collection_w.compact()
        collection_w.get_compaction_state()
        # release
        partition_w2.release()
        partition_w2.drop()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 200})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 200})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not found'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_compact_release_collection(self):
        """
        target: test load partition compact release collection
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. load one partition
                4. compact
                5. release the collection
                6. search
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix, partition_num=1)[0]
        partition_w1, partition_w2 = collection_w.partitions
        df = cf.gen_default_dataframe_data()
        # insert data
        partition_w1.insert(df[:100])
        partition_w1.insert(df[100:200])
        partition_w2.insert(df[200:300])
        # load
        partition_w2.load()
        # compact
        collection_w.compact()
        collection_w.get_compaction_state()
        # release
        collection_w.release()
        partition_w2.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_collection_release_partition_compact(self):
        """
        target: test load collection release partition compact
        method: 1. create a collection and 2 partitions
                2. insert data multi times
                3. load the collection
                4. release one partition
                5. compact
                6. search
        expected: No exception
        """
        collection_w = self.init_collection_general(prefix, partition_num=1)[0]
        partition_w1, partition_w2 = collection_w.partitions
        df = cf.gen_default_dataframe_data()
        # insert data
        partition_w1.insert(df[:100])
        partition_w1.insert(df[100:200])
        partition_w2.insert(df[200:300])
        # load && release
        collection_w.load()
        partition_w1.release()
        # compact
        collection_w.compact()
        collection_w.get_compaction_state()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 300,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_load_collection_release_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. flush
                4. load the collection
                5. release one partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # flush
        collection_w.flush()
        # load && release
        collection_w.load()
        partition_w1.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_load_collection_release_collection(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. flush
                4. load the collection
                5. release the collection
                6. load one partition
                7. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # flush
        collection_w.flush()
        # load && release
        collection_w.load()
        collection_w.release()
        partition_w2.load()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_load_partition_release_collection(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. flush
                4. load one partition
                5. release the collection
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # flush
        collection_w.flush()
        # load && release
        partition_w2.load()
        collection_w.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_load_partition_drop_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. flush
                4. load one partition
                5. release and drop the partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, default_index_params)
        # flush
        collection_w.flush()
        # load && release
        partition_w2.load()
        partition_w2.release()
        partition_w2.drop()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[
                                partition_w1.name, partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not found'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not found'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_flush_load_collection_drop_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. flush
                4. load collection
                5. release and drop one partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # flush
        collection_w.flush()
        # load && release
        collection_w.load()
        partition_w2.release()
        partition_w2.drop()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not found'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_flush_release_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the collection
                4. flush
                5. search on the collection -> len(res)==200
                5. release one partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load
        collection_w.load()
        # flush
        collection_w.flush()
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 200})
        # release
        partition_w2.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_flush_release_collection(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. flush
                5. release the collection
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, default_index_params)
        # load
        partition_w2.load()
        # flush
        collection_w.flush()
        # release
        collection_w.release()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[
                                partition_w1.name, partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_flush_release_partition(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. flush
                5. drop the non-loaded partition
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load
        partition_w1.load()
        # flush
        collection_w.flush()
        # release
        partition_w2.drop()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not found'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_release_partition_flush(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the collection
                4. release one partition
                5. flush
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load && release
        collection_w.load()
        partition_w2.release()
        # flush
        collection_w.flush()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_release_collection_flush(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the collection
                4. release the collection
                5. load one partition
                6. flush
                7. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load && release
        collection_w.load()
        collection_w.release()
        partition_w2.load()
        # flush
        collection_w.flush()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[
                                partition_w1.name, partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_partition_release_collection_flush(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load the partition
                4. release the collection
                5. flush
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, default_index_params)
        # load && release
        partition_w2.load()
        collection_w.release()
        # flush
        collection_w.flush()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})

    @pytest.mark.tags(CaseLabel.L1)
    def test_load_partition_drop_partition_flush(self):
        """
        target: test delete load collection release partition
        method: 1. create a collection and 2 partitions
                2. insert data
                3. load one partition
                4. release and drop the partition
                5. flush
                6. search
        expected: No exception
        """
        # insert data
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, default_index_params)
        # load && release
        partition_w2.load()
        partition_w2.release()
        partition_w2.drop()
        # flush
        collection_w.flush()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not found'})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_release_collection_multi_times(self):
        """
        target: test load and release multiple times
        method: 1. create a collection and 2 partitions
                2. load and release multiple times
                3. search
        expected: No exception
        """
        # init the collection
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load and release
        for i in range(5):
            collection_w.release()
            partition_w2.load()
        # search on collection, partition1, partition2
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w1.name],
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: 'not loaded'})
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            partition_names=[partition_w2.name],
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_collection_release_all_partitions(self):
        """
        target: test load and release all partitions
        method: 1. create a collection and 2 partitions
                2. load collection and release all partitions
                3. search
        expected: No exception
        """
        # init the collection
        collection_w = self.init_collection_general(prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, default_index_params)
        # load and release
        collection_w.load()
        partition_w1.release()
        partition_w2.release()
        # search on collection
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 65535,
                                         ct.err_msg: "collection not loaded"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="issue #24446")
    def test_search_load_collection_create_partition(self):
        """
        target: test load collection and create partition and search
        method: 1. create a collection and 2 partitions
                2. load collection and create a partition
                3. search
        expected: No exception
        """
        # init the collection
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load and release
        collection_w.load()
        partition_w3 = collection_w.create_partition("_default3")[0]
        # search on collection
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 200})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_load_partition_create_partition(self):
        """
        target: test load partition and create partition and search
        method: 1. create a collection and 2 partitions
                2. load partition and create a partition
                3. search
        expected: No exception
        """
        # init the collection
        collection_w = self.init_collection_general(
            prefix, True, 200, partition_num=1, is_index=False)[0]
        partition_w1, partition_w2 = collection_w.partitions
        collection_w.create_index(default_search_field, ct.default_flat_index)
        # load and release
        partition_w1.load()
        partition_w3 = collection_w.create_partition("_default3")[0]
        # search on collection
        collection_w.search(vectors[:1], field_name, default_search_params, 200,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": 1, "limit": 100})
