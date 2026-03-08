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


class TestSearchArray(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("array_element_data_type", [DataType.INT64])
    def test_search_array_with_inverted_index(self, array_element_data_type):
        # create collection
        additional_params = {"max_length": 1000} if array_element_data_type == DataType.VARCHAR else {}
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="contains", dtype=DataType.ARRAY, element_type=array_element_data_type, max_capacity=2000,
                        **additional_params),
            FieldSchema(name="contains_any", dtype=DataType.ARRAY, element_type=array_element_data_type,
                        max_capacity=2000, **additional_params),
            FieldSchema(name="contains_all", dtype=DataType.ARRAY, element_type=array_element_data_type,
                        max_capacity=2000, **additional_params),
            FieldSchema(name="equals", dtype=DataType.ARRAY, element_type=array_element_data_type, max_capacity=2000,
                        **additional_params),
            FieldSchema(name="array_length_field", dtype=DataType.ARRAY, element_type=array_element_data_type,
                        max_capacity=2000, **additional_params),
            FieldSchema(name="array_access", dtype=DataType.ARRAY, element_type=array_element_data_type,
                        max_capacity=2000, **additional_params),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=128)
        ]
        schema = CollectionSchema(fields=fields, description="test collection", enable_dynamic_field=True)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        # insert data
        train_data, query_expr = cf.prepare_array_test_data(3000, hit_rate=0.05)
        collection_w.insert(train_data)
        index_params = {"metric_type": "L2", "index_type": "HNSW", "params": {"M": 48, "efConstruction": 500}}
        collection_w.create_index("emb", index_params=index_params)
        for f in ["contains", "contains_any", "contains_all", "equals", "array_length_field", "array_access"]:
            collection_w.create_index(f, {"index_type": "INVERTED"})
        collection_w.load()

        for item in query_expr:
            expr = item["expr"]
            ground_truth_candidate = item["ground_truth"]
            res, _ = collection_w.search(
                data=[np.array([random.random() for j in range(128)], dtype=np.dtype("float32"))],
                anns_field="emb",
                param={"metric_type": "L2", "params": {"M": 32, "efConstruction": 360}},
                limit=10,
                expr=expr,
                output_fields=["*"],
            )
            assert len(res) == 1
            for i in range(len(res)):
                assert len(res[i]) == 10
                for hit in res[i]:
                    assert hit.id in ground_truth_candidate
