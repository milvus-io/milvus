import random
from sklearn import preprocessing
import numpy as np
import sys
import json
import time
from utils import constant
from utils.utils import gen_collection_name
from utils.util_log import test_log as logger
import pytest
from base.testbase import TestBase
from utils.utils import (gen_unique_str, get_data_by_payload, get_common_fields_by_data, gen_vector)
from pymilvus import (
    Collection, utility
)


@pytest.mark.L0
class TestSearchVectorWithPartitionIsolation(TestBase):

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [16])
    def test_search_vector_with_all_vector_datatype(self, nb, dim, insert_round, auto_id,
                                                      is_partition_key, enable_dynamic_schema):
        """
        Insert a vector with a simple payload
        """
        # create a collection
        name = gen_collection_name()
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": auto_id,
                "enableDynamicField": enable_dynamic_schema,
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "user_id", "dataType": "Int64", "isPartitionKey": is_partition_key,
                     "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "float_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "float16_vector", "dataType": "Float16Vector",
                     "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "bfloat16_vector", "dataType": "BFloat16Vector",
                     "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "binary_vector", "dataType": "BinaryVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [
                {"fieldName": "float_vector", "indexName": "float_vector", "metricType": "COSINE"},
                {"fieldName": "float16_vector", "indexName": "float16_vector", "metricType": "COSINE"},
                {"fieldName": "bfloat16_vector", "indexName": "bfloat16_vector", "metricType": "COSINE"},
                {"fieldName": "binary_vector", "indexName": "binary_vector", "metricType": "HAMMING",
                 "indexConfig": {"index_type": "BIN_IVF_FLAT", "nlist": "512"}}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 200
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 200
        # insert data
        for i in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": i%100,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "float_vector": gen_vector(datatype="FloatVector", dim=dim),
                        "float16_vector": gen_vector(datatype="Float16Vector", dim=dim),
                        "bfloat16_vector": gen_vector(datatype="BFloat16Vector", dim=dim),
                        "binary_vector": gen_vector(datatype="BinaryVector", dim=dim)
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i%100,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "float_vector": gen_vector(datatype="FloatVector", dim=dim),
                        "float16_vector": gen_vector(datatype="Float16Vector", dim=dim),
                        "bfloat16_vector": gen_vector(datatype="BFloat16Vector", dim=dim),
                        "binary_vector": gen_vector(datatype="BinaryVector", dim=dim)
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 200
            assert rsp['data']['insertCount'] == nb
        # search data
        payload = {
            "collectionName": name,
            "data": [gen_vector(datatype="FloatVector", dim=dim)],
            "annsField": "float_vector",
            "filter": "word_count > 100",
            "groupingField": "user_id",
            "outputFields": ["*"],
            "searchParams": {
                "metricType": "COSINE",
                "params": {
                    "radius": "0.1",
                    "range_filter": "0.8"
                }
            },
            "limit": 100,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 200
