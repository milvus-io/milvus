import random
from sklearn import preprocessing
import numpy as np
import pandas as pd
import sys
import json
import time
from utils import constant
from utils.utils import gen_collection_name, get_sorted_distance, patch_faker_text, en_vocabularies_distribution, zh_vocabularies_distribution
from utils.util_log import test_log as logger
import pytest
from base.testbase import TestBase
from utils.utils import (gen_unique_str, get_data_by_payload, get_common_fields_by_data, gen_vector, analyze_documents)
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
    Collection, utility
)
from faker import Faker
Faker.seed(19530)
fake_en = Faker("en_US")
fake_zh = Faker("zh_CN")

patch_faker_text(fake_en, en_vocabularies_distribution)
patch_faker_text(fake_zh, zh_vocabularies_distribution)


@pytest.mark.L0
class TestInsertVector(TestBase):

    @pytest.mark.parametrize("insert_round", [3])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    def test_insert_entities_with_simple_payload(self, nb, dim, insert_round):
        """
        Insert a vector with a simple payload
        """
        # create a collection
        name = gen_collection_name()
        collection_payload = {
            "collectionName": name,
            "dimension": dim,
            "metricType": "L2"
        }
        rsp = self.collection_client.collection_create(collection_payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = get_data_by_payload(collection_payload, nb)
            payload = {
                "collectionName": name,
                "data": data,
            }
            body_size = sys.getsizeof(json.dumps(payload))
            logger.info(f"body size: {body_size / 1024 / 1024} MB")
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("is_partition_key", [True, False])
    @pytest.mark.parametrize("enable_dynamic_schema", [True, False])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    def test_insert_entities_with_all_scalar_datatype(self, nb, dim, insert_round, auto_id,
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
                    {"fieldName": "bool", "dataType": "Bool", "elementTypeParams": {}},
                    {"fieldName": "json", "dataType": "JSON", "elementTypeParams": {}},
                    {"fieldName": "int_array", "dataType": "Array", "elementDataType": "Int64",
                     "elementTypeParams": {"max_capacity": "1024"}},
                    {"fieldName": "varchar_array", "dataType": "Array", "elementDataType": "VarChar",
                     "elementTypeParams": {"max_capacity": "1024", "max_length": "256"}},
                    {"fieldName": "bool_array", "dataType": "Array", "elementDataType": "Bool",
                     "elementTypeParams": {"max_capacity": "1024"}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "image_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [
                {"fieldName": "text_emb", "indexName": "text_emb", "metricType": "L2"},
                {"fieldName": "image_emb", "indexName": "image_emb", "metricType": "L2"}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "bool": random.choice([True, False]),
                        "json": {"key": i},
                        "int_array": [i],
                        "varchar_array": [f"varchar_{i}"],
                        "bool_array": [random.choice([True, False])],
                        "text_emb": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[
                            0].tolist(),
                        "image_emb": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[
                            0].tolist(),
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "bool": random.choice([True, False]),
                        "json": {"key": i},
                        "int_array": [i],
                        "varchar_array": [f"varchar_{i}"],
                        "bool_array": [random.choice([True, False])],
                        "text_emb": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[
                            0].tolist(),
                        "image_emb": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[
                            0].tolist(),
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        # query data to make sure the data is inserted
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "user_id > 0", "limit": 50})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 50

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("pass_fp32_to_fp16_or_bf16", [True, False])
    def test_insert_entities_with_all_vector_datatype(self, nb, dim, insert_round, auto_id,
                                                      is_partition_key, enable_dynamic_schema,
                                                      pass_fp32_to_fp16_or_bf16):
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
                {"fieldName": "float_vector", "indexName": "float_vector", "metricType": "L2"},
                {"fieldName": "float16_vector", "indexName": "float16_vector", "metricType": "L2"},
                {"fieldName": "bfloat16_vector", "indexName": "bfloat16_vector", "metricType": "L2"},
                {"fieldName": "binary_vector", "indexName": "binary_vector", "metricType": "HAMMING",
                 "params": {"index_type": "BIN_IVF_FLAT", "nlist": "512"}}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "float_vector": gen_vector(datatype="FloatVector", dim=dim),
                        "float16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="Float16Vector", dim=dim)
                        ),
                        "bfloat16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="BFloat16Vector", dim=dim)
                        ),
                        "binary_vector": gen_vector(datatype="BinaryVector", dim=dim),
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "float_vector": gen_vector(datatype="FloatVector", dim=dim),
                        "float16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="Float16Vector", dim=dim)
                        ),
                        "bfloat16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="BFloat16Vector", dim=dim)
                        ),
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
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        c = Collection(name)
        res = c.query(
            expr="user_id > 0",
            limit=1,
            output_fields=["*"],
        )
        logger.info(f"res: {res}")
        # query data to make sure the data is inserted
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "user_id > 0", "limit": 50})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 50

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("pass_fp32_to_fp16_or_bf16", [True, False])
    def test_insert_entities_with_all_vector_datatype_0(self, nb, dim, insert_round, auto_id,
                                                        is_partition_key, enable_dynamic_schema,
                                                        pass_fp32_to_fp16_or_bf16):
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
                    {"fieldName": "book_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "float_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "float16_vector", "dataType": "Float16Vector",
                     "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "bfloat16_vector", "dataType": "BFloat16Vector",
                     "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [
                {"fieldName": "book_vector", "indexName": "book_vector", "metricType": "L2",
                 "params": {"index_type": "FLAT"}},
                {"fieldName": "float_vector", "indexName": "float_vector", "metricType": "L2",
                 "params": {"index_type": "IVF_FLAT", "nlist": 128}},
                {"fieldName": "float16_vector", "indexName": "float16_vector", "metricType": "L2",
                 "params": {"index_type": "IVF_SQ8", "nlist": "128"}},
                {"fieldName": "bfloat16_vector", "indexName": "bfloat16_vector", "metricType": "L2",
                 "params": {"index_type": "IVF_PQ", "nlist": 128, "m": 16, "nbits": 8}},
            ]
        }

        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "book_vector": gen_vector(datatype="FloatVector", dim=dim),
                        "float_vector": gen_vector(datatype="FloatVector", dim=dim),
                        "float16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="Float16Vector", dim=dim)
                        ),
                        "bfloat16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="BFloat16Vector", dim=dim)
                        ),
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "book_vector": gen_vector(datatype="FloatVector", dim=dim),
                        "float_vector": gen_vector(datatype="FloatVector", dim=dim),
                        "float16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="Float16Vector", dim=dim)
                        ),
                        "bfloat16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="BFloat16Vector", dim=dim)
                        ),
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        c = Collection(name)
        res = c.query(
            expr="user_id > 0",
            limit=1,
            output_fields=["*"],
        )
        logger.info(f"res: {res}")
        # query data to make sure the data is inserted
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "user_id > 0", "limit": 50})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 50

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("pass_fp32_to_fp16_or_bf16", [True, False])
    def test_insert_entities_with_all_vector_datatype_1(self, nb, dim, insert_round, auto_id,
                                                      is_partition_key, enable_dynamic_schema,
                                                        pass_fp32_to_fp16_or_bf16):
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
                ]
            },
            "indexParams": [
                {"fieldName": "float_vector", "indexName": "float_vector", "metricType": "L2",
                 "params": {"index_type": "HNSW", "M": 32, "efConstruction": 360}},
                {"fieldName": "float16_vector", "indexName": "float16_vector", "metricType": "L2",
                 "params": {"index_type": "SCANN", "nlist": "128"}},
                {"fieldName": "bfloat16_vector", "indexName": "bfloat16_vector", "metricType": "L2",
                 "params": {"index_type": "DISKANN"}},
            ]
        }

        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "float_vector": gen_vector(datatype="FloatVector", dim=dim),
                        "float16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="Float16Vector", dim=dim)
                        ),
                        "bfloat16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="BFloat16Vector", dim=dim)
                        ),
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "float_vector": gen_vector(datatype="FloatVector", dim=dim),
                        "float16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="Float16Vector", dim=dim)
                        ),
                        "bfloat16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="BFloat16Vector", dim=dim)
                        ),
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        c = Collection(name)
        res = c.query(
            expr="user_id > 0",
            limit=1,
            output_fields=["*"],
        )
        logger.info(f"res: {res}")
        # query data to make sure the data is inserted
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "user_id > 0", "limit": 50})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 50

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    def test_insert_entities_with_all_vector_datatype_2(self, nb, dim, insert_round, auto_id,
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
                    {"fieldName": "binary_vector_0", "dataType": "BinaryVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "binary_vector_1", "dataType": "BinaryVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "sparse_float_vector_0", "dataType": "SparseFloatVector"},
                    {"fieldName": "sparse_float_vector_1", "dataType": "SparseFloatVector"},
                ]
            },
            "indexParams": [
                {"fieldName": "binary_vector_0", "indexName": "binary_vector_0_index", "metricType": "HAMMING",
                 "params": {"index_type": "BIN_FLAT"}},
                {"fieldName": "binary_vector_1", "indexName": "binary_vector_1_index", "metricType": "HAMMING",
                 "params": {"index_type": "BIN_IVF_FLAT", "nlist": "512"}},
                {"fieldName": "sparse_float_vector_0", "indexName": "sparse_float_vector_0_index", "metricType": "IP",
                 "params": {"index_type": "SPARSE_INVERTED_INDEX", "drop_ratio_build": "0.2"}},
                {"fieldName": "sparse_float_vector_1", "indexName": "sparse_float_vector_1_index", "metricType": "IP",
                 "params": {"index_type": "SPARSE_WAND", "drop_ratio_build": "0.2"}}
            ]
        }

        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "binary_vector_0": gen_vector(datatype="BinaryVector", dim=dim),
                        "binary_vector_1": gen_vector(datatype="BinaryVector", dim=dim),
                        "sparse_float_vector_0": gen_vector(datatype="SparseFloatVector", dim=dim, sparse_format="dok"),
                        "sparse_float_vector_1": gen_vector(datatype="SparseFloatVector", dim=dim, sparse_format="dok"),
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "binary_vector_0": gen_vector(datatype="BinaryVector", dim=dim),
                        "binary_vector_1": gen_vector(datatype="BinaryVector", dim=dim),
                        "sparse_float_vector_0": gen_vector(datatype="SparseFloatVector", dim=dim, sparse_format="dok"),
                        "sparse_float_vector_1": gen_vector(datatype="SparseFloatVector", dim=dim, sparse_format="dok"),
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        c = Collection(name)
        res = c.query(
            expr="user_id > 0",
            limit=1,
            output_fields=["*"],
        )
        logger.info(f"res: {res}")
        # query data to make sure the data is inserted
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "user_id > 0", "limit": 50})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 50

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("is_partition_key", [True, False])
    @pytest.mark.parametrize("enable_dynamic_schema", [True, False])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    def test_insert_entities_with_all_json_datatype(self, nb, dim, insert_round, auto_id,
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
                    {"fieldName": "bool", "dataType": "Bool", "elementTypeParams": {}},
                    {"fieldName": "json", "dataType": "JSON", "elementTypeParams": {}},
                    {"fieldName": "int_array", "dataType": "Array", "elementDataType": "Int64",
                     "elementTypeParams": {"max_capacity": "1024"}},
                    {"fieldName": "varchar_array", "dataType": "Array", "elementDataType": "VarChar",
                     "elementTypeParams": {"max_capacity": "1024", "max_length": "256"}},
                    {"fieldName": "bool_array", "dataType": "Array", "elementDataType": "Bool",
                     "elementTypeParams": {"max_capacity": "1024"}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "image_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [
                {"fieldName": "text_emb", "indexName": "text_emb", "metricType": "L2"},
                {"fieldName": "image_emb", "indexName": "image_emb", "metricType": "L2"}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        json_value = [
            1,
            1.0,
            "1",
            [1, 2, 3],
            ["1", "2", "3"],
            [1, 2, "3"],
            {"key": "value"},
        ]
        # insert data
        for i in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "bool": random.choice([True, False]),
                        "json": json_value[i%len(json_value)],
                        "int_array": [i],
                        "varchar_array": [f"varchar_{i}"],
                        "bool_array": [random.choice([True, False])],
                        "text_emb": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[
                            0].tolist(),
                        "image_emb": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[
                            0].tolist(),
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "bool": random.choice([True, False]),
                        "json": json_value[i%len(json_value)],
                        "int_array": [i],
                        "varchar_array": [f"varchar_{i}"],
                        "bool_array": [random.choice([True, False])],
                        "text_emb": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[
                            0].tolist(),
                        "image_emb": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[
                            0].tolist(),
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        # query data to make sure the data is inserted
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "user_id > 0", "limit": 50})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 50

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("is_partition_key", [True, False])
    @pytest.mark.parametrize("enable_dynamic_schema", [True, False])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    def test_insert_entities_with_default_none(self, nb, dim, insert_round, auto_id, is_partition_key,
                                               enable_dynamic_schema):
        """
        Insert a vector with defaultValue and none
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
                     "elementTypeParams": {}, "defaultValue": 10},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}, "nullable": True},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"},
                     "defaultValue": "default", "nullable": True},
                    {"fieldName": "json", "dataType": "JSON", "elementTypeParams": {}, "nullable": True},
                    {"fieldName": "varchar_array", "dataType": "Array", "elementDataType": "VarChar",
                     "elementTypeParams": {"max_capacity": "1024", "max_length": "256"}, "nullable": True},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [
                {"fieldName": "text_emb", "indexName": "text_emb", "metricType": "L2"},
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for k in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": None,
                        "word_count": None,
                        "book_describe": None,
                        "json": None,
                        "varchar_array": None,
                        "text_emb": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[0].tolist(),
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": None,
                        "word_count": None,
                        "book_describe": None,
                        "json": None,
                        "varchar_array": None,
                        "text_emb": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[0].tolist(),
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        # query data to make sure the data is inserted
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "user_id > 0", "limit": 5})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 5
        assert rsp['data'][0]['book_describe'] == 'default'
        assert rsp['data'][0]['word_count'] is None
        assert rsp['data'][0]['json'] is None

@pytest.mark.L0
class TestInsertVectorNegative(TestBase):

    def test_insert_vector_with_invalid_collection_name(self):
        """
        Insert a vector with an invalid collection name
        """

        # create a collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        assert rsp['code'] == 0
        # insert data
        nb = 100
        data = get_data_by_payload(payload, nb)
        payload = {
            "collectionName": "invalid_collection_name",
            "data": data,
        }
        body_size = sys.getsizeof(json.dumps(payload))
        logger.info(f"body size: {body_size / 1024 / 1024} MB")
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 100
        assert "can't find collection" in rsp['message']

    def test_insert_vector_with_invalid_database_name(self):
        """
        Insert a vector with an invalid database name
        """
        # create a collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        assert rsp['code'] == 0
        # insert data
        nb = 10
        data = get_data_by_payload(payload, nb)
        payload = {
            "collectionName": name,
            "data": data,
        }
        body_size = sys.getsizeof(json.dumps(payload))
        logger.info(f"body size: {body_size / 1024 / 1024} MB")
        success = False
        rsp = self.vector_client.vector_insert(payload, db_name="invalid_database")
        assert rsp['code'] == 800

    def test_insert_vector_with_mismatch_dim(self):
        """
        Insert a vector with mismatch dim
        """
        # create a collection
        name = gen_collection_name()
        dim = 32
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        assert rsp['code'] == 0
        # insert data
        nb = 1
        data = [
            {"id": i,
                "vector": [np.float64(random.random()) for _ in range(dim + 1)],
            } for i in range(nb)
        ]
        payload = {
            "collectionName": name,
            "data": data,
        }
        body_size = sys.getsizeof(json.dumps(payload))
        logger.info(f"body size: {body_size / 1024 / 1024} MB")
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 1804
        assert "fail to deal the insert data" in rsp['message']

    def test_insert_entities_with_none_no_nullable_field(self):
        """
        Insert a vector with none no nullable field
        """
        # create a collection
        name = gen_collection_name()
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": True,
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{128}"}},
                ]
            }
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        data = []
        for i in range(10):
            tmp = {
                "word_count": i if i % 2 else None,
                "text_emb": preprocessing.normalize([np.array([random.random() for _ in range(128)])])[0].tolist(),
            }
            data.append(tmp)
        payload = {
            "collectionName": name,
            "data": data,
        }
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 1804
        assert "fail to deal the insert data" in rsp['message']


@pytest.mark.L0
class TestUpsertVector(TestBase):

    @pytest.mark.parametrize("insert_round", [2])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("id_type", ["Int64", "VarChar"])
    def test_upsert_vector_default(self, nb, dim, insert_round, id_type):
        # create a collection
        name = gen_collection_name()
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": f"{id_type}", "isPrimary": True, "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "user_id", "dataType": "Int64", "isPartitionKey": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "text_emb", "indexName": "text_emb_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for j in range(nb):
                tmp = {
                    "book_id": i * nb + j if id_type == "Int64" else f"{i * nb + j}",
                    "user_id": i * nb + j,
                    "word_count": i * nb + j,
                    "book_describe": f"book_{i * nb + j}",
                    "text_emb": preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
                }
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            body_size = sys.getsizeof(json.dumps(payload))
            logger.info(f"body size: {body_size / 1024 / 1024} MB")
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
            c = Collection(name)
            c.flush()

        # upsert data
        for i in range(insert_round):
            data = []
            for j in range(nb):
                tmp = {
                    "book_id": i * nb + j if id_type == "Int64" else f"{i * nb + j}",
                    "user_id": i * nb + j + 1,
                    "word_count": i * nb + j + 2,
                    "book_describe": f"book_{i * nb + j + 3}",
                    "text_emb": preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
                }
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            body_size = sys.getsizeof(json.dumps(payload))
            logger.info(f"body size: {body_size / 1024 / 1024} MB")
            rsp = self.vector_client.vector_upsert(payload)
        # query data to make sure the data is updated
        if id_type == "Int64":
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": "book_id > 0"})
        if id_type == "VarChar":
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": "book_id > '0'"})
        for data in rsp['data']:
            assert data['user_id'] == int(data['book_id']) + 1
            assert data['word_count'] == int(data['book_id']) + 2
            assert data['book_describe'] == f"book_{int(data['book_id']) + 3}"
        res = utility.get_query_segment_info(name)
        logger.info(f"res: {res}")

    @pytest.mark.parametrize("insert_round", [2])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("id_type", ["Int64", "VarChar"])
    @pytest.mark.xfail(reason="currently not support auto_id for upsert")
    def test_upsert_vector_pk_auto_id(self, nb, dim, insert_round, id_type):
        # create a collection
        name = gen_collection_name()
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": True,
                "fields": [
                    {"fieldName": "book_id", "dataType": f"{id_type}", "isPrimary": True, "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "user_id", "dataType": "Int64", "isPartitionKey": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "text_emb", "indexName": "text_emb_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        ids = []
        # insert data
        for i in range(insert_round):
            data = []
            for j in range(nb):
                tmp = {
                    "book_id": i * nb + j if id_type == "Int64" else f"{i * nb + j}",
                    "user_id": i * nb + j,
                    "word_count": i * nb + j,
                    "book_describe": f"book_{i * nb + j}",
                    "text_emb": preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
                }
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            body_size = sys.getsizeof(json.dumps(payload))
            logger.info(f"body size: {body_size / 1024 / 1024} MB")
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
            ids.extend(rsp['data']['insertIds'])
            c = Collection(name)
            c.flush()

        # upsert data
        for i in range(insert_round):
            data = []
            for j in range(nb):
                tmp = {
                    "book_id": ids[i * nb + j],
                    "user_id": i * nb + j + 1,
                    "word_count": i * nb + j + 2,
                    "book_describe": f"book_{i * nb + j + 3}",
                    "text_emb": preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
                }
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            body_size = sys.getsizeof(json.dumps(payload))
            logger.info(f"body size: {body_size / 1024 / 1024} MB")
            rsp = self.vector_client.vector_upsert(payload)
        # query data to make sure the data is updated
        if id_type == "Int64":
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": "book_id > 0"})
        if id_type == "VarChar":
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": "book_id > '0'"})
        for data in rsp['data']:
            assert data['user_id'] == int(data['book_id']) + 1
            assert data['word_count'] == int(data['book_id']) + 2
            assert data['book_describe'] == f"book_{int(data['book_id']) + 3}"
        res = utility.get_query_segment_info(name)
        logger.info(f"res: {res}")

    @pytest.mark.parametrize("insert_round", [2])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("id_type", ["Int64", "VarChar"])
    def test_upsert_vector_with_default_none(self, nb, dim, insert_round, id_type):
        # create a collection
        name = gen_collection_name()
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": f"{id_type}", "isPrimary": True, "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "user_id", "dataType": "Int64", "isPartitionKey": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}, "defaultValue": 123},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"},
                     "nullable": True},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "text_emb", "indexName": "text_emb_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for j in range(nb):
                tmp = {
                    "book_id": i * nb + j if id_type == "Int64" else f"{i * nb + j}",
                    "user_id": i * nb + j,
                    "word_count": i * nb + j,
                    "book_describe": f"book_{i * nb + j}",
                    "text_emb": preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
                }
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            body_size = sys.getsizeof(json.dumps(payload))
            logger.info(f"body size: {body_size / 1024 / 1024} MB")
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
            c = Collection(name)
            c.flush()

        # upsert data
        for i in range(insert_round):
            data = []
            for j in range(nb):
                tmp = {
                    "book_id": i * nb + j if id_type == "Int64" else f"{i * nb + j}",
                    "user_id": i * nb + j + 1,
                    "word_count": None,
                    "book_describe": None,
                    "text_emb": preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
                }
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            body_size = sys.getsizeof(json.dumps(payload))
            logger.info(f"body size: {body_size / 1024 / 1024} MB")
            rsp = self.vector_client.vector_upsert(payload)
        # query data to make sure the data is updated
        if id_type == "Int64":
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": "book_id > 0"})
        if id_type == "VarChar":
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": "book_id > '0'"})
        for data in rsp['data']:
            assert data['user_id'] == int(data['book_id']) + 1
            assert data['word_count'] == 123
            assert data['book_describe'] is None


@pytest.mark.L0
class TestUpsertVectorNegative(TestBase):

    def test_upsert_vector_with_invalid_collection_name(self):
        """
        upsert a vector with an invalid collection name
        """

        # create a collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        assert rsp['code'] == 0
        # insert data
        nb = 100
        data = get_data_by_payload(payload, nb)
        payload = {
            "collectionName": "invalid_collection_name",
            "data": data,
        }
        body_size = sys.getsizeof(json.dumps(payload))
        logger.info(f"body size: {body_size / 1024 / 1024} MB")
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 100
        assert "can't find collection" in rsp['message']

    def test_upsert_entities_with_none_no_nullable_field(self):
        """
        Insert a vector with none no nullable field
        """
        # create a collection
        name = gen_collection_name()
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": True,
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{128}"}},
                ]
            }
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        data = []
        for i in range(10):
            tmp = {
                "word_count": i if i % 2 else None,
                "text_emb": preprocessing.normalize([np.array([random.random() for _ in range(128)])])[0].tolist(),
            }
            data.append(tmp)
        payload = {
            "collectionName": name,
            "data": data,
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 1804
        assert "fail to deal the insert data" in rsp['message']


@pytest.mark.L0
class TestSearchVector(TestBase):

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [16])
    @pytest.mark.parametrize("pass_fp32_to_fp16_or_bf16", [True, False])
    def test_search_vector_with_all_vector_datatype(self, nb, dim, insert_round, auto_id,
                                                      is_partition_key, enable_dynamic_schema,
                                                      pass_fp32_to_fp16_or_bf16):
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
                 "params": {"index_type": "BIN_IVF_FLAT", "nlist": "512"}}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": i%10,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "float_vector": gen_vector(datatype="FloatVector", dim=dim),
                        "float16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="Float16Vector", dim=dim)
                        ),
                        "bfloat16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="BFloat16Vector", dim=dim)
                        ),
                        "binary_vector": gen_vector(datatype="BinaryVector", dim=dim)
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i%10,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "float_vector": gen_vector(datatype="FloatVector", dim=dim),
                        "float16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="Float16Vector", dim=dim)
                        ),
                        "bfloat16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="BFloat16Vector", dim=dim)
                        ),
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
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        # search data
        payload = {
            "collectionName": name,
            "data": [gen_vector(datatype="FloatVector", dim=dim)],
            "annsField": "float_vector",
            "filter": "word_count > 100",
            "groupingField": "user_id",
            "outputFields": ["*"],
            "limit": 100
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 0
        # assert no dup user_id
        user_ids = [r["user_id"]for r in rsp['data']]
        assert len(user_ids) == len(set(user_ids))

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("nq", [1, 2])
    @pytest.mark.parametrize("metric_type", ['COSINE', "L2", "IP"])
    def test_search_vector_with_float_vector_datatype(self, nb, dim, insert_round, auto_id,
                                                      is_partition_key, enable_dynamic_schema, nq, metric_type):
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
                ]
            },
            "indexParams": [
                {"fieldName": "float_vector", "indexName": "float_vector", "metricType": metric_type},
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
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
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i%100,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "float_vector": gen_vector(datatype="FloatVector", dim=dim),
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        # search data
        payload = {
            "collectionName": name,
            "data": [gen_vector(datatype="FloatVector", dim=dim) for _ in range(nq)],
            "filter": "word_count > 100",
            "groupingField": "user_id",
            "outputFields": ["*"],
            "limit": 100,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 0
        assert len(rsp['data']) == 100 * nq


    @pytest.mark.parametrize("insert_round", [1, 10])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("is_partition_key", [True, False])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("groupingField", ['user_id', None])
    @pytest.mark.parametrize("sparse_format", ['dok', 'coo'])
    def test_search_vector_with_sparse_float_vector_datatype(self, nb, dim, insert_round, auto_id,
                                                      is_partition_key, enable_dynamic_schema, groupingField, sparse_format):
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
                    {"fieldName": "sparse_float_vector", "dataType": "SparseFloatVector"},
                ]
            },
            "indexParams": [
                {"fieldName": "sparse_float_vector", "indexName": "sparse_float_vector", "metricType": "IP",
                 "params": {"index_type": "SPARSE_INVERTED_INDEX", "drop_ratio_build": "0.2"}}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for j in range(nb):
                idx = i * nb + j
                if auto_id:
                    tmp = {
                        "user_id": idx%100,
                        "word_count": j,
                        "book_describe": f"book_{idx}",
                        "sparse_float_vector": gen_vector(datatype="SparseFloatVector", dim=dim, sparse_format=sparse_format),
                    }
                else:
                    tmp = {
                        "book_id": idx,
                        "user_id": idx%100,
                        "word_count": j,
                        "book_describe": f"book_{idx}",
                        "sparse_float_vector": gen_vector(datatype="SparseFloatVector", dim=dim, sparse_format=sparse_format),
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        # search data
        payload = {
            "collectionName": name,
            "data": [gen_vector(datatype="SparseFloatVector", dim=dim, sparse_format="dok")],
            "filter": "word_count > 100",
            "outputFields": ["*"],
            "searchParams": {
                "metricType": "IP",
                "params": {
                    "drop_ratio_search": "0.2",
                }
            },
            "limit": 500,
        }
        if groupingField:
            payload["groupingField"] = groupingField
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 0


    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("is_partition_key", [True, False])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("groupingField", ['user_id', None])
    @pytest.mark.parametrize("tokenizer", ['standard'])
    def test_search_vector_for_en_full_text_search(self, nb, dim, insert_round, auto_id,
                                                      is_partition_key, enable_dynamic_schema, groupingField, tokenizer):
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
                    {"fieldName": "document_content", "dataType": "VarChar",
                     "elementTypeParams": {"max_length": "1000", "enable_analyzer": True,
                                           "analyzer_params": {
                                               "tokenizer": tokenizer,
                                           },
                                           "enable_match": True}},
                    {"fieldName": "sparse_vector", "dataType": "SparseFloatVector"},
                ],
                "functions": [
                    {
                        "name": "bm25_fn",
                        "type": "BM25",
                        "inputFieldNames": ["document_content"],
                        "outputFieldNames": ["sparse_vector"],
                        "params": {}
                    }
                ]
            },

            "indexParams": [
                {"fieldName": "sparse_vector", "indexName": "sparse_vector", "metricType": "BM25",
                 "params": {"index_type": "SPARSE_INVERTED_INDEX"}}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        if tokenizer == 'standard':
            fake = fake_en
        elif tokenizer == 'jieba':
            fake = fake_zh
        else:
            raise Exception("Invalid tokenizer")

        # insert data
        for i in range(insert_round):
            data = []
            for j in range(nb):
                idx = i * nb + j
                if auto_id:
                    tmp = {
                        "user_id": idx%100,
                        "word_count": j,
                        "book_describe": f"book_{idx}",
                        "document_content": fake.text().lower(),
                    }
                else:
                    tmp = {
                        "book_id": idx,
                        "user_id": idx%100,
                        "word_count": j,
                        "book_describe": f"book_{idx}",
                        "document_content": fake.text().lower(),
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        assert rsp['code'] == 0

        # search data
        payload = {
            "collectionName": name,
            "data": [fake.text().lower() for _ in range(1)],
            "filter": "word_count > 100",
            "outputFields": ["*"],
            "searchParams": {
                "params": {
                    "drop_ratio_search": "0.2",
                }
            },
            "limit": 500,
        }
        if groupingField:
            payload["groupingField"] = groupingField
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 0
        assert len(rsp['data']) > 0


    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("is_partition_key", [True, False])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("groupingField", ['user_id', None])
    @pytest.mark.parametrize("tokenizer", ['jieba'])
    @pytest.mark.xfail(reason="issue: https://github.com/milvus-io/milvus/issues/36751")
    def test_search_vector_for_zh_full_text_search(self, nb, dim, insert_round, auto_id,
                                                      is_partition_key, enable_dynamic_schema, groupingField, tokenizer):
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
                    {"fieldName": "document_content", "dataType": "VarChar",
                     "elementTypeParams": {"max_length": "1000", "enable_analyzer": True,
                                           "analyzer_params": {
                                               "tokenizer": tokenizer,
                                           },
                                           "enable_match": True}},
                    {"fieldName": "sparse_vector", "dataType": "SparseFloatVector"},
                ],
                "functions": [
                    {
                        "name": "bm25_fn",
                        "type": "BM25",
                        "inputFieldNames": ["document_content"],
                        "outputFieldNames": ["sparse_vector"],
                        "params": {}
                    }
                ]
            },

            "indexParams": [
                {"fieldName": "sparse_vector", "indexName": "sparse_vector", "metricType": "BM25",
                 "params": {"index_type": "SPARSE_INVERTED_INDEX"}}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        if tokenizer == 'standard':
            fake = fake_en
        elif tokenizer == 'jieba':
            fake = fake_zh
        else:
            raise Exception("Invalid tokenizer")

        # insert data
        for i in range(insert_round):
            data = []
            for j in range(nb):
                idx = i * nb + j
                if auto_id:
                    tmp = {
                        "user_id": idx%100,
                        "word_count": j,
                        "book_describe": f"book_{idx}",
                        "document_content": fake.text().lower(),
                    }
                else:
                    tmp = {
                        "book_id": idx,
                        "user_id": idx%100,
                        "word_count": j,
                        "book_describe": f"book_{idx}",
                        "document_content": fake.text().lower(),
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        assert rsp['code'] == 0

        # search data
        payload = {
            "collectionName": name,
            "data": [fake.text().lower() for _ in range(2)],
            "filter": "word_count > 100",
            "outputFields": ["*"],
            "searchParams": {
                "params": {
                    "drop_ratio_search": "0.2",
                }
            },
            "limit": 500,
        }
        if groupingField:
            payload["groupingField"] = groupingField
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 0
        assert len(rsp['data']) > 0




    @pytest.mark.parametrize("insert_round", [2])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("metric_type", ['HAMMING'])
    def test_search_vector_with_binary_vector_datatype(self, metric_type, nb, dim, insert_round, auto_id,
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
                    {"fieldName": "binary_vector", "dataType": "BinaryVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [
                {"fieldName": "binary_vector", "indexName": "binary_vector", "metricType": metric_type,
                 "params": {"index_type": "BIN_IVF_FLAT", "nlist": "512"}}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": i%100,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "binary_vector": gen_vector(datatype="BinaryVector", dim=dim),
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i%100,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "binary_vector": gen_vector(datatype="BinaryVector", dim=dim),
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        # flush data
        c = Collection(name)
        c.flush()
        time.sleep(5)
        # wait for index
        rsp = self.index_client.index_describe(collection_name=name, index_name="binary_vector")

        # search data
        payload = {
            "collectionName": name,
            "data": [gen_vector(datatype="BinaryVector", dim=dim)],
            "filter": "word_count > 100",
            "outputFields": ["*"],
            "limit": 100,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 0
        assert len(rsp['data']) == 100

    @pytest.mark.parametrize("metric_type", ["IP", "L2", "COSINE"])
    def test_search_vector_with_simple_payload(self, metric_type):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        self.init_collection(name, metric_type=metric_type)

        # search data
        dim = 128
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        payload = {
            "collectionName": name,
            "data": [vector_to_search],
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        limit = int(payload.get("limit", 100))
        assert len(res) == limit
        ids = [item['id'] for item in res]
        assert len(ids) == len(set(ids))
        distance = [item['distance'] for item in res]
        if metric_type == "L2":
            assert distance == sorted(distance)
        if metric_type == "IP" or metric_type == "COSINE":
            assert distance == sorted(distance, reverse=True)

    @pytest.mark.parametrize("sum_limit_offset", [16384, 16385])
    @pytest.mark.xfail(reason="")
    def test_search_vector_with_exceed_sum_limit_offset(self, sum_limit_offset):
        """
        Search a vector with a simple payload
        """
        max_search_sum_limit_offset = constant.MAX_SUM_OFFSET_AND_LIMIT
        name = gen_collection_name()
        self.name = name
        nb = sum_limit_offset + 2000
        metric_type = "IP"
        limit = 100
        self.init_collection(name, metric_type=metric_type, nb=nb, batch_size=2000)

        # search data
        dim = 128
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        payload = {
            "collectionName": name,
            "vector": vector_to_search,
            "limit": limit,
            "offset": sum_limit_offset - limit,
        }
        rsp = self.vector_client.vector_search(payload)
        if sum_limit_offset > max_search_sum_limit_offset:
            assert rsp['code'] == 65535
            return
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        limit = int(payload.get("limit", 100))
        assert len(res) == limit
        ids = [item['id'] for item in res]
        assert len(ids) == len(set(ids))
        distance = [item['distance'] for item in res]
        if metric_type == "L2":
            assert distance == sorted(distance)
        if metric_type == "IP":
            assert distance == sorted(distance, reverse=True)

    @pytest.mark.parametrize("offset", [0, 100])
    @pytest.mark.parametrize("limit", [100])
    @pytest.mark.parametrize("metric_type", ["L2", "IP", "COSINE"])
    def test_search_vector_with_complex_payload(self, limit, offset, metric_type):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        nb = limit + offset + 3000
        dim = 128
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb, metric_type=metric_type)
        vector_field = schema_payload.get("vectorField")
        # search data
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        output_fields = get_common_fields_by_data(data, exclude_fields=[vector_field])
        payload = {
            "collectionName": name,
            "data": [vector_to_search],
            "outputFields": output_fields,
            "filter": "uid >= 0",
            "limit": limit,
            "offset": offset,
        }
        rsp = self.vector_client.vector_search(payload)
        if offset + limit > constant.MAX_SUM_OFFSET_AND_LIMIT:
            assert rsp['code'] == 90126
            return
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        assert len(res) == limit
        for item in res:
            assert item.get("uid") >= 0
            for field in output_fields:
                assert field in item

    @pytest.mark.parametrize("filter_expr", ["uid >= 0", "uid >= 0 and uid < 100", "uid in [1,2,3]"])
    def test_search_vector_with_complex_int_filter(self, filter_expr):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        limit = 100
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        vector_field = schema_payload.get("vectorField")
        # search data
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        output_fields = get_common_fields_by_data(data, exclude_fields=[vector_field])
        payload = {
            "collectionName": name,
            "data": [vector_to_search],
            "outputFields": output_fields,
            "filter": filter_expr,
            "limit": limit,
            "offset": 0,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        assert len(res) <= limit
        for item in res:
            uid = item.get("uid")
            eval(filter_expr)

    @pytest.mark.parametrize("filter_expr", ["name > \"placeholder\"", "name like \"placeholder%\""])
    def test_search_vector_with_complex_varchar_filter(self, filter_expr):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        limit = 100
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        names = []
        for item in data:
            names.append(item.get("name"))
        names.sort()
        logger.info(f"names: {names}")
        mid = len(names) // 2
        prefix = names[mid][0:2]
        vector_field = schema_payload.get("vectorField")
        # search data
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        output_fields = get_common_fields_by_data(data, exclude_fields=[vector_field])
        filter_expr = filter_expr.replace("placeholder", prefix)
        logger.info(f"filter_expr: {filter_expr}")
        payload = {
            "collectionName": name,
            "data": [vector_to_search],
            "outputFields": output_fields,
            "filter": filter_expr,
            "limit": limit,
            "offset": 0,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        assert len(res) <= limit
        for item in res:
            name = item.get("name")
            logger.info(f"name: {name}")
            if ">" in filter_expr:
                assert name > prefix
            if "like" in filter_expr:
                assert name.startswith(prefix)

    @pytest.mark.parametrize("filter_expr", ["uid < 100 and name > \"placeholder\"",
                                             "uid < 100 and name like \"placeholder%\""
                                             ])
    def test_search_vector_with_complex_int64_varchar_and_filter(self, filter_expr):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        limit = 100
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        names = []
        for item in data:
            names.append(item.get("name"))
        names.sort()
        logger.info(f"names: {names}")
        mid = len(names) // 2
        prefix = names[mid][0:2]
        vector_field = schema_payload.get("vectorField")
        # search data
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        output_fields = get_common_fields_by_data(data, exclude_fields=[vector_field])
        filter_expr = filter_expr.replace("placeholder", prefix)
        logger.info(f"filter_expr: {filter_expr}")
        payload = {
            "collectionName": name,
            "data": [vector_to_search],
            "outputFields": output_fields,
            "filter": filter_expr,
            "limit": limit,
            "offset": 0,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        assert len(res) <= limit
        for item in res:
            uid = item.get("uid")
            name = item.get("name")
            logger.info(f"name: {name}")
            uid_expr = filter_expr.split("and")[0]
            assert eval(uid_expr) is True
            varchar_expr = filter_expr.split("and")[1]
            if ">" in varchar_expr:
                assert name > prefix
            if "like" in varchar_expr:
                assert name.startswith(prefix)

    @pytest.mark.parametrize("consistency_level", ["Strong", "Bounded", "Eventually", "Session"])
    def test_search_vector_with_consistency_level(self, consistency_level):
        """
        Search a vector with different consistency level
        """
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        limit = 100
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        names = []
        for item in data:
            names.append(item.get("name"))
        names.sort()
        logger.info(f"names: {names}")
        mid = len(names) // 2
        prefix = names[mid][0:2]
        vector_field = schema_payload.get("vectorField")
        # search data
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        output_fields = get_common_fields_by_data(data, exclude_fields=[vector_field])
        payload = {
            "collectionName": name,
            "data": [vector_to_search],
            "outputFields": output_fields,
            "limit": limit,
            "offset": 0,
            "consistencyLevel": consistency_level
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        assert len(res) == limit

    @pytest.mark.parametrize("metric_type", ["L2", "COSINE", "IP"])
    def test_search_vector_with_range_search(self, metric_type):
        """
        Search a vector with range search with different metric type
        """
        name = gen_collection_name()
        self.name = name
        nb = 3000
        dim = 128
        limit = 100
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb, metric_type=metric_type)
        vector_field = schema_payload.get("vectorField")
        # search data
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        training_data = [item[vector_field] for item in data]
        distance_sorted = get_sorted_distance(training_data, [vector_to_search], metric_type)
        r1, r2 = distance_sorted[0][nb//2], distance_sorted[0][nb//2+limit+int((0.5*limit))] # recall is not 100% so add 50% to make sure the range is more than limit
        if metric_type == "L2":
            r1, r2 = r2, r1
        output_fields = get_common_fields_by_data(data, exclude_fields=[vector_field])
        logger.info(f"r1: {r1}, r2: {r2}")
        payload = {
            "collectionName": name,
            "data": [vector_to_search],
            "outputFields": output_fields,
            "limit": limit,
            "offset": 0,
            "searchParams": {
                "params": {
                    "radius": r1,
                    "range_filter": r2,
                }
            }
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        assert len(res) >= limit*0.8
        # add buffer to the distance of comparison
        if metric_type == "L2":
            r1 = r1 + 10**-6
            r2 = r2 - 10**-6
        else:
            r1 = r1 - 10**-6
            r2 = r2 + 10**-6
        for item in res:
            distance = item.get("distance")
            if metric_type == "L2":
                assert r1 > distance > r2
            else:
                assert r1 < distance < r2

    @pytest.mark.parametrize("ignore_growing", [True, False])
    def test_search_vector_with_ignore_growing(self, ignore_growing):
        """
        Search a vector with range search with different metric type
        """
        name = gen_collection_name()
        self.name = name
        metric_type = "COSINE"
        nb = 1000
        dim = 128
        limit = 100
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb, metric_type=metric_type)
        vector_field = schema_payload.get("vectorField")
        # search data
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        training_data = [item[vector_field] for item in data]
        distance_sorted = get_sorted_distance(training_data, [vector_to_search], metric_type)
        r1, r2 = distance_sorted[0][nb//2], distance_sorted[0][nb//2+limit+int((0.2*limit))] # recall is not 100% so add 20% to make sure the range is correct
        if metric_type == "L2":
            r1, r2 = r2, r1
        output_fields = get_common_fields_by_data(data, exclude_fields=[vector_field])

        payload = {
            "collectionName": name,
            "data": [vector_to_search],
            "outputFields": output_fields,
            "limit": limit,
            "offset": 0,
            "searchParams": {
                "ignoreGrowing": ignore_growing

            }
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        if ignore_growing is True:
            assert len(res) == 0
        else:
            assert len(res) == limit


    @pytest.mark.parametrize("tokenizer", ["jieba", "standard"])
    def test_search_vector_with_text_match_filter(self, tokenizer):
        """
        Query a vector with a simple payload
        """
        fake = fake_en
        language = "en"
        if tokenizer == "jieba":
            fake = fake_zh
            language = "zh"
        # create a collection
        dim = 128
        analyzer_params = {
            "tokenizer": tokenizer,
        }
        name = gen_collection_name()
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
				enable_match=True,
                is_partition_key=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
				enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
				enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
				enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        collection = Collection(name=name, schema=schema
        )
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        data_size = 3000
        batch_size = 1000
        # insert data
        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.sentence().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)]
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = analyze_documents(df[field].tolist(), language=language)
        for i in range(0, data_size, batch_size):
            tmp = data[i:i + batch_size]
            payload = {
                "collectionName": name,
                "data": tmp,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == len(tmp)
        collection.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection.load()
        time.sleep(5)
        vector_to_search = [[random.random() for _ in range(dim)]]
        for field in text_fields:
            token = wf_map[field].most_common()[0][0]
            expr = f"text_match({field}, '{token}')"
            logger.info(f"expr: {expr}")
            rsp = self.vector_client.vector_search({"collectionName": name, "data":vector_to_search, "filter": f"{expr}", "outputFields": ["*"]})
            assert rsp['code'] == 0, rsp
            for d in rsp['data']:
                assert token in d[field]

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("nq", [1, 2])
    @pytest.mark.parametrize("metric_type", ['COSINE', "L2", "IP"])
    def test_search_vector_with_default_none(self, nb, dim, insert_round, auto_id, is_partition_key,
                                             enable_dynamic_schema, nq, metric_type):
        """
        Insert a vector with default and none
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
                     "elementTypeParams": {}, "defaultValue": 8888},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}, "nullable": True},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"},
                     "nullable": True, "defaultValue": "8888"},
                    {"fieldName": "float_vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [
                {"fieldName": "float_vector", "indexName": "float_vector", "metricType": metric_type},
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": i % 100 if i % 2 else None,
                        "word_count": None,
                        "book_describe": None,
                        "float_vector": gen_vector(datatype="FloatVector", dim=dim),
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i % 100 if i % 2 else None,
                        "word_count": None,
                        "book_describe": None,
                        "float_vector": gen_vector(datatype="FloatVector", dim=dim),
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        # search data
        payload = {
            "collectionName": name,
            "data": [gen_vector(datatype="FloatVector", dim=dim) for _ in range(nq)],
            "filter": "book_id >= 0",
            # "groupingField": "user_id",
            "outputFields": ["*"],
            "limit": 100,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 0
        assert rsp['data'][0]['book_describe'] == "8888"
        assert rsp['data'][0]['word_count'] is None
        assert len(rsp['data']) == 100 * nq

@pytest.mark.L0
class TestSearchVectorNegative(TestBase):

    @pytest.mark.parametrize("metric_type", ["L2"])
    def test_search_vector_without_required_data_param(self, metric_type):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        self.init_collection(name, metric_type=metric_type)

        # search data
        dim = 128
        payload = {
            "collectionName": name,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 1802

    @pytest.mark.parametrize("invalid_metric_type", ["L2", "IP", "UNSUPPORTED"])
    @pytest.mark.xfail(reason="issue: https://github.com/milvus-io/milvus/issues/37138")
    def test_search_vector_with_invalid_metric_type(self, invalid_metric_type):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        self.init_collection(name, metric_type="COSINE")

        # search data
        dim = 128
        payload = {
            "collectionName": name,
            "data": [preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()],
            "searchParams": {
                "metricType": invalid_metric_type
            }
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] != 0


    @pytest.mark.parametrize("limit", [0, 16385])
    def test_search_vector_with_invalid_limit(self, limit):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        dim = 128
        schema_payload, data = self.init_collection(name, dim=dim)
        vector_field = schema_payload.get("vectorField")
        # search data
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        output_fields = get_common_fields_by_data(data, exclude_fields=[vector_field])
        payload = {
            "collectionName": name,
            "data": [vector_to_search],
            "outputFields": output_fields,
            "filter": "uid >= 0",
            "limit": limit,
            "offset": 0,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 65535

    @pytest.mark.parametrize("offset", [-1, 100_001])
    def test_search_vector_with_invalid_offset(self, offset):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        dim = 128
        schema_payload, data = self.init_collection(name, dim=dim)
        vector_field = schema_payload.get("vectorField")
        # search data
        dim = 128
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        output_fields = get_common_fields_by_data(data, exclude_fields=[vector_field])
        payload = {
            "collectionName": name,
            "data": [vector_to_search],
            "outputFields": output_fields,
            "filter": "uid >= 0",
            "limit": 100,
            "offset": offset,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 65535

    def test_search_vector_with_invalid_collection_name(self):
        """
        Search a vector with invalid collection name
        """
        name = gen_collection_name()
        self.name = name
        dim = 128
        schema_payload, data = self.init_collection(name, dim=dim)
        vector_field = schema_payload.get("vectorField")
        # search data
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        output_fields = get_common_fields_by_data(data, exclude_fields=[vector_field])
        payload = {
            "collectionName": "invalid_collection_name",
            "data": [vector_to_search],
            "outputFields": output_fields,
            "filter": "uid >= 0",
            "limit": 100,
            "offset": 0,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 100
        assert "can't find collection" in rsp['message']


@pytest.mark.L0
class TestAdvancedSearchVector(TestBase):

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [2])
    def test_advanced_search_vector_with_multi_float32_vector_datatype(self, nb, dim, insert_round, auto_id,
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
                    {"fieldName": "float_vector_1", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "float_vector_2", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [
                {"fieldName": "float_vector_1", "indexName": "float_vector_1", "metricType": "COSINE"},
                {"fieldName": "float_vector_2", "indexName": "float_vector_2", "metricType": "COSINE"},

            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": i%100,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "float_vector_1": gen_vector(datatype="FloatVector", dim=dim),
                        "float_vector_2": gen_vector(datatype="FloatVector", dim=dim),
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i%100,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "float_vector_1": gen_vector(datatype="FloatVector", dim=dim),
                        "float_vector_2": gen_vector(datatype="FloatVector", dim=dim),

                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        # advanced search data

        payload = {
            "collectionName": name,
            "search": [{
                "data": [gen_vector(datatype="FloatVector", dim=dim)],
                "annsField": "float_vector_1",
                "limit": 10,
                "outputFields": ["*"]
            },
                {
                "data": [gen_vector(datatype="FloatVector", dim=dim)],
                "annsField": "float_vector_2",
                "limit": 10,
                "outputFields": ["*"]
                }

            ],
            "rerank": {
                "strategy": "rrf",
                "params": {
                    "k": 10,
                }
            },
            "limit": 10,
            "outputFields": ["user_id", "word_count", "book_describe"]
        }

        rsp = self.vector_client.vector_advanced_search(payload)
        assert rsp['code'] == 0
        assert len(rsp['data']) == 10
        assert len(rsp['topks']) == 1
        assert rsp['topks'][0] == 10
        


@pytest.mark.L0
class TestHybridSearchVector(TestBase):

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [2])
    def test_hybrid_search_vector_with_multi_float32_vector_datatype(self, nb, dim, insert_round, auto_id,
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
                    {"fieldName": "float_vector_1", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "float_vector_2", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [
                {"fieldName": "float_vector_1", "indexName": "float_vector_1", "metricType": "COSINE"},
                {"fieldName": "float_vector_2", "indexName": "float_vector_2", "metricType": "COSINE"},

            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": i%100,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "float_vector_1": gen_vector(datatype="FloatVector", dim=dim),
                        "float_vector_2": gen_vector(datatype="FloatVector", dim=dim),
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i%100,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "float_vector_1": gen_vector(datatype="FloatVector", dim=dim),
                        "float_vector_2": gen_vector(datatype="FloatVector", dim=dim),

                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        # advanced search data

        payload = {
            "collectionName": name,
            "search": [{
                "data": [gen_vector(datatype="FloatVector", dim=dim)],
                "annsField": "float_vector_1",
                "limit": 10,
                "outputFields": ["*"]
            },
                {
                "data": [gen_vector(datatype="FloatVector", dim=dim)],
                "annsField": "float_vector_2",
                "limit": 10,
                "outputFields": ["*"]
                }

            ],
            "rerank": {
                "strategy": "rrf",
                "params": {
                    "k": 10,
                }
            },
            "limit": 10,
            "outputFields": ["user_id", "word_count", "book_describe"]
        }

        rsp = self.vector_client.vector_hybrid_search(payload)
        assert rsp['code'] == 0
        assert len(rsp['data']) == 10


@pytest.mark.L0
class TestQueryVector(TestBase):

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    def test_query_entities_with_all_scalar_datatype(self, nb, dim, insert_round, auto_id,
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
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "25536"}},
                    {"fieldName": "bool", "dataType": "Bool", "elementTypeParams": {}},
                    {"fieldName": "json", "dataType": "JSON", "elementTypeParams": {}},
                    {"fieldName": "int_array", "dataType": "Array", "elementDataType": "Int64",
                     "elementTypeParams": {"max_capacity": "1024"}},
                    {"fieldName": "varchar_array", "dataType": "Array", "elementDataType": "VarChar",
                     "elementTypeParams": {"max_capacity": "1024", "max_length": "256"}},
                    {"fieldName": "bool_array", "dataType": "Array", "elementDataType": "Bool",
                     "elementTypeParams": {"max_capacity": "1024"}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "image_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [
                {"fieldName": "text_emb", "indexName": "text_emb", "metricType": "L2"},
                {"fieldName": "image_emb", "indexName": "image_emb", "metricType": "L2"}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{gen_unique_str(length=1000)}",
                        "bool": random.choice([True, False]),
                        "json": {"key": [i]},
                        "int_array": [i],
                        "varchar_array": [f"varchar_{i}"],
                        "bool_array": [random.choice([True, False])],
                        "text_emb": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[
                            0].tolist(),
                        "image_emb": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[
                            0].tolist(),
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i,
                        "word_count": i,
                        "book_describe": gen_unique_str(length=1000),
                        "bool": random.choice([True, False]),
                        "json": {"key": i},
                        "int_array": [i],
                        "varchar_array": [f"varchar_{i}"],
                        "bool_array": [random.choice([True, False])],
                        "text_emb": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[
                            0].tolist(),
                        "image_emb": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[
                            0].tolist(),
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        # query data to make sure the data is inserted
        # 1. query for int64
        payload = {
            "collectionName": name,
            "filter": "user_id > 0",
            "limit": 50,
            "outputFields": ["*"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 0
        assert len(rsp['data']) == 50

        # 2. query for varchar
        payload = {
            "collectionName": name,
            "filter": "book_describe like \"book%\"",
            "limit": 50,
            "outputFields": ["*"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 0
        assert len(rsp['data']) == 50

        # 3. query for json
        payload = {
            "collectionName": name,
            "filter": "json_contains(json['key'] , 1)",
            "limit": 50,
            "outputFields": ["*"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp['data']) == 1

        # 4. query for array
        payload = {
            "collectionName": name,
            "filter": "array_contains(int_array, 1)",
            "limit": 50,
            "outputFields": ["*"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp['data']) == 1

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("pass_fp32_to_fp16_or_bf16", [True, False])
    def test_query_entities_with_all_vector_datatype(self, nb, dim, insert_round, auto_id,
                                                      is_partition_key, enable_dynamic_schema,
                                                      pass_fp32_to_fp16_or_bf16):
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
                {"fieldName": "float_vector", "indexName": "float_vector", "metricType": "L2"},
                {"fieldName": "float16_vector", "indexName": "float16_vector", "metricType": "L2"},
                {"fieldName": "bfloat16_vector", "indexName": "bfloat16_vector", "metricType": "L2"},
                {"fieldName": "binary_vector", "indexName": "binary_vector", "metricType": "HAMMING",
                 "params": {"index_type": "BIN_IVF_FLAT", "nlist": "512"}}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "float_vector": gen_vector(datatype="FloatVector", dim=dim),
                        "float16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="Float16Vector", dim=dim)
                        ),
                        "bfloat16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="BFloat16Vector", dim=dim)
                        ),
                        "binary_vector": gen_vector(datatype="BinaryVector", dim=dim)
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i,
                        "word_count": i,
                        "book_describe": f"book_{i}",
                        "float_vector": gen_vector(datatype="FloatVector", dim=dim),
                        "float16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="Float16Vector", dim=dim)
                        ),
                        "bfloat16_vector": (
                            gen_vector(datatype="FloatVector", dim=dim)
                            if pass_fp32_to_fp16_or_bf16
                            else gen_vector(datatype="BFloat16Vector", dim=dim)
                        ),
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
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        c = Collection(name)
        res = c.query(
            expr="user_id > 0",
            limit=50,
            output_fields=["*"],
        )
        logger.info(f"res: {res}")
        # query data to make sure the data is inserted
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "user_id > 0", "limit": 50})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 50

    @pytest.mark.parametrize("expr", ["10+20 <= uid < 20+30", "uid in [1,2,3,4]",
                                      "uid > 0", "uid >= 0", "uid > 0",
                                      "uid > -100 and uid < 100"])
    @pytest.mark.parametrize("include_output_fields", [True, False])
    @pytest.mark.parametrize("partial_fields", [True, False])
    def test_query_vector_with_int64_filter(self, expr, include_output_fields, partial_fields):
        """
        Query a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        schema_payload, data = self.init_collection(name)
        output_fields = get_common_fields_by_data(data)
        if partial_fields:
            output_fields = output_fields[:len(output_fields) // 2]
            if "uid" not in output_fields:
                output_fields.append("uid")
        else:
            output_fields = output_fields

        # query data
        payload = {
            "collectionName": name,
            "filter": expr,
            "limit": 100,
            "offset": 0,
            "outputFields": output_fields
        }
        if not include_output_fields:
            payload.pop("outputFields")
            if 'vector' in output_fields:
                output_fields.remove("vector")
        time.sleep(5)
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        for r in res:
            uid = r['uid']
            assert eval(expr) is True
            for field in output_fields:
                assert field in r

    def test_query_vector_with_count(self):
        """
        Query a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        self.init_collection(name, nb=3000)
        # query for "count(*)"
        payload = {
            "collectionName": name,
            "filter": " ",
            "limit": 0,
            "outputFields": ["count(*)"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 0
        assert rsp['data'][0]['count(*)'] == 3000

    @pytest.mark.xfail(reason="query by id is not supported")
    def test_query_vector_by_id(self):
        """
        Query a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        _, _, insert_ids = self.init_collection(name, nb=3000, return_insert_id=True)
        payload = {
            "collectionName": name,
            "id": insert_ids,
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 0

    @pytest.mark.parametrize("filter_expr", ["name > \"placeholder\"", "name like \"placeholder%\""])
    @pytest.mark.parametrize("include_output_fields", [True, False])
    def test_query_vector_with_varchar_filter(self, filter_expr, include_output_fields):
        """
        Query a vector with a complex payload
        """
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        limit = 100
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        names = []
        for item in data:
            names.append(item.get("name"))
        names.sort()
        logger.info(f"names: {names}")
        mid = len(names) // 2
        prefix = names[mid][0:2]
        # search data
        output_fields = get_common_fields_by_data(data)
        filter_expr = filter_expr.replace("placeholder", prefix)
        logger.info(f"filter_expr: {filter_expr}")
        payload = {
            "collectionName": name,
            "outputFields": output_fields,
            "filter": filter_expr,
            "limit": limit,
            "offset": 0,
        }
        if not include_output_fields:
            payload.pop("outputFields")
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        assert len(res) <= limit
        for item in res:
            name = item.get("name")
            logger.info(f"name: {name}")
            if ">" in filter_expr:
                assert name > prefix
            if "like" in filter_expr:
                assert name.startswith(prefix)

    @pytest.mark.parametrize("sum_of_limit_offset", [16384])
    def test_query_vector_with_large_sum_of_limit_offset(self, sum_of_limit_offset):
        """
        Query a vector with sum of limit and offset larger than max value
        """
        max_sum_of_limit_offset = 16384
        name = gen_collection_name()
        filter_expr = "name > \"placeholder\""
        self.name = name
        nb = 200
        dim = 128
        limit = 100
        offset = sum_of_limit_offset - limit
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        names = []
        for item in data:
            names.append(item.get("name"))
        names.sort()
        logger.info(f"names: {names}")
        mid = len(names) // 2
        prefix = names[mid][0:2]
        # search data
        output_fields = get_common_fields_by_data(data)
        filter_expr = filter_expr.replace("placeholder", prefix)
        logger.info(f"filter_expr: {filter_expr}")
        payload = {
            "collectionName": name,
            "outputFields": output_fields,
            "filter": filter_expr,
            "limit": limit,
            "offset": offset,
        }
        rsp = self.vector_client.vector_query(payload)
        if sum_of_limit_offset > max_sum_of_limit_offset:
            assert rsp['code'] == 1
            return
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        assert len(res) <= limit
        for item in res:
            name = item.get("name")
            logger.info(f"name: {name}")
            if ">" in filter_expr:
                assert name > prefix
            if "like" in filter_expr:
                assert name.startswith(prefix)

    @pytest.mark.parametrize("tokenizer", ["jieba", "standard"])
    def test_query_vector_with_text_match_filter(self, tokenizer):
        """
        Query a vector with a simple payload
        """
        fake = fake_en
        language = "en"
        if tokenizer == "jieba":
            fake = fake_zh
            language = "zh"
        # create a collection
        dim = 128
        analyzer_params = {
            "tokenizer": tokenizer,
        }
        name = gen_collection_name()
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
				enable_match=True,
                is_partition_key=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
				enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
				enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
				enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        collection = Collection(name=name, schema=schema
        )
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        data_size = 3000
        batch_size = 1000
        # insert data
        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.sentence().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)]
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = analyze_documents(df[field].tolist(), language=language)
        for i in range(0, data_size, batch_size):
            tmp = data[i:i + batch_size]
            payload = {
                "collectionName": name,
                "data": tmp,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == len(tmp)
        collection.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        collection.load()
        time.sleep(5)
        for field in text_fields:
            token = wf_map[field].most_common()[0][0]
            expr = f"text_match({field}, '{token}')"
            logger.info(f"expr: {expr}")
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": f"{expr}", "outputFields": ["*"]})
            assert rsp['code'] == 0, rsp
            for d in rsp['data']:
                assert token in d[field]

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    def test_query_entities_with_default_none(self, nb, dim, insert_round, auto_id, is_partition_key,
                                              enable_dynamic_schema):
        """
        Insert a vector with default and none
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
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}, "defaultValue": 8888},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "25536"},
                     "nullable": True},
                    {"fieldName": "bool", "dataType": "Bool", "elementTypeParams": {}, "nullable": True},
                    {"fieldName": "json", "dataType": "JSON", "elementTypeParams": {}, "nullable": True},
                    {"fieldName": "int_array", "dataType": "Array", "elementDataType": "Int64",
                     "elementTypeParams": {"max_capacity": "1024"}, "nullable": True},
                    {"fieldName": "varchar_array", "dataType": "Array", "elementDataType": "VarChar",
                     "elementTypeParams": {"max_capacity": "1024", "max_length": "256"}, "nullable": True},
                    {"fieldName": "bool_array", "dataType": "Array", "elementDataType": "Bool",
                     "elementTypeParams": {"max_capacity": "1024"}, "nullable": True},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [
                {"fieldName": "text_emb", "indexName": "text_emb", "metricType": "L2"},
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for i in range(nb):
                if auto_id:
                    tmp = {
                        "user_id": i,
                        "word_count": None,
                        "book_describe": None,
                        "bool": random.choice([True, False]),
                        "json": None,
                        "int_array": None,
                        "varchar_array": None,
                        "bool_array": None,
                        "text_emb": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[0].tolist(),
                    }
                else:
                    tmp = {
                        "book_id": i,
                        "user_id": i,
                        "word_count": None,
                        "book_describe": None,
                        "bool": random.choice([True, False]),
                        "json": None,
                        "int_array": None,
                        "varchar_array": None,
                        "bool_array": None,
                        "text_emb": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[0].tolist(),
                    }
                if enable_dynamic_schema:
                    tmp.update({f"dynamic_field_{i}": i})
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        # query data to make sure the data is inserted
        payload = {
            "collectionName": name,
            "filter": "user_id > 0",
            "limit": 50,
            "outputFields": ["*"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 0
        assert rsp['data'][0]['book_describe'] is None
        assert rsp['data'][0]['word_count'] == 8888
        assert rsp['data'][0]['json'] is None
        assert rsp['data'][0]['varchar_array'] is None
        assert len(rsp['data']) == 50


@pytest.mark.L0
class TestQueryVectorNegative(TestBase):

    def test_query_with_wrong_filter_expr(self):
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        schema_payload, data, insert_ids = self.init_collection(name, dim=dim, nb=nb, return_insert_id=True)
        output_fields = get_common_fields_by_data(data)
        uids = []
        for item in data:
            uids.append(item.get("uid"))
        payload = {
            "collectionName": name,
            "outputFields": output_fields,
            "filter": f"{insert_ids}",
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 1100
        assert "failed to create query plan" in rsp['message']


@pytest.mark.L0
class TestGetVector(TestBase):

    def test_get_vector_with_simple_payload(self):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        self.init_collection(name)

        # search data
        dim = 128
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        payload = {
            "collectionName": name,
            "data": [vector_to_search],
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        limit = int(payload.get("limit", 100))
        assert len(res) == limit
        ids = [item['id'] for item in res]
        assert len(ids) == len(set(ids))
        payload = {
            "collectionName": name,
            "outputFields": ["*"],
            "id": ids[0],
        }
        rsp = self.vector_client.vector_get(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {res}")
        logger.info(f"res: {len(res)}")
        for item in res:
            assert item['id'] == ids[0]

    @pytest.mark.L0
    @pytest.mark.parametrize("id_field_type", ["list", "one"])
    @pytest.mark.parametrize("include_invalid_id", [True, False])
    @pytest.mark.parametrize("include_output_fields", [True, False])
    def test_get_vector_complex(self, id_field_type, include_output_fields, include_invalid_id):
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        output_fields = get_common_fields_by_data(data)
        uids = []
        for item in data:
            uids.append(item.get("uid"))
        payload = {
            "collectionName": name,
            "outputFields": output_fields,
            "filter": f"uid in {uids}",
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        ids = []
        for r in res:
            ids.append(r['id'])
        logger.info(f"ids: {len(ids)}")
        id_to_get = None
        if id_field_type == "list":
            id_to_get = ids
        if id_field_type == "one":
            id_to_get = ids[0]
        if include_invalid_id:
            if isinstance(id_to_get, list):
                id_to_get[-1] = 0
            else:
                id_to_get = 0
        # get by id list
        payload = {
            "collectionName": name,
            "outputFields": output_fields,
            "id": id_to_get
        }
        rsp = self.vector_client.vector_get(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        if isinstance(id_to_get, list):
            if include_invalid_id:
                assert len(res) == len(id_to_get) - 1
            else:
                assert len(res) == len(id_to_get)
        else:
            if include_invalid_id:
                assert len(res) == 0
            else:
                assert len(res) == 1
        for r in rsp['data']:
            if isinstance(id_to_get, list):
                assert r['id'] in id_to_get
            else:
                assert r['id'] == id_to_get
            if include_output_fields:
                for field in output_fields:
                    assert field in r


@pytest.mark.L0
class TestDeleteVector(TestBase):

    @pytest.mark.xfail(reason="delete by id is not supported")
    def test_delete_vector_by_id(self):
        """
        Query a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        _, _, insert_ids = self.init_collection(name, nb=3000, return_insert_id=True)
        payload = {
            "collectionName": name,
            "id": insert_ids,
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 0

    @pytest.mark.parametrize("id_field_type", ["list", "one"])
    def test_delete_vector_by_pk_field_ids(self, id_field_type):
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        schema_payload, data, insert_ids = self.init_collection(name, dim=dim, nb=nb, return_insert_id=True)
        time.sleep(1)
        id_to_delete = None
        if id_field_type == "list":
            id_to_delete = insert_ids
        if id_field_type == "one":
            id_to_delete = insert_ids[0]
        if isinstance(id_to_delete, list):
            payload = {
                "collectionName": name,
                "filter": f"id in {id_to_delete}"
            }
        else:
            payload = {
                "collectionName": name,
                "filter": f"id == {id_to_delete}"
            }
        rsp = self.vector_client.vector_delete(payload)
        assert rsp['code'] == 0
        # verify data deleted by get
        payload = {
            "collectionName": name,
            "id": id_to_delete
        }
        rsp = self.vector_client.vector_get(payload)
        assert len(rsp['data']) == 0

    @pytest.mark.parametrize("id_field_type", ["list", "one"])
    def test_delete_vector_by_filter_pk_field(self, id_field_type):
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        time.sleep(1)
        output_fields = get_common_fields_by_data(data)
        uids = []
        for item in data:
            uids.append(item.get("uid"))
        payload = {
            "collectionName": name,
            "outputFields": output_fields,
            "filter": f"uid in {uids}",
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        ids = []
        for r in res:
            ids.append(r['id'])
        logger.info(f"ids: {len(ids)}")
        id_to_get = None
        if id_field_type == "list":
            id_to_get = ids
        if id_field_type == "one":
            id_to_get = ids[0]
        if isinstance(id_to_get, list):
            if len(id_to_get) >= 100:
                id_to_get = id_to_get[-100:]
        # delete by id list
        if isinstance(id_to_get, list):
            payload = {
                "collectionName": name,
                "filter": f"id in {id_to_get}",
            }
        else:
            payload = {
                "collectionName": name,
                "filter": f"id == {id_to_get}",
            }

        rsp = self.vector_client.vector_delete(payload)
        assert rsp['code'] == 0
        logger.info(f"delete res: {rsp}")

        # verify data deleted
        if not isinstance(id_to_get, list):
            id_to_get = [id_to_get]
        payload = {
            "collectionName": name,
            "filter": f"id in {id_to_get}",
        }
        time.sleep(5)
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 0
        assert len(rsp['data']) == 0

    def test_delete_vector_by_custom_pk_field(self):
        dim = 128
        nb = 3000
        insert_round = 1

        name = gen_collection_name()
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "text_emb", "indexName": "text_emb_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        pk_values = []
        # insert data
        for i in range(insert_round):
            data = []
            for j in range(nb):
                tmp = {
                    "book_id": i * nb + j,
                    "word_count": i * nb + j,
                    "book_describe": f"book_{i * nb + j}",
                    "text_emb": preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
                }
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            tmp = [d["book_id"] for d in data]
            pk_values.extend(tmp)
            body_size = sys.getsizeof(json.dumps(payload))
            logger.info(f"body size: {body_size / 1024 / 1024} MB")
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        # query data before delete
        c = Collection(name)
        res = c.query(expr="", output_fields=["count(*)"])
        logger.info(f"res: {res}")

        # delete data
        payload = {
            "collectionName": name,
            "filter": f"book_id in {pk_values}",
        }
        rsp = self.vector_client.vector_delete(payload)

        # query data after delete
        res = c.query(expr="", output_fields=["count(*)"], consistency_level="Strong")
        logger.info(f"res: {res}")
        assert res[0]["count(*)"] == 0

    def test_delete_vector_by_filter_custom_field(self):
        dim = 128
        nb = 3000
        insert_round = 1

        name = gen_collection_name()
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "text_emb", "indexName": "text_emb_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        # insert data
        for i in range(insert_round):
            data = []
            for j in range(nb):
                tmp = {
                    "book_id": i * nb + j,
                    "word_count": i * nb + j,
                    "book_describe": f"book_{i * nb + j}",
                    "text_emb": preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
                }
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data,
            }
            body_size = sys.getsizeof(json.dumps(payload))
            logger.info(f"body size: {body_size / 1024 / 1024} MB")
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            assert rsp['data']['insertCount'] == nb
        # query data before delete
        c = Collection(name)
        res = c.query(expr="", output_fields=["count(*)"])
        logger.info(f"res: {res}")

        # delete data
        payload = {
            "collectionName": name,
            "filter": "word_count >= 0",
        }
        rsp = self.vector_client.vector_delete(payload)

        # query data after delete
        res = c.query(expr="", output_fields=["count(*)"], consistency_level="Strong")
        logger.info(f"res: {res}")
        assert res[0]["count(*)"] == 0


    def test_delete_vector_with_non_primary_key(self):
        """
        Delete a vector with a non-primary key, expect no data were deleted
        """
        name = gen_collection_name()
        self.name = name
        self.init_collection(name, dim=128, nb=300)
        expr = "uid > 0"
        payload = {
            "collectionName": name,
            "filter": expr,
            "limit": 3000,
            "offset": 0,
            "outputFields": ["id", "uid"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        id_list = [r['uid'] for r in res]
        delete_expr = f"uid in {[i for i in id_list[:10]]}"
        # query data before delete
        payload = {
            "collectionName": name,
            "filter": delete_expr,
            "limit": 3000,
            "offset": 0,
            "outputFields": ["id", "uid"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        num_before_delete = len(res)
        logger.info(f"res: {len(res)}")
        # delete data
        payload = {
            "collectionName": name,
            "filter": delete_expr,
        }
        rsp = self.vector_client.vector_delete(payload)
        # query data after delete
        payload = {
            "collectionName": name,
            "filter": delete_expr,
            "limit": 3000,
            "offset": 0,
            "outputFields": ["id", "uid"]
        }
        time.sleep(1)
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp["data"]) == 0


@pytest.mark.L0
class TestDeleteVectorNegative(TestBase):

    def test_delete_vector_with_invalid_collection_name(self):
        """
        Delete a vector with an invalid collection name
        """
        name = gen_collection_name()
        self.name = name
        self.init_collection(name, dim=128, nb=3000)

        # query data
        # expr = f"id in {[i for i in range(10)]}".replace("[", "(").replace("]", ")")
        expr = "id > 0"
        payload = {
            "collectionName": name,
            "filter": expr,
            "limit": 3000,
            "offset": 0,
            "outputFields": ["id", "uid"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        id_list = [r['id'] for r in res]
        delete_expr = f"id in {[i for i in id_list[:10]]}"
        # query data before delete
        payload = {
            "collectionName": name,
            "filter": delete_expr,
            "limit": 3000,
            "offset": 0,
            "outputFields": ["id", "uid"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        # delete data
        payload = {
            "collectionName": name + "_invalid",
            "filter": delete_expr,
        }
        rsp = self.vector_client.vector_delete(payload)
        assert rsp['code'] == 100
        assert "can't find collection" in rsp['message']

@pytest.mark.L1
class TestVectorWithAuth(TestBase):
    def test_upsert_vector_with_invalid_api_key(self):
        """
        Insert a vector with invalid api key
        """
        # create a collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        assert rsp['code'] == 0
        # insert data
        nb = 10
        data = [
            {
                "vector": [np.float64(random.random()) for _ in range(dim)],
            } for _ in range(nb)
        ]
        payload = {
            "collectionName": name,
            "data": data,
        }
        body_size = sys.getsizeof(json.dumps(payload))
        logger.info(f"body size: {body_size / 1024 / 1024} MB")
        client = self.vector_client
        client.api_key = "invalid_api_key"
        rsp = client.vector_insert(payload)
        assert rsp['code'] == 1800
    def test_insert_vector_with_invalid_api_key(self):
        """
        Insert a vector with invalid api key
        """
        # create a collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        assert rsp['code'] == 0
        # insert data
        nb = 10
        data = [
            {
                "vector": [np.float64(random.random()) for _ in range(dim)],
            } for _ in range(nb)
        ]
        payload = {
            "collectionName": name,
            "data": data,
        }
        body_size = sys.getsizeof(json.dumps(payload))
        logger.info(f"body size: {body_size / 1024 / 1024} MB")
        client = self.vector_client
        client.api_key = "invalid_api_key"
        rsp = client.vector_insert(payload)
        assert rsp['code'] == 1800
    def test_delete_vector_with_invalid_api_key(self):
        """
        Delete a vector with an invalid api key
        """
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        output_fields = get_common_fields_by_data(data)
        uids = []
        for item in data:
            uids.append(item.get("uid"))
        payload = {
            "collectionName": name,
            "outputFields": output_fields,
            "filter": f"uid in {uids}",
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 0
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        ids = []
        for r in res:
            ids.append(r['id'])
        logger.info(f"ids: {len(ids)}")
        id_to_get = ids
        # delete by id list
        payload = {
            "collectionName": name,
            "filter": f"uid in {uids}"
        }
        client = self.vector_client
        client.api_key = "invalid_api_key"
        rsp = client.vector_delete(payload)
        assert rsp['code'] == 1800