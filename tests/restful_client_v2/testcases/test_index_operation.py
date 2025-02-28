import random
from sklearn import preprocessing
import numpy as np
import time
import concurrent.futures
from typing import Dict, List
from utils.utils import gen_collection_name, patch_faker_text, en_vocabularies_distribution, \
    zh_vocabularies_distribution
from utils.util_log import test_log as logger
import pytest
from base.testbase import TestBase
from utils.utils import gen_vector
from pymilvus import (
    Collection
)
from faker import Faker

Faker.seed(19530)
fake_en = Faker("en_US")
fake_zh = Faker("zh_CN")

patch_faker_text(fake_en, en_vocabularies_distribution)
patch_faker_text(fake_zh, zh_vocabularies_distribution)

index_param_map = {
    "FLAT": {},
    "IVF_SQ8": {"nlist": 128},
    "HNSW": {"M": 16, "efConstruction": 200},
    "BM25_SPARSE_INVERTED_INDEX": {"bm25_k1": 0.5, "bm25_b": 0.5},
    "AUTOINDEX": {}
}


@pytest.mark.L0
class TestCreateIndex(TestBase):

    @pytest.mark.parametrize("metric_type", ["L2", "COSINE", "IP"])
    @pytest.mark.parametrize("index_type", ["AUTOINDEX", "IVF_SQ8", "HNSW"])
    @pytest.mark.parametrize("dim", [128])
    def test_index_default(self, dim, metric_type, index_type):
        """
        target: test create collection
        method: create a collection with a simple schema
        expected: create collection success
        """
        name = gen_collection_name()
        client = self.collection_client
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            }
        }
        logger.info(f"create collection {name} with payload: {payload}")
        rsp = client.collection_create(payload)
        c = Collection(name)
        c.flush()
        # list index, expect empty
        rsp = self.index_client.index_list(name)
        # create index
        payload = {
            "collectionName": name,
            "indexParams": [
                {"fieldName": "book_intro", "indexName": "book_intro_vector",
                 "metricType": f"{metric_type}",
                 "indexType": f"{index_type}",
                 "params": index_param_map[index_type]
                 }
            ]
        }

        # Create multiple index creation tasks
        num_threads = 10  # Number of concurrent tasks
        payloads = [payload.copy() for _ in range(num_threads)]

        def create_index(idx_payload: Dict) -> Dict:
            return self.index_client.index_create(idx_payload)

        # Execute index creation concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_payload = {executor.submit(create_index, p): p for p in payloads}
            for future in concurrent.futures.as_completed(future_to_payload):
                try:
                    rsp = future.result()
                    assert rsp['code'] == 0
                except Exception as e:
                    logger.info(f'Index creation failed with error: {str(e)}')
                    raise

        time.sleep(10)  # Wait for all indexes to be ready
        # list index, expect not empty
        rsp = self.index_client.index_list(collection_name=name)
        # describe index
        rsp = self.index_client.index_describe(collection_name=name, index_name="book_intro_vector")
        assert rsp['code'] == 0
        assert len(rsp['data']) == len(payload['indexParams'])
        expected_index = sorted(payload['indexParams'], key=lambda x: x['fieldName'])
        actual_index = sorted(rsp['data'], key=lambda x: x['fieldName'])
        for i in range(len(expected_index)):
            assert expected_index[i]['fieldName'] == actual_index[i]['fieldName']
            assert expected_index[i]['indexName'] == actual_index[i]['indexName']
            assert expected_index[i]['metricType'] == actual_index[i]['metricType']
            assert expected_index[i]["indexType"] == actual_index[i]['indexType']
        # check index by pymilvus
        index_info = [index.to_dict() for index in c.indexes]
        logger.info(f"index_info: {index_info}")
        for index in index_info:
            index_param = index["index_param"]
            if index_param["index_type"] == "SPARSE_INVERTED_INDEX":
                assert index_param["metric_type"] == "BM25"
                assert index_param.get("params", {}) == index_param_map["BM25_SPARSE_INVERTED_INDEX"]
            else:
                assert index_param["metric_type"] == metric_type
                assert index_param["index_type"] == index_type
                assert index_param.get("params", {}) == index_param_map[index_type]
        # drop index
        for i in range(len(actual_index)):
            payload = {
                "collectionName": name,
                "indexName": actual_index[i]['indexName']
            }
            rsp = self.index_client.index_drop(payload)
            assert rsp['code'] == 0
        # list index, expect empty
        rsp = self.index_client.index_list(collection_name=name)
        assert rsp['data'] == []

    @pytest.mark.parametrize("index_type", ["INVERTED"])
    @pytest.mark.parametrize("dim", [128])
    def test_index_for_scalar_field(self, dim, index_type):
        """
        target: test create collection
        method: create a collection with a simple schema
        expected: create collection success
        """
        name = gen_collection_name()
        client = self.collection_client
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            }
        }
        logger.info(f"create collection {name} with payload: {payload}")
        rsp = client.collection_create(payload)
        # insert data
        for i in range(1):
            data = []
            for j in range(3000):
                tmp = {
                    "book_id": j,
                    "word_count": j,
                    "book_describe": f"book_{j}",
                    "book_intro": preprocessing.normalize([np.array([random.random() for _ in range(dim)])])[
                        0].tolist(),
                }
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data
            }
            rsp = self.vector_client.vector_insert(payload)
        c = Collection(name)
        c.flush()
        # list index, expect empty
        rsp = self.index_client.index_list(name)

        # create index
        payload = {
            "collectionName": name,
            "indexParams": [{"fieldName": "word_count", "indexName": "word_count_vector", "indexType": "INVERTED",
                             "params": {"index_type": "INVERTED"}}]
        }
        rsp = self.index_client.index_create(payload)
        assert rsp['code'] == 0
        time.sleep(10)
        # list index, expect not empty
        rsp = self.index_client.index_list(collection_name=name)
        # describe index
        rsp = self.index_client.index_describe(collection_name=name, index_name="word_count_vector")
        assert rsp['code'] == 0
        assert len(rsp['data']) == len(payload['indexParams'])
        expected_index = sorted(payload['indexParams'], key=lambda x: x['fieldName'])
        actual_index = sorted(rsp['data'], key=lambda x: x['fieldName'])
        for i in range(len(expected_index)):
            assert expected_index[i]['fieldName'] == actual_index[i]['fieldName']
            assert expected_index[i]['indexName'] == actual_index[i]['indexName']
            assert expected_index[i]['indexType'] == actual_index[i]['indexType']

    @pytest.mark.parametrize("index_type", ["BIN_FLAT", "BIN_IVF_FLAT"])
    @pytest.mark.parametrize("metric_type", ["JACCARD", "HAMMING"])
    @pytest.mark.parametrize("dim", [128])
    def test_index_for_binary_vector_field(self, dim, metric_type, index_type):
        """
        target: test create collection
        method: create a collection with a simple schema
        expected: create collection success
        """
        name = gen_collection_name()
        client = self.collection_client
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "binary_vector", "dataType": "BinaryVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            }
        }
        logger.info(f"create collection {name} with payload: {payload}")
        rsp = client.collection_create(payload)
        # insert data
        for i in range(1):
            data = []
            for j in range(3000):
                tmp = {
                    "book_id": j,
                    "word_count": j,
                    "book_describe": f"book_{j}",
                    "binary_vector": gen_vector(datatype="BinaryVector", dim=dim)
                }
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data
            }
            rsp = self.vector_client.vector_insert(payload)
        c = Collection(name)
        c.flush()
        # list index, expect empty
        rsp = self.index_client.index_list(name)

        # create index
        index_name = "binary_vector_index"
        payload = {
            "collectionName": name,
            "indexParams": [{"fieldName": "binary_vector", "indexName": index_name, "metricType": metric_type, "indexType": index_type,
                             "params": {"index_type": index_type}}]
        }
        if index_type == "BIN_IVF_FLAT":
            payload["indexParams"][0]["params"]["nlist"] = "16384"
        rsp = self.index_client.index_create(payload)
        assert rsp['code'] == 0
        time.sleep(10)
        # list index, expect not empty
        rsp = self.index_client.index_list(collection_name=name)
        # describe index
        rsp = self.index_client.index_describe(collection_name=name, index_name=index_name)
        assert rsp['code'] == 0
        assert len(rsp['data']) == len(payload['indexParams'])
        expected_index = sorted(payload['indexParams'], key=lambda x: x['fieldName'])
        actual_index = sorted(rsp['data'], key=lambda x: x['fieldName'])
        for i in range(len(expected_index)):
            assert expected_index[i]['fieldName'] == actual_index[i]['fieldName']
            assert expected_index[i]['indexName'] == actual_index[i]['indexName']
            assert expected_index[i]['indexType'] == actual_index[i]['indexType']

    @pytest.mark.parametrize("insert_round", [1])
    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("is_partition_key", [True])
    @pytest.mark.parametrize("enable_dynamic_schema", [True])
    @pytest.mark.parametrize("nb", [3000])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("tokenizer", ['standard', 'jieba'])
    @pytest.mark.parametrize("index_type", ['SPARSE_INVERTED_INDEX', 'SPARSE_WAND'])
    @pytest.mark.parametrize("bm25_k1", [1.2, 1.5])
    @pytest.mark.parametrize("bm25_b", [0.7, 0.5])
    @pytest.mark.xfail(reason="issue: https://github.com/milvus-io/milvus/issues/36365")
    def test_create_index_for_full_text_search(self, nb, dim, insert_round, auto_id, is_partition_key,
                                               enable_dynamic_schema, tokenizer, index_type, bm25_k1, bm25_b):
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
                        "user_id": idx % 100,
                        "word_count": j,
                        "book_describe": f"book_{idx}",
                        "document_content": fake.text().lower(),
                    }
                else:
                    tmp = {
                        "book_id": idx,
                        "user_id": idx % 100,
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

        # create index
        payload = {
            "collectionName": name,
            "indexParams": [
                {"fieldName": "sparse_vector", "indexName": "sparse_vector",
                 "metricType": "BM25",
                 "indexType": index_type,
                 "params": {"bm25_k1": bm25_k1, "bm25_b": bm25_b}
                 }
            ]
        }
        rsp = self.index_client.index_create(payload)
        c = Collection(name)
        index_info = [index.to_dict() for index in c.indexes]
        logger.info(f"index_info: {index_info}")
        for info in index_info:
            assert info['index_param']['metric_type'] == 'BM25'
            assert info['index_param']["params"]['bm25_k1'] == bm25_k1
            assert info['index_param']["params"]['bm25_b'] == bm25_b
            assert info['index_param']['index_type'] == index_type


@pytest.mark.L1
class TestCreateIndexNegative(TestBase):

    @pytest.mark.parametrize("index_type", ["BIN_FLAT", "BIN_IVF_FLAT"])
    @pytest.mark.parametrize("metric_type", ["L2", "IP", "COSINE"])
    @pytest.mark.parametrize("dim", [128])
    def test_index_for_binary_vector_field_with_mismatch_metric_type(self, dim, metric_type, index_type):
        """
        """
        name = gen_collection_name()
        client = self.collection_client
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "binary_vector", "dataType": "BinaryVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            }
        }
        logger.info(f"create collection {name} with payload: {payload}")
        rsp = client.collection_create(payload)
        # insert data
        for i in range(1):
            data = []
            for j in range(3000):
                tmp = {
                    "book_id": j,
                    "word_count": j,
                    "book_describe": f"book_{j}",
                    "binary_vector": gen_vector(datatype="BinaryVector", dim=dim)
                }
                data.append(tmp)
            payload = {
                "collectionName": name,
                "data": data
            }
            rsp = self.vector_client.vector_insert(payload)
        c = Collection(name)
        c.flush()
        # list index, expect empty
        rsp = self.index_client.index_list(name)

        # create index
        index_name = "binary_vector_index"
        payload = {
            "collectionName": name,
            "indexParams": [{"fieldName": "binary_vector", "indexName": index_name, "metricType": metric_type,
                             "params": {"index_type": index_type}}]
        }
        if index_type == "BIN_IVF_FLAT":
            payload["indexParams"][0]["params"]["nlist"] = "16384"
        rsp = self.index_client.index_create(payload)
        assert rsp['code'] == 1100
        assert "not supported" in rsp['message']
