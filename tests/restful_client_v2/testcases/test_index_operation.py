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
from utils.utils import gen_vector
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
    Collection
)


@pytest.mark.L0
class TestCreateIndex(TestBase):

    @pytest.mark.parametrize("metric_type", ["L2"])
    @pytest.mark.parametrize("index_type", ["AUTOINDEX", "HNSW"])
    @pytest.mark.parametrize("dim", [128])
    def test_index_e2e(self, dim, metric_type, index_type):
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
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector",
                             "metricType": f"{metric_type}"}]
        }
        if index_type == "HNSW":
            payload["indexParams"][0]["params"] = {"index_type": "HNSW", "M": "16", "efConstruction": "200"}
        if index_type == "AUTOINDEX":
            payload["indexParams"][0]["params"] = {"index_type": "AUTOINDEX"}
        rsp = self.index_client.index_create(payload)
        assert rsp['code'] == 0
        time.sleep(10)
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
            assert expected_index[i]["params"]['index_type'] == actual_index[i]['indexType']

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
            "indexParams": [{"fieldName": "word_count", "indexName": "word_count_vector",
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
            assert expected_index[i]['params']['index_type'] == actual_index[i]['indexType']

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
            "indexParams": [{"fieldName": "binary_vector", "indexName": index_name, "metricType": metric_type,
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
            assert expected_index[i]['params']['index_type'] == actual_index[i]['indexType']


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
