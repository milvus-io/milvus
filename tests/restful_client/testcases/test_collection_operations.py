import datetime
import logging
import time
from utils.util_log import test_log as logger
from utils.utils import gen_collection_name
import pytest
from api.milvus import CollectionClient
from base.testbase import TestBase
import threading


@pytest.mark.L0
class TestCreateCollection(TestBase):

    @pytest.mark.parametrize("vector_field", [None, "vector", "emb"])
    @pytest.mark.parametrize("primary_field", [None, "id", "doc_id"])
    @pytest.mark.parametrize("metric_type", ["L2", "IP"])
    @pytest.mark.parametrize("dim", [32, 32768])
    @pytest.mark.parametrize("db_name", ["prod", "default"])
    def test_create_collections_default(self, dim, metric_type, primary_field, vector_field, db_name):
        """
        target: test create collection
        method: create a collection with a simple schema
        expected: create collection success
        """
        self.create_database(db_name)
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        client.db_name = db_name
        payload = {
            "collectionName": name,
            "dimension": dim,
            "metricType": metric_type,
            "primaryField": primary_field,
            "vectorField": vector_field,
        }
        if primary_field is None:
            del payload["primaryField"]
        if vector_field is None:
            del payload["vectorField"]
        logging.info(f"create collection {name} with payload: {payload}")
        rsp = client.collection_create(payload)
        assert rsp['code'] == 200
        rsp = client.collection_list()

        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        rsp = client.collection_describe(name)
        assert rsp['code'] == 200
        assert rsp['data']['collectionName'] == name

    def test_create_collections_concurrent_with_same_param(self):
        """
        target: test create collection with same param
        method: concurrent create collections with same param with multi thread
        expected: create collections all success
        """
        concurrent_rsp = []

        def create_collection(c_name, vector_dim, c_metric_type):
            collection_payload = {
                "collectionName": c_name,
                "dimension": vector_dim,
                "metricType": c_metric_type,
            }
            rsp = client.collection_create(collection_payload)
            concurrent_rsp.append(rsp)
            logger.info(rsp)

        name = gen_collection_name()
        dim = 128
        metric_type = "L2"
        client = self.collection_client
        threads = []
        for i in range(10):
            t = threading.Thread(target=create_collection, args=(name, dim, metric_type,))
            threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        time.sleep(10)
        success_cnt = 0
        for rsp in concurrent_rsp:
            if rsp["code"] == 200:
                success_cnt += 1
        logger.info(concurrent_rsp)
        assert success_cnt == 10
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        rsp = client.collection_describe(name)
        assert rsp['code'] == 200
        assert rsp['data']['collectionName'] == name
        assert f"FloatVector({dim})" in str(rsp['data']['fields'])

    def test_create_collections_concurrent_with_different_param(self):
        """
        target: test create collection with different param
        method: concurrent create collections with different param with multi thread
        expected: only one collection can success
        """
        concurrent_rsp = []

        def create_collection(c_name, vector_dim, c_metric_type):
            collection_payload = {
                "collectionName": c_name,
                "dimension": vector_dim,
                "metricType": c_metric_type,
            }
            rsp = client.collection_create(collection_payload)
            concurrent_rsp.append(rsp)
            logger.info(rsp)

        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        threads = []
        for i in range(0, 5):
            t = threading.Thread(target=create_collection, args=(name, dim + i, "L2",))
            threads.append(t)
        for i in range(5, 10):
            t = threading.Thread(target=create_collection, args=(name, dim + i, "IP",))
            threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        time.sleep(10)
        success_cnt = 0
        for rsp in concurrent_rsp:
            if rsp["code"] == 200:
                success_cnt += 1
        logger.info(concurrent_rsp)
        assert success_cnt == 1
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        rsp = client.collection_describe(name)
        assert rsp['code'] == 200
        assert rsp['data']['collectionName'] == name


@pytest.mark.L1
class TestCreateCollectionNegative(TestBase):

    def test_create_collections_with_invalid_api_key(self):
        """
        target: test create collection with invalid api key(wrong username and password)
        method: create collections with invalid api key
        expected: create collection failed
        """
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        client.api_key = "illegal_api_key"
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = client.collection_create(payload)
        assert rsp['code'] == 1800

    @pytest.mark.parametrize("name",
                             [" ", "test_collection_" * 100, "test collection", "test/collection", "test\collection"])
    def test_create_collections_with_invalid_collection_name(self, name):
        """
        target: test create collection with invalid collection name
        method: create collections with invalid collection name
        expected: create collection failed with right error message
        """
        dim = 128
        client = self.collection_client
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = client.collection_create(payload)
        assert rsp['code'] == 1


@pytest.mark.L0
class TestListCollections(TestBase):

    def test_list_collections_default(self):
        """
        target: test list collection with a simple schema
        method: create collections and list them
        expected: created collections are in list
        """
        client = self.collection_client
        name_list = []
        for i in range(2):
            name = gen_collection_name()
            dim = 128
            payload = {
                "collectionName": name,
                "dimension": dim,
            }
            time.sleep(1)
            rsp = client.collection_create(payload)
            assert rsp['code'] == 200
            name_list.append(name)
        rsp = client.collection_list()
        all_collections = rsp['data']
        for name in name_list:
            assert name in all_collections


@pytest.mark.L1
class TestListCollectionsNegative(TestBase):
    def test_list_collections_with_invalid_api_key(self):
        """
        target: test list collection with an invalid api key
        method: list collection with invalid api key
        expected: raise error with right error code and message
        """
        client = self.collection_client
        name_list = []
        for i in range(2):
            name = gen_collection_name()
            dim = 128
            payload = {
                "collectionName": name,
                "dimension": dim,
            }
            time.sleep(1)
            rsp = client.collection_create(payload)
            assert rsp['code'] == 200
            name_list.append(name)
        client = self.collection_client
        client.api_key = "illegal_api_key"
        rsp = client.collection_list()
        assert rsp['code'] == 1800


@pytest.mark.L0
class TestDescribeCollection(TestBase):

    def test_describe_collections_default(self):
        """
        target: test describe collection with a simple schema
        method: describe collection
        expected: info of description is same with param passed to create collection
        """
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = client.collection_create(payload)
        assert rsp['code'] == 200
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        rsp = client.collection_describe(name)
        assert rsp['code'] == 200
        assert rsp['data']['collectionName'] == name
        assert f"FloatVector({dim})" in str(rsp['data']['fields'])


@pytest.mark.L1
class TestDescribeCollectionNegative(TestBase):
    def test_describe_collections_with_invalid_api_key(self):
        """
        target: test describe collection with invalid api key
        method: describe collection with invalid api key
        expected: raise error with right error code and message
        """
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = client.collection_create(payload)
        assert rsp['code'] == 200
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        illegal_client = CollectionClient(self.url, "illegal_api_key")
        rsp = illegal_client.collection_describe(name)
        assert rsp['code'] == 1800

    def test_describe_collections_with_invalid_collection_name(self):
        """
        target: test describe collection with invalid collection name
        method: describe collection with invalid collection name
        expected: raise error with right error code and message
        """
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = client.collection_create(payload)
        assert rsp['code'] == 200
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        invalid_name = "invalid_name"
        rsp = client.collection_describe(invalid_name)
        assert rsp['code'] == 1


@pytest.mark.L0
class TestDropCollection(TestBase):
    def test_drop_collections_default(self):
        """
        Drop a collection with a simple schema
        target: test drop collection with a simple schema
        method: drop collection
        expected: dropped collection was not in collection list
        """
        clo_list = []
        for i in range(5):
            time.sleep(1)
            name = 'test_collection_' + datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f_%f")
            payload = {
                "collectionName": name,
                "dimension": 128,
            }
            rsp = self.collection_client.collection_create(payload)
            assert rsp['code'] == 200
            clo_list.append(name)
        rsp = self.collection_client.collection_list()
        all_collections = rsp['data']
        for name in clo_list:
            assert name in all_collections
        for name in clo_list:
            time.sleep(0.2)
            payload = {
                "collectionName": name,
            }
            rsp = self.collection_client.collection_drop(payload)
            assert rsp['code'] == 200
        rsp = self.collection_client.collection_list()
        all_collections = rsp['data']
        for name in clo_list:
            assert name not in all_collections


@pytest.mark.L1
class TestDropCollectionNegative(TestBase):
    def test_drop_collections_with_invalid_api_key(self):
        """
        target: test drop collection with invalid api key
        method: drop collection with invalid api key
        expected: raise error with right error code and message; collection still in collection list
        """
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = client.collection_create(payload)
        assert rsp['code'] == 200
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        # drop collection
        payload = {
            "collectionName": name,
        }
        illegal_client = CollectionClient(self.url, "invalid_api_key")
        rsp = illegal_client.collection_drop(payload)
        assert rsp['code'] == 1800
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections

    def test_drop_collections_with_invalid_collection_name(self):
        """
        target: test drop collection with invalid collection name
        method: drop collection with invalid collection name
        expected: raise error with right error code and message
        """
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = client.collection_create(payload)
        assert rsp['code'] == 200
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        # drop collection
        invalid_name = "invalid_name"
        payload = {
            "collectionName": invalid_name,
        }
        rsp = client.collection_drop(payload)
        assert rsp['code'] == 100
