import datetime
import logging
import time
from utils.util_log import test_log as logger
from utils.utils import gen_collection_name
import pytest
from api.milvus import CollectionClient
from base.testbase import TestBase
import threading
from utils.utils import get_data_by_payload
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
    Collection
)


@pytest.mark.L0
class TestCreateCollection(TestBase):

    @pytest.mark.parametrize("dim", [128])
    def test_create_collections_quick_setup(self, dim):
        """
        target: test create collection
        method: create a collection with a simple schema
        expected: create collection success
        """
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        logging.info(f"create collection {name} with payload: {payload}")
        rsp = client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = client.collection_list()

        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        rsp = client.collection_describe(name)
        assert rsp['code'] == 0
        assert rsp['data']['collectionName'] == name
        assert rsp['data']['autoId'] is False
        assert rsp['data']['enableDynamicField'] is True
        assert "COSINE" in str(rsp['data']["indexes"])

    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("metric_type", ["L2", "COSINE", "IP"])
    @pytest.mark.parametrize("id_type", ["Int64", "VarChar"])
    @pytest.mark.parametrize("primary_field", ["id", "url"])
    @pytest.mark.parametrize("vector_field", ["vector", "embedding"])
    def test_create_collection_quick_setup_with_custom(self, vector_field, primary_field, dim, id_type, metric_type):
        """
        Insert a vector with a simple payload
        """
        # create a collection
        name = gen_collection_name()
        collection_payload = {
            "collectionName": name,
            "dimension": dim,
            "metricType": metric_type,
            "primaryFieldName": primary_field,
            "vectorFieldName": vector_field,
            "idType": id_type,
        }
        if id_type == "VarChar":
            collection_payload["params"] = {"max_length": "256"}
        rsp = self.collection_client.collection_create(collection_payload)
        assert rsp['code'] == 0
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 0
        assert rsp['data']['collectionName'] == name
        fields = [f["name"] for f in rsp['data']['fields']]
        assert primary_field in fields
        assert vector_field in fields
        for f in rsp['data']['fields']:
            if f['name'] == primary_field:
                assert f['type'] == id_type
                assert f['primaryKey'] is True
        for index in rsp['data']['indexes']:
            assert index['metricType'] == metric_type

    @pytest.mark.parametrize("enable_dynamic_field", [False, "False", "0"])
    @pytest.mark.parametrize("request_shards_num", [2, "2"])
    @pytest.mark.parametrize("request_ttl_seconds", [360, "360"])
    def test_create_collections_without_params(self, enable_dynamic_field, request_shards_num, request_ttl_seconds):
        """
        target: test create collection
        method: create a collection with a simple schema
        expected: create collection success
        """
        name = gen_collection_name()
        dim = 128
        metric_type = "COSINE"
        client = self.collection_client
        num_shards = 2
        consistency_level = "Strong"
        ttl_seconds = 360
        payload = {
            "collectionName": name,
            "dimension": dim,
            "metricType": metric_type,
            "params":{
                "enableDynamicField": enable_dynamic_field,
                "shardsNum": request_shards_num,
                "consistencyLevel": f"{consistency_level}",
                "ttlSeconds": request_ttl_seconds,
            },
        }

        logging.info(f"create collection {name} with payload: {payload}")
        rsp = client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = client.collection_list()

        all_collections = rsp['data']
        assert name in all_collections
        # describe collection by pymilvus
        c = Collection(name)
        res = c.describe()
        logger.info(f"describe collection: {res}")
        # describe collection
        time.sleep(10)
        rsp = client.collection_describe(name)
        logger.info(f"describe collection: {rsp}")

        ttl_seconds_actual = None
        for d in rsp["data"]["properties"]:
            if d["key"] == "collection.ttl.seconds":
                ttl_seconds_actual = int(d["value"])
        assert rsp['code'] == 0
        assert rsp['data']['enableDynamicField'] == False
        assert rsp['data']['collectionName'] == name
        assert rsp['data']['shardsNum'] == num_shards
        assert rsp['data']['consistencyLevel'] == consistency_level
        assert ttl_seconds_actual == ttl_seconds

    def test_create_collections_with_all_params(self):
        """
        target: test create collection
        method: create a collection with a simple schema
        expected: create collection success
        """
        name = gen_collection_name()
        dim = 128
        metric_type = "COSINE"
        client = self.collection_client
        num_shards = 2
        num_partitions = 36
        consistency_level = "Strong"
        ttl_seconds = 360
        payload = {
            "collectionName": name,
            "enableDynamicField": True,
            "params":{
                "shardsNum": f"{num_shards}",
                "partitionsNum": f"{num_partitions}",
                "consistencyLevel": f"{consistency_level}",
                "ttlSeconds": f"{ttl_seconds}",
            },
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "isPartitionKey": True, "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "json", "dataType": "JSON", "elementTypeParams": {}},
                    {"fieldName": "int_array", "dataType": "Array", "elementDataType": "Int64",
                     "elementTypeParams": {"max_capacity": "1024"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [
                {"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": f"{metric_type}"}]
        }

        logging.info(f"create collection {name} with payload: {payload}")
        rsp = client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = client.collection_list()

        all_collections = rsp['data']
        assert name in all_collections
        # describe collection by pymilvus
        c = Collection(name)
        res = c.describe()
        logger.info(f"describe collection: {res}")
        # describe collection
        time.sleep(10)
        rsp = client.collection_describe(name)
        logger.info(f"describe collection: {rsp}")

        ttl_seconds_actual = None
        for d in rsp["data"]["properties"]:
            if d["key"] == "collection.ttl.seconds":
                ttl_seconds_actual = int(d["value"])
        assert rsp['code'] == 0
        assert rsp['data']['collectionName'] == name
        assert rsp['data']['shardsNum'] == num_shards
        assert rsp['data']['partitionsNum'] == num_partitions
        assert rsp['data']['consistencyLevel'] == consistency_level
        assert ttl_seconds_actual == ttl_seconds


    @pytest.mark.parametrize("auto_id", [True, False])
    @pytest.mark.parametrize("enable_dynamic_field", [True, False])
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("dim", [128])
    def test_create_collections_custom_without_index(self, dim, auto_id, enable_dynamic_field, enable_partition_key):
        """
        target: test create collection
        method: create a collection with a simple schema
        expected: create collection success
        """
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": auto_id,
                "enableDynamicField": enable_dynamic_field,
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "user_id", "dataType": "Int64", "isPartitionKey": enable_partition_key,
                     "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "image_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            }
        }
        logging.info(f"create collection {name} with payload: {payload}")
        rsp = client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = client.collection_list()

        all_collections = rsp['data']
        assert name in all_collections
        c = Collection(name)
        logger.info(f"schema: {c.schema}")
        # describe collection
        rsp = client.collection_describe(name)
        assert rsp['code'] == 0
        assert rsp['data']['collectionName'] == name
        assert rsp['data']['autoId'] == auto_id
        assert c.schema.auto_id == auto_id
        assert rsp['data']['enableDynamicField'] == enable_dynamic_field
        assert c.schema.enable_dynamic_field == enable_dynamic_field
        # assert no index created
        indexes = rsp['data']['indexes']
        assert len(indexes) == 0
        # assert not loaded
        assert rsp['data']['load'] == "LoadStateNotLoad"
        for field in rsp['data']['fields']:
            if field['name'] == "user_id":
                assert field['partitionKey'] == enable_partition_key
        for field in c.schema.fields:
            if field.name == "user_id":
                assert field.is_partition_key == enable_partition_key

    @pytest.mark.parametrize("metric_type", ["L2", "IP", "COSINE"])
    @pytest.mark.parametrize("dim", [128])
    def test_create_collections_one_float_vector_with_index(self, dim, metric_type):
        """
        target: test create collection
        method: create a collection with a simple schema
        expected: create collection success
        """
        name = gen_collection_name()
        dim = 128
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
            },
            "indexParams": [
                {"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": f"{metric_type}"}]
        }
        logging.info(f"create collection {name} with payload: {payload}")
        rsp = client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = client.collection_list()

        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        time.sleep(10)
        rsp = client.collection_describe(name)
        assert rsp['code'] == 0
        assert rsp['data']['collectionName'] == name
        # assert index created
        indexes = rsp['data']['indexes']
        assert len(indexes) == len(payload['indexParams'])
        # assert load success
        assert rsp['data']['load'] == "LoadStateLoaded"

    @pytest.mark.parametrize("metric_type", ["L2", "IP", "COSINE"])
    @pytest.mark.parametrize("dim", [128])
    def test_create_collections_multi_float_vector_with_one_index(self, dim, metric_type):
        """
        target: test create collection
        method: create a collection with a simple schema
        expected: create collection success
        """
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "image_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [
                {"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": f"{metric_type}"}]
        }
        logging.info(f"create collection {name} with payload: {payload}")
        rsp = client.collection_create(payload)
        assert rsp['code'] == 65535
        rsp = client.collection_list()

        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        time.sleep(10)
        rsp = client.collection_describe(name)
        assert rsp['code'] == 0
        assert rsp['data']['collectionName'] == name
        # assert index created
        indexes = rsp['data']['indexes']
        assert len(indexes) == len(payload['indexParams'])
        # assert load success
        assert rsp['data']['load'] == "LoadStateNotLoad"

    @pytest.mark.parametrize("metric_type", ["L2", "IP", "COSINE"])
    @pytest.mark.parametrize("dim", [128])
    def test_create_collections_multi_float_vector_with_all_index(self, dim, metric_type):
        """
        target: test create collection
        method: create a collection with a simple schema
        expected: create collection success
        """
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "image_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [
                {"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": f"{metric_type}"},
                {"fieldName": "image_intro", "indexName": "image_intro_vector", "metricType": f"{metric_type}"}]
        }
        logging.info(f"create collection {name} with payload: {payload}")
        rsp = client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = client.collection_list()

        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        time.sleep(10)
        rsp = client.collection_describe(name)
        assert rsp['code'] == 0
        assert rsp['data']['collectionName'] == name
        # assert index created
        indexes = rsp['data']['indexes']
        assert len(indexes) == len(payload['indexParams'])
        # assert load success
        assert rsp['data']['load'] == "LoadStateLoaded"

    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("enable_dynamic_field", [True])
    @pytest.mark.parametrize("enable_partition_key", [True])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("metric_type", ["L2", "IP", "COSINE"])
    def test_create_collections_float16_vector_datatype(self, dim, auto_id, enable_dynamic_field, enable_partition_key,
                                                        metric_type):
        """
        target: test create collection
        method: create a collection with a simple schema
        expected: create collection success
        """
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": auto_id,
                "enableDynamicField": enable_dynamic_field,
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "float16_vector", "dataType": "Float16Vector",
                     "elementTypeParams": {"dim": f"{dim}"}},
                    {"fieldName": "bfloat16_vector", "dataType": "BFloat16Vector",
                     "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [
                {"fieldName": "float16_vector", "indexName": "float16_vector_index", "metricType": f"{metric_type}"},
                {"fieldName": "bfloat16_vector", "indexName": "bfloat16_vector_index", "metricType": f"{metric_type}"}]

        }
        logging.info(f"create collection {name} with payload: {payload}")
        rsp = client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = client.collection_list()

        all_collections = rsp['data']
        assert name in all_collections
        c = Collection(name)
        logger.info(f"schema: {c.schema}")
        # describe collection
        rsp = client.collection_describe(name)
        assert rsp['code'] == 0
        assert rsp['data']['collectionName'] == name
        assert len(rsp['data']['fields']) == len(c.schema.fields)

    @pytest.mark.parametrize("auto_id", [True])
    @pytest.mark.parametrize("enable_dynamic_field", [True])
    @pytest.mark.parametrize("enable_partition_key", [True])
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("metric_type", ["JACCARD", "HAMMING"])
    @pytest.mark.skip(reason="https://github.com/milvus-io/milvus/issues/31494")
    def test_create_collections_binary_vector_datatype(self, dim, auto_id, enable_dynamic_field, enable_partition_key,
                                                       metric_type):
        """
        target: test create collection
        method: create a collection with a simple schema
        expected: create collection success
        """
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": auto_id,
                "enableDynamicField": enable_dynamic_field,
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "binary_vector", "dataType": "BinaryVector", "elementTypeParams": {"dim": f"{dim}"}},
                ]
            },
            "indexParams": [
                {"fieldName": "binary_vector", "indexName": "binary_vector_index", "metricType": f"{metric_type}"}
            ]

        }
        logging.info(f"create collection {name} with payload: {payload}")
        rsp = client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = client.collection_list()

        all_collections = rsp['data']
        assert name in all_collections
        c = Collection(name)
        logger.info(f"schema: {c.schema}")
        # describe collection
        rsp = client.collection_describe(name)
        assert rsp['code'] == 0
        assert rsp['data']['collectionName'] == name
        assert len(rsp['data']['fields']) == len(c.schema.fields)

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
            if rsp['code'] == 0:
                success_cnt += 1
        logger.info(concurrent_rsp)
        assert success_cnt == 10
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        rsp = client.collection_describe(name)
        assert rsp['code'] == 0
        assert rsp['data']['collectionName'] == name

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
            if rsp['code'] == 0:
                success_cnt += 1
        logger.info(concurrent_rsp)
        assert success_cnt == 1
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        rsp = client.collection_describe(name)
        assert rsp['code'] == 0
        assert rsp['data']['collectionName'] == name


@pytest.mark.L1
class TestCreateCollectionNegative(TestBase):

    def test_create_collections_custom_with_invalid_datatype(self):
        """
        """
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VARCHAR", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            }
        }
        logging.info(f"create collection {name} with payload: {payload}")
        rsp = client.collection_create(payload)
        assert rsp['code'] == 1100

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
        assert rsp['code'] == 1100
        assert "Invalid collection name" in rsp['message'] or "invalid parameter" in rsp['message']


@pytest.mark.L0
class TestHasCollections(TestBase):

    def test_has_collections_default(self):
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
                "metricType": "L2",
                "dimension": dim,
            }
            time.sleep(1)
            rsp = client.collection_create(payload)
            assert rsp['code'] == 0
            name_list.append(name)
        rsp = client.collection_list()
        all_collections = rsp['data']
        for name in name_list:
            assert name in all_collections
            rsp = client.collection_has(collection_name=name)
            assert rsp['data']['has'] is True

    def test_has_collections_with_not_exist_name(self):
        """
        target: test list collection with a simple schema
        method: create collections and list them
        expected: created collections are in list
        """
        client = self.collection_client
        name_list = []
        for i in range(2):
            name = gen_collection_name()
            name_list.append(name)
        rsp = client.collection_list()
        all_collections = rsp['data']
        for name in name_list:
            assert name not in all_collections
            rsp = client.collection_has(collection_name=name)
            assert rsp['data']['has'] is False


@pytest.mark.L0
class TestGetCollectionStats(TestBase):

    def test_get_collections_stats(self):
        """
        target: test list collection with a simple schema
        method: create collections and list them
        expected: created collections are in list
        """
        client = self.collection_client
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "metricType": "L2",
            "dimension": dim,
        }
        time.sleep(1)
        rsp = client.collection_create(payload)
        assert rsp['code'] == 0
        # describe collection
        client.collection_describe(collection_name=name)
        rsp = client.collection_stats(collection_name=name)
        assert rsp['code'] == 0
        assert rsp['data']['rowCount'] == 0
        # insert data
        nb = 3000
        data = get_data_by_payload(payload, nb)
        payload = {
            "collectionName": name,
            "data": data
        }
        self.vector_client.vector_insert(payload=payload)
        c = Collection(name)
        count = c.query(expr="", output_fields=["count(*)"])
        logger.info(f"count: {count}")
        c.flush()
        rsp = client.collection_stats(collection_name=name)
        assert rsp['data']['rowCount'] == nb


class TestLoadReleaseCollection(TestBase):

    def test_load_and_release_collection(self):
        name = gen_collection_name()
        dim = 128
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
        rsp = client.collection_create(payload)
        assert rsp['code'] == 0
        # create index before load
        index_params = [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        payload = {
            "collectionName": name,
            "indexParams": index_params
        }
        rsp = self.index_client.index_create(payload)

        # get load state before load
        rsp = client.collection_load_state(collection_name=name)
        assert rsp['data']['loadState'] == "LoadStateNotLoad"

        # describe collection
        client.collection_describe(collection_name=name)
        rsp = client.collection_load(collection_name=name)
        assert rsp['code'] == 0
        rsp = client.collection_load_state(collection_name=name)
        assert rsp['data']['loadState'] in ["LoadStateLoaded", "LoadStateLoading"]
        time.sleep(5)
        rsp = client.collection_load_state(collection_name=name)
        assert rsp['data']['loadState'] == "LoadStateLoaded"

        # release collection
        rsp = client.collection_release(collection_name=name)
        time.sleep(5)
        rsp = client.collection_load_state(collection_name=name)
        assert rsp['data']['loadState'] == "LoadStateNotLoad"

@pytest.mark.L0
class TestGetCollectionLoadState(TestBase):

    def test_get_collection_load_state(self):
        """
        target: test list collection with a simple schema
        method: create collections and list them
        expected: created collections are in list
        """
        client = self.collection_client
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "metricType": "L2",
            "dimension": dim,
        }
        rsp = client.collection_create(payload)
        assert rsp['code'] == 0
        # describe collection
        client.collection_describe(collection_name=name)
        rsp = client.collection_load_state(collection_name=name)
        assert rsp['code'] == 0
        t0 = time.time()
        while time.time() - t0 < 10:
            rsp = client.collection_load_state(collection_name=name)
            if rsp['data']['loadState'] != "LoadStateNotLoad":
                break
            time.sleep(1)
        assert rsp['data']['loadState'] in ["LoadStateLoading", "LoadStateLoaded"]
        # insert data
        nb = 3000
        data = get_data_by_payload(payload, nb)
        payload = {
            "collectionName": name,
            "data": data
        }
        self.vector_client.vector_insert(payload=payload)
        rsp = client.collection_load_state(collection_name=name)
        assert rsp['data']['loadState'] in ["LoadStateLoading", "LoadStateLoaded"]
        time.sleep(10)
        rsp = client.collection_load_state(collection_name=name)
        assert rsp['data']['loadState'] == "LoadStateLoaded"


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
                "metricType": "L2",
                "dimension": dim,
            }
            time.sleep(1)
            rsp = client.collection_create(payload)
            assert rsp['code'] == 0
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
                "metricType": "L2",
                "dimension": dim,
            }
            time.sleep(1)
            rsp = client.collection_create(payload)
            assert rsp['code'] == 0
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
            "metricType": "L2"
        }
        rsp = client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        rsp = client.collection_describe(name)
        assert rsp['code'] == 0
        assert rsp['data']['collectionName'] == name
        assert rsp['data']['autoId'] is False
        assert rsp['data']['enableDynamicField'] is True
        assert len(rsp['data']['indexes']) == 1

    def test_describe_collections_custom(self):
        """
        target: test describe collection with a simple schema
        method: describe collection
        expected: info of description is same with param passed to create collection
        """
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        fields = [
            FieldSchema(name='reviewer_id', dtype=DataType.INT64, description="", is_primary=True),
            FieldSchema(name='store_address', dtype=DataType.VARCHAR, description="", max_length=512,
                        is_partition_key=True),
            FieldSchema(name='review', dtype=DataType.VARCHAR, description="", max_length=16384),
            FieldSchema(name='vector', dtype=DataType.FLOAT_VECTOR, description="", dim=384, is_index=True),
        ]

        schema = CollectionSchema(
            fields=fields,
            description="",
            enable_dynamic_field=True,
            # The following is an alternative to setting `is_partition_key` in a field schema.
            partition_key_field="store_address"
        )

        collection = Collection(
            name=name,
            schema=schema,
        )
        logger.info(f"schema: {schema}")
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        rsp = client.collection_describe(name)
        assert rsp['code'] == 0
        assert rsp['data']['collectionName'] == name

        for field in rsp['data']['fields']:
            if field['name'] == "store_address":
                assert field['partitionKey'] is True
            if field['name'] == "reviewer_id":
                assert field['primaryKey'] is True
        assert rsp['data']['autoId'] is False
        assert rsp['data']['enableDynamicField'] is True


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
        assert rsp['code'] == 0
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        illegal_client = CollectionClient(self.endpoint, "illegal_api_key")
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
        assert rsp['code'] == 0
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        invalid_name = "invalid_name"
        rsp = client.collection_describe(invalid_name)
        assert rsp['code'] == 100
        assert "can't find collection" in rsp['message']


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
                "metricType": "L2"
            }
            rsp = self.collection_client.collection_create(payload)
            assert rsp['code'] == 0
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
            assert rsp['code'] == 0
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
        assert rsp['code'] == 0
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        # drop collection
        payload = {
            "collectionName": name,
        }
        illegal_client = CollectionClient(self.endpoint, "invalid_api_key")
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
        assert rsp['code'] == 0
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        # drop collection
        invalid_name = "invalid_name"
        payload = {
            "collectionName": invalid_name,
        }
        rsp = client.collection_drop(payload)
        assert rsp['code'] == 0


@pytest.mark.L0
class TestRenameCollection(TestBase):

    def test_rename_collection(self):
        """
        target: test rename collection
        method: rename collection
        expected: renamed collection is in collection list
        """
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        payload = {
            "collectionName": name,
            "metricType": "L2",
            "dimension": dim,
        }
        rsp = client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        new_name = gen_collection_name()
        payload = {
            "collectionName": name,
            "newCollectionName": new_name,
        }
        rsp = client.collection_rename(payload)
        assert rsp['code'] == 0
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert new_name in all_collections
        assert name not in all_collections
