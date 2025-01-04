import random
import time
from utils.utils import gen_collection_name
from utils.util_log import test_log as logger
import pytest
from base.testbase import TestBase
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
    Collection
)


@pytest.mark.L0
class TestRestfulSdkCompatibility(TestBase):

    @pytest.mark.parametrize("dim", [128, 256])
    @pytest.mark.parametrize("enable_dynamic", [True, False])
    @pytest.mark.parametrize("num_shards", [1, 2])
    def test_collection_created_by_sdk_describe_by_restful(self, dim, enable_dynamic, num_shards):
        """
        """
        # 1. create collection by sdk
        name = gen_collection_name()
        default_fields = [
            FieldSchema(name="int64", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="float", dtype=DataType.FLOAT),
            FieldSchema(name="varchar", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=dim)
        ]
        default_schema = CollectionSchema(fields=default_fields, description="test collection",
                                          enable_dynamic_field=enable_dynamic)
        collection = Collection(name=name, schema=default_schema, num_shards=num_shards)
        logger.info(collection.schema)
        # 2. use restful to get collection info
        client = self.collection_client
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        rsp = client.collection_describe(name)
        assert rsp['code'] == 0
        assert rsp['data']['collectionName'] == name
        assert rsp['data']['enableDynamicField'] == enable_dynamic
        assert rsp['data']['load'] == "LoadStateNotLoad"
        assert rsp['data']['shardsNum'] == num_shards

    @pytest.mark.parametrize("metric_type", ["L2", "IP", "COSINE"])
    @pytest.mark.parametrize("dim", [128])
    def test_collection_created_by_restful_describe_by_sdk(self, dim, metric_type):
        """
        """
        name = gen_collection_name()
        dim = 128
        client = self.collection_client
        payload = {
            "collectionName": name,
            "dimension": dim,
            "metricType": metric_type,
        }
        rsp = client.collection_create(payload)
        assert rsp['code'] == 0
        collection = Collection(name=name)
        logger.info(collection.schema)
        field_names = [field.name for field in collection.schema.fields]
        assert len(field_names) == 2
        assert collection.schema.enable_dynamic_field is True
        assert len(collection.indexes) > 0

    @pytest.mark.parametrize("metric_type", ["L2", "IP"])
    def test_collection_created_index_by_sdk_describe_by_restful(self, metric_type):
        """
        """
        # 1. create collection by sdk
        name = gen_collection_name()
        default_fields = [
            FieldSchema(name="int64", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="float", dtype=DataType.FLOAT),
            FieldSchema(name="varchar", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=128)
        ]
        default_schema = CollectionSchema(fields=default_fields, description="test collection",
                                          enable_dynamic_field=True)
        collection = Collection(name=name, schema=default_schema)
        # create index by sdk
        index_param = {"metric_type": metric_type, "index_type": "IVF_FLAT", "params": {"nlist": 128}}
        collection.create_index(field_name="float_vector", index_params=index_param)
        # 2. use restful to get collection info
        client = self.collection_client
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        rsp = client.collection_describe(name)
        assert rsp['code'] == 0
        assert rsp['data']['collectionName'] == name
        assert len(rsp['data']['indexes']) == 1 and rsp['data']['indexes'][0]['metricType'] == metric_type

    @pytest.mark.parametrize("metric_type", ["L2", "IP"])
    def test_collection_load_by_sdk_describe_by_restful(self, metric_type):
        """
        """
        # 1. create collection by sdk
        name = gen_collection_name()
        default_fields = [
            FieldSchema(name="int64", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="float", dtype=DataType.FLOAT),
            FieldSchema(name="varchar", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=128)
        ]
        default_schema = CollectionSchema(fields=default_fields, description="test collection",
                                          enable_dynamic_field=True)
        collection = Collection(name=name, schema=default_schema)
        # create index by sdk
        index_param = {"metric_type": metric_type, "index_type": "IVF_FLAT", "params": {"nlist": 128}}
        collection.create_index(field_name="float_vector", index_params=index_param)
        collection.load()
        # 2. use restful to get collection info
        client = self.collection_client
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        rsp = client.collection_describe(name)
        assert rsp['data']['load'] == "LoadStateLoaded"

    def test_collection_create_by_sdk_insert_vector_by_restful(self):
        """
        """
        # 1. create collection by sdk
        dim = 128
        nb = 100
        name = gen_collection_name()
        default_fields = [
            FieldSchema(name="int64", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="float", dtype=DataType.FLOAT),
            FieldSchema(name="varchar", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="json", dtype=DataType.JSON),
            FieldSchema(name="int_array", dtype=DataType.ARRAY, element_type=DataType.INT64, max_capacity=1024),
            FieldSchema(name="varchar_array", dtype=DataType.ARRAY, element_type=DataType.VARCHAR, max_capacity=1024, max_length=65535),
            FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=128)
        ]
        default_schema = CollectionSchema(fields=default_fields, description="test collection",
                                          enable_dynamic_field=True)
        collection = Collection(name=name, schema=default_schema)
        # create index by sdk
        index_param = {"metric_type": "L2", "index_type": "IVF_FLAT", "params": {"nlist": 128}}
        collection.create_index(field_name="float_vector", index_params=index_param)
        collection.load()
        # insert data by restful
        data = [
            {"int64": i,
             "float": i,
             "varchar": str(i),
             "json": {f"key_{i}": f"value_{i}"},
             "int_array": [random.randint(0, 100) for _ in range(10)],
             "varchar_array": [str(i) for _ in range(10)],
             "float_vector": [random.random() for _ in range(dim)], "age": i}
            for i in range(nb)
        ]
        client = self.vector_client
        payload = {
            "collectionName": name,
            "data": data,
        }
        rsp = client.vector_insert(payload)
        assert rsp['code'] == 0
        assert rsp['data']['insertCount'] == nb
        assert len(rsp['data']["insertIds"]) == nb

    def test_collection_create_by_sdk_search_vector_by_restful(self):
        """
        """
        dim = 128
        nb = 100
        name = gen_collection_name()
        default_fields = [
            FieldSchema(name="int64", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="float", dtype=DataType.FLOAT),
            FieldSchema(name="varchar", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=128)
        ]
        default_schema = CollectionSchema(fields=default_fields, description="test collection",
                                          enable_dynamic_field=True)
        # init collection by sdk
        collection = Collection(name=name, schema=default_schema)
        index_param = {"metric_type": "L2", "index_type": "IVF_FLAT", "params": {"nlist": 128}}
        collection.create_index(field_name="float_vector", index_params=index_param)
        collection.load()
        data = [
            {"int64": i, "float": i, "varchar": str(i), "float_vector": [random.random() for _ in range(dim)], "age": i}
            for i in range(nb)
        ]
        collection.insert(data)
        client = self.vector_client
        payload = {
            "collectionName": name,
            "data": [[random.random() for _ in range(dim)]],
            "limit": 10
        }
        # search data by restful
        rsp = client.vector_search(payload)
        assert rsp['code'] == 0
        assert len(rsp['data']) == 1
        assert len(rsp['data'][0]) == 10

    def test_collection_create_by_sdk_query_vector_by_restful(self):
        """
        """
        dim = 128
        nb = 100
        name = gen_collection_name()
        default_fields = [
            FieldSchema(name="int64", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="float", dtype=DataType.FLOAT),
            FieldSchema(name="varchar", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=128)
        ]
        default_schema = CollectionSchema(fields=default_fields, description="test collection",
                                          enable_dynamic_field=True)
        # init collection by sdk
        collection = Collection(name=name, schema=default_schema)
        index_param = {"metric_type": "L2", "index_type": "IVF_FLAT", "params": {"nlist": 128}}
        collection.create_index(field_name="float_vector", index_params=index_param)
        collection.load()
        data = [
            {"int64": i, "float": i, "varchar": str(i), "float_vector": [random.random() for _ in range(dim)], "age": i}
            for i in range(nb)
        ]
        collection.insert(data)
        client = self.vector_client
        payload = {
            "collectionName": name,
            "filter": "int64 in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]",
        }
        # query data by restful
        rsp = client.vector_query(payload)
        assert rsp['code'] == 0
        assert len(rsp['data']) == 10

    def test_collection_create_by_restful_search_vector_by_sdk(self):
        """
        """
        name = gen_collection_name()
        dim = 128
        # insert data by restful
        self.init_collection(name, metric_type="L2", dim=dim)
        time.sleep(5)
        # search data by sdk
        collection = Collection(name=name)
        nq = 5
        vectors_to_search = [[random.random() for i in range(dim)] for j in range(nq)]
        res = collection.search(data=vectors_to_search, anns_field="vector", param={}, limit=10)
        assert len(res) == nq
        assert len(res[0]) == 10

    def test_collection_create_by_restful_query_vector_by_sdk(self):
        """
        """
        name = gen_collection_name()
        dim = 128
        # insert data by restful
        self.init_collection(name, metric_type="L2", dim=dim)
        time.sleep(5)
        # query data by sdk
        collection = Collection(name=name)
        res = collection.query(expr=f"uid in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]", output_fields=["*"])
        for item in res:
            uid = item["uid"]
            assert uid in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    def test_collection_create_by_restful_delete_vector_by_sdk(self):
        """
        """
        name = gen_collection_name()
        dim = 128
        # insert data by restful
        self.init_collection(name, metric_type="L2", dim=dim)
        time.sleep(5)
        # query data by sdk
        collection = Collection(name=name)
        res = collection.query(expr=f"uid in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]", output_fields=["*"])
        pk_id_list = []
        for item in res:
            uid = item["uid"]
            pk_id_list.append(item["id"])
        expr = f"id in {pk_id_list}"
        collection.delete(expr)
        time.sleep(5)
        res = collection.query(expr=f"uid in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]", output_fields=["*"])
        assert len(res) == 0

    def test_collection_create_by_sdk_delete_vector_by_restful(self):
        """
        """
        dim = 128
        nb = 100
        name = gen_collection_name()
        default_fields = [
            FieldSchema(name="int64", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="float", dtype=DataType.FLOAT),
            FieldSchema(name="varchar", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=128)
        ]
        default_schema = CollectionSchema(fields=default_fields, description="test collection",
                                          enable_dynamic_field=True)
        # init collection by sdk
        collection = Collection(name=name, schema=default_schema)
        index_param = {"metric_type": "L2", "index_type": "IVF_FLAT", "params": {"nlist": 128}}
        collection.create_index(field_name="float_vector", index_params=index_param)
        collection.load()
        data = [
            {"int64": i, "float": i, "varchar": str(i), "float_vector": [random.random() for _ in range(dim)], "age": i}
            for i in range(nb)
        ]
        collection.insert(data)
        time.sleep(5)
        res = collection.query(expr=f"int64 in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]", output_fields=["*"])
        pk_id_list = []
        for item in res:
            pk_id_list.append(item["int64"])
        payload = {
            "collectionName": name,
            "filter": f"int64 in {pk_id_list}"
        }
        # delete data by restful
        rsp = self.vector_client.vector_delete(payload)
        time.sleep(5)
        res = collection.query(expr=f"int64 in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]", output_fields=["*"])
        assert len(res) == 0
