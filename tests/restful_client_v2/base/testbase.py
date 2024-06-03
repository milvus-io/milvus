import json
import sys
import pytest
import time
from pymilvus import connections, db
from utils.util_log import test_log as logger
from api.milvus import (VectorClient, CollectionClient, PartitionClient, IndexClient, AliasClient,
                        UserClient, RoleClient, ImportJobClient, StorageClient)
from utils.utils import get_data_by_payload


def get_config():
    pass


class Base:
    name = None
    protocol = None
    host = None
    port = None
    endpoint = None
    api_key = None
    username = None
    password = None
    invalid_api_key = None
    vector_client = None
    collection_client = None
    partition_client = None
    index_client = None
    alias_client = None
    user_client = None
    role_client = None
    import_job_client = None
    storage_client = None


class TestBase(Base):

    def teardown_method(self):
        self.collection_client.api_key = self.api_key
        all_collections = self.collection_client.collection_list()['data']
        if self.name in all_collections:
            logger.info(f"collection {self.name} exist, drop it")
            payload = {
                "collectionName": self.name,
            }
            try:
                rsp = self.collection_client.collection_drop(payload)
            except Exception as e:
                logger.error(e)

    @pytest.fixture(scope="function", autouse=True)
    def init_client(self, endpoint, token, minio_host, bucket_name, root_path):
        self.endpoint = f"{endpoint}"
        self.api_key = f"{token}"
        self.invalid_api_key = "invalid_token"
        self.vector_client = VectorClient(self.endpoint, self.api_key)
        self.collection_client = CollectionClient(self.endpoint, self.api_key)
        self.partition_client = PartitionClient(self.endpoint, self.api_key)
        self.index_client = IndexClient(self.endpoint, self.api_key)
        self.alias_client = AliasClient(self.endpoint, self.api_key)
        self.user_client = UserClient(self.endpoint, self.api_key)
        self.role_client = RoleClient(self.endpoint, self.api_key)
        self.import_job_client = ImportJobClient(self.endpoint, self.api_key)
        self.storage_client = StorageClient(f"{minio_host}:9000", "minioadmin", "minioadmin", bucket_name, root_path)
        if token is None:
            self.vector_client.api_key = None
            self.collection_client.api_key = None
            self.partition_client.api_key = None
        connections.connect(uri=endpoint, token=token)

    def init_collection(self, collection_name, pk_field="id", metric_type="L2", dim=128, nb=100, batch_size=1000, return_insert_id=False):
        # create collection
        schema_payload = {
            "collectionName": collection_name,
            "dimension": dim,
            "metricType": metric_type,
            "description": "test collection",
            "primaryField": pk_field,
            "vectorField": "vector",
        }
        rsp = self.collection_client.collection_create(schema_payload)
        assert rsp['code'] == 0
        self.wait_collection_load_completed(collection_name)
        batch_size = batch_size
        batch = nb // batch_size
        remainder = nb % batch_size
        data = []
        insert_ids = []
        for i in range(batch):
            nb = batch_size
            data = get_data_by_payload(schema_payload, nb)
            payload = {
                "collectionName": collection_name,
                "data": data
            }
            body_size = sys.getsizeof(json.dumps(payload))
            logger.debug(f"body size: {body_size / 1024 / 1024} MB")
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            if return_insert_id:
                insert_ids.extend(rsp['data']['insertIds'])
        # insert remainder data
        if remainder:
            nb = remainder
            data = get_data_by_payload(schema_payload, nb)
            payload = {
                "collectionName": collection_name,
                "data": data
            }
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 0
            if return_insert_id:
                insert_ids.extend(rsp['data']['insertIds'])
        if return_insert_id:
            return schema_payload, data, insert_ids

        return schema_payload, data

    def wait_collection_load_completed(self, name):
        t0 = time.time()
        timeout = 60
        while True and time.time() - t0 < timeout:
            rsp = self.collection_client.collection_describe(name)
            if "data" in rsp and "load" in rsp["data"] and rsp["data"]["load"] == "LoadStateLoaded":
                break
            else:
                time.sleep(5)

    def create_database(self, db_name="default"):
        all_db = db.list_database()
        logger.info(f"all database: {all_db}")
        if db_name not in all_db:
            logger.info(f"create database: {db_name}")
            try:
                db.create_database(db_name=db_name)
            except Exception as e:
                logger.error(e)

    def update_database(self, db_name="default"):
        self.create_database(db_name=db_name)
        db.using_database(db_name=db_name)
        self.collection_client.db_name = db_name
        self.vector_client.db_name = db_name
        self.import_job_client.db_name = db_name
