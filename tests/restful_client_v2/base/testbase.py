import json
import sys
import pytest
import time
import uuid
from pymilvus import connections, db, MilvusClient
from utils.util_log import test_log as logger
from api.milvus import (VectorClient, CollectionClient, PartitionClient, IndexClient, AliasClient,
                        UserClient, RoleClient, ImportJobClient, StorageClient, Requests, DatabaseClient)
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
    milvus_client = None
    database_client = None


class TestBase(Base):
    req = None

    def teardown_method(self):
        # Clean up collections
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
                logger.error(f"drop collection error: {e}")

        for item in self.collection_client.name_list:
            db_name = item[0]
            c_name = item[1]
            payload = {
                "collectionName": c_name,
                "dbName": db_name
            }
            try:
                self.collection_client.collection_drop(payload)
            except Exception as e:
                logger.error(f"drop collection error: {e}")


        # Clean up databases created by this client
        self.database_client.api_key = self.api_key
        for db_name in self.database_client.db_names[:]:  # Create a copy of the list to iterate
            logger.info(f"database {db_name} exist, drop it")
            try:
                rsp = self.database_client.database_drop({"dbName": db_name})
            except Exception as e:
                logger.error(f"drop database error: {e}")

    @pytest.fixture(scope="function", autouse=True)
    def init_client(self, endpoint, token, minio_host, bucket_name, root_path):
        _uuid = str(uuid.uuid1())
        self.req = Requests()
        self.req.update_uuid(_uuid)
        self.endpoint = f"{endpoint}"
        self.api_key = f"{token}"
        self.invalid_api_key = "invalid_token"
        self.vector_client = VectorClient(self.endpoint, self.api_key)
        self.vector_client.update_uuid(_uuid)
        self.collection_client = CollectionClient(self.endpoint, self.api_key)
        self.collection_client.update_uuid(_uuid)
        self.partition_client = PartitionClient(self.endpoint, self.api_key)
        self.partition_client.update_uuid(_uuid)
        self.index_client = IndexClient(self.endpoint, self.api_key)
        self.index_client.update_uuid(_uuid)
        self.alias_client = AliasClient(self.endpoint, self.api_key)
        self.alias_client.update_uuid(_uuid)
        self.user_client = UserClient(self.endpoint, self.api_key)
        self.user_client.update_uuid(_uuid)
        self.role_client = RoleClient(self.endpoint, self.api_key)
        self.role_client.update_uuid(_uuid)
        self.import_job_client = ImportJobClient(self.endpoint, self.api_key)
        self.import_job_client.update_uuid(_uuid)
        self.storage_client = StorageClient(f"{minio_host}:9000", "minioadmin", "minioadmin", bucket_name, root_path)
        self.database_client = DatabaseClient(self.endpoint, self.api_key)
        self.database_client.update_uuid(_uuid)
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

        full_data = []
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
            full_data.extend(data)
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
            full_data.extend(data)
        if return_insert_id:
            return schema_payload, full_data, insert_ids

        return schema_payload, full_data

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

    def wait_load_completed(self, collection_name, db_name="default", timeout=5):
        t0 = time.time()
        while True and time.time() - t0 < timeout:
            rsp = self.collection_client.collection_describe(collection_name, db_name=db_name)
            if "data" in rsp and "load" in rsp["data"] and rsp["data"]["load"] == "LoadStateLoaded":
                logger.info(f"collection {collection_name} load completed in {time.time() - t0} seconds")
                break
            else:
                time.sleep(1)
