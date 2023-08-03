import json
import sys

import pytest
import time
from pymilvus import connections, db
from utils.util_log import test_log as logger
from api.milvus import VectorClient, CollectionClient
from utils.utils import get_data_by_payload


def get_config():
    pass


class Base:
    name = None
    protocol = None
    host = None
    port = None
    url = None
    uri = None
    api_key = None
    username = None
    name_list = None
    password = None
    invalid_api_key = None
    vector_client = None
    collection_client = None


class TestBase(Base):

    def teardown_method(self):
        self.collection_client.api_key = self.api_key
        all_db = self.list_database()
        for db_name in all_db:
            all_collections = self.collection_client.collection_list(db_name=db_name)['data']
            logger.info(f"all collections: {all_collections}")
            if not isinstance(self.name_list, list):
                c_name_list = [self.name_list]
            else:
                c_name_list = self.name_list
            for c_name in c_name_list:
                if c_name in all_collections:
                    logger.info(f"collection {c_name} exist, drop it")
                    payload = {
                        "collectionName": c_name,
                        "dbName": db_name
                    }
                    try:
                        rsp = self.collection_client.collection_drop(payload)
                        logger.info(f"drop collection {c_name} response: {rsp}")
                    except Exception as e:
                        logger.error(e)

    @pytest.fixture(scope="function", autouse=True)
    def init_client(self, protocol, host, port, username, password):
        self.protocol = protocol
        self.host = host
        self.port = port
        self.url = f"{host}:{port}/v1"
        self.uri = f"{protocol}://{host}:{port}"
        self.username = username
        self.password = password
        self.name_list = []
        self.api_key = f"{self.username}:{self.password}"
        self.invalid_api_key = "invalid_token"
        self.vector_client = VectorClient(self.url, self.api_key, protocol=self.protocol)
        self.collection_client = CollectionClient(self.url, self.api_key, protocol=self.protocol)

    def init_collection(self, collection_name, pk_field="id", metric_type="L2", dim=128, nb=100, batch_size=1000):
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
        assert rsp['code'] == 200
        self.wait_collection_load_completed(collection_name)
        batch_size = batch_size
        batch = nb // batch_size
        # in case of nb < batch_size
        if batch == 0:
            batch = 1
            batch_size = nb
        data = []
        for i in range(batch):
            nb = batch_size
            data = get_data_by_payload(schema_payload, nb)
            payload = {
                "collectionName": collection_name,
                "data": data
            }
            body_size = sys.getsizeof(json.dumps(payload))
            logger.info(f"body size: {body_size / 1024 / 1024} MB")
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 200
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
        try:
            # first try to connect to milvus without auth
            connections.connect(host=self.host, port=self.port)
        except Exception as e:
            # logger.debug(e)
            # if failed, connect to milvus with auth
            connections.connect(uri=self.uri, token=self.api_key)
        all_db = db.list_database()
        logger.info(f"all database: {all_db}")
        if db_name not in all_db:
            logger.info(f"create database: {db_name}")
            try:
                db.create_database(db_name=db_name)
            except Exception as e:
                logger.error(e)

    def list_database(self, db_name="default"):
        try:
            # first try to connect to milvus without auth
            connections.connect(host=self.host, port=self.port)
        except Exception as e:
            logger.debug(e)
            # if failed, connect to milvus with auth
            connections.connect(uri=self.uri, token=self.api_key)
        all_db = db.list_database()
        logger.info(f"all database: {all_db}")
        return all_db

    def update_database(self, db_name="default"):
        self.create_database(db_name=db_name)
        self.collection_client.db_name = db_name
        self.vector_client.db_name = db_name

