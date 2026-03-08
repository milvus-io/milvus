import random
from sklearn import preprocessing
import numpy as np
from utils.utils import gen_collection_name
from utils.util_log import test_log as logger
import pytest
from base.testbase import TestBase


@pytest.mark.L0
class TestAliasE2E(TestBase):

    def test_alias_e2e(self):
        """
        """
        # list alias before create
        rsp = self.alias_client.list_alias()
        name = gen_collection_name()
        client = self.collection_client
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "book_intro", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{128}"}}
                ]
            },
            "indexParams": [{"fieldName": "book_intro", "indexName": "book_intro_vector", "metricType": "L2"}]
        }
        logger.info(f"create collection {name} with payload: {payload}")
        rsp = client.collection_create(payload)
        # create alias
        alias_name = name + "_alias"
        payload = {
            "collectionName": name,
            "aliasName": alias_name
        }
        rsp = self.alias_client.create_alias(payload)
        assert rsp['code'] == 0
        # list alias after create
        rsp = self.alias_client.list_alias()
        assert alias_name in rsp['data']
        # describe alias
        rsp = self.alias_client.describe_alias(alias_name)
        assert rsp['data']["aliasName"] == alias_name
        assert rsp['data']["collectionName"] == name

        # do crud operation by alias
        # insert data by alias
        data = []
        for j in range(3000):
            tmp = {
                "book_id": j,
                "word_count": j,
                "book_describe": f"book_{j}",
                "book_intro": preprocessing.normalize([np.array([random.random() for _ in range(128)])])[0].tolist(),
            }
            data.append(tmp)

        payload = {
            "collectionName": alias_name,
            "data": data
        }
        rsp = self.vector_client.vector_insert(payload)
        # delete data by alias
        payload = {
            "collectionName": alias_name,
            "ids": [1, 2, 3]
        }
        rsp = self.vector_client.vector_delete(payload)

        # upsert data by alias
        upsert_data = []
        for j in range(100):
            tmp = {
                "book_id": j,
                "word_count": j + 1,
                "book_describe": f"book_{j + 2}",
                "book_intro": preprocessing.normalize([np.array([random.random() for _ in range(128)])])[0].tolist(),
            }
            upsert_data.append(tmp)
        payload = {
            "collectionName": alias_name,
            "data": upsert_data
        }
        rsp = self.vector_client.vector_upsert(payload)
        # search data by alias
        payload = {
            "collectionName": alias_name,
            "vector": preprocessing.normalize([np.array([random.random() for i in range(128)])])[0].tolist()
        }
        rsp = self.vector_client.vector_search(payload)
        # query data by alias
        payload = {
            "collectionName": alias_name,
            "filter": "book_id > 10"
        }
        rsp = self.vector_client.vector_query(payload)

        # alter alias to another collection
        new_name = gen_collection_name()
        payload = {
            "collectionName": new_name,
            "metricType": "L2",
            "dimension": 128,
        }
        rsp = client.collection_create(payload)
        payload = {
            "collectionName": new_name,
            "aliasName": alias_name
        }
        rsp = self.alias_client.alter_alias(payload)
        # describe alias
        rsp = self.alias_client.describe_alias(alias_name)
        assert rsp['data']["aliasName"] == alias_name
        assert rsp['data']["collectionName"] == new_name
        # query data by alias, expect no data
        payload = {
            "collectionName": alias_name,
            "filter": "id > 0"
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['data'] == []
