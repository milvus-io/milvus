import random
from sklearn import preprocessing
import numpy as np
from utils.utils import gen_collection_name
import pytest
from base.testbase import TestBase
from pymilvus import (
    Collection
)


@pytest.mark.L0
class TestPartitionE2E(TestBase):

    def test_partition_e2e(self):
        """
        target: test create collection
        method: create a collection with a simple schema
        expected: create collection success
        """
        name = gen_collection_name()
        dim = 128
        metric_type = "L2"
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
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        rsp = client.collection_list()
        all_collections = rsp['data']
        assert name in all_collections
        # describe collection
        rsp = client.collection_describe(name)
        assert rsp['code'] == 0
        assert rsp['data']['collectionName'] == name
        # insert data to default partition
        data = []
        for j in range(3000):
            tmp = {
                "book_id": j,
                "word_count": j,
                "book_describe": f"book_{j}",
                "book_intro": preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
            }
            data.append(tmp)
        payload = {
            "collectionName": name,
            "data": data,
        }
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0
        # create partition
        partition_name = "test_partition"
        rsp = self.partition_client.partition_create(collection_name=name, partition_name=partition_name)
        assert rsp['code'] == 0
        # insert data to partition
        data = []
        for j in range(3000, 6000):
            tmp = {
                "book_id": j,
                "word_count": j,
                "book_describe": f"book_{j}",
                "book_intro": preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
            }
            data.append(tmp)
        payload = {
            "collectionName": name,
            "partitionName": partition_name,
            "data": data,
        }
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0

        # create partition again
        rsp = self.partition_client.partition_create(collection_name=name, partition_name=partition_name)
        # list partitions
        rsp = self.partition_client.partition_list(collection_name=name)
        assert rsp['code'] == 0
        assert partition_name in rsp['data']
        # has partition
        rsp = self.partition_client.partition_has(collection_name=name, partition_name=partition_name)
        assert rsp['code'] == 0
        assert rsp['data']["has"] is True
        # flush and get partition statistics
        c = Collection(name=name)
        c.flush()
        rsp = self.partition_client.partition_stats(collection_name=name, partition_name=partition_name)
        assert rsp['code'] == 0
        assert rsp['data']['rowCount'] == 3000

        # release partition
        rsp = self.partition_client.partition_release(collection_name=name, partition_names=[partition_name])
        assert rsp['code'] == 0
        # release partition again
        rsp = self.partition_client.partition_release(collection_name=name, partition_names=[partition_name])
        assert rsp['code'] == 0
        # load partition
        rsp = self.partition_client.partition_load(collection_name=name, partition_names=[partition_name])
        assert rsp['code'] == 0
        # load partition again
        rsp = self.partition_client.partition_load(collection_name=name, partition_names=[partition_name])
        assert rsp['code'] == 0
        # drop partition when it is loaded
        rsp = self.partition_client.partition_drop(collection_name=name, partition_name=partition_name)
        assert rsp['code'] == 65535
        # drop partition after release
        rsp = self.partition_client.partition_release(collection_name=name, partition_names=[partition_name])
        rsp = self.partition_client.partition_drop(collection_name=name, partition_name=partition_name)
        assert rsp['code'] == 0
        # has partition
        rsp = self.partition_client.partition_has(collection_name=name, partition_name=partition_name)
        assert rsp['code'] == 0
        assert rsp['data']["has"] is False
