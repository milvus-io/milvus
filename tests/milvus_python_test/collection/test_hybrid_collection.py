import pdb
import pytest
import logging
import itertools
from time import sleep
from multiprocessing import Process
from milvus import IndexType, MetricType
from milvus.client.types import DataType
from utils import *

dim = 128
drop_collection_interval_time = 3
index_file_size = 10
vectors = gen_vectors(100, dim)

class TestHybridCollection:
    """
    ******************************************************************
      The following cases are used to test `create_hybrid_collection` function
    ******************************************************************
    """

    def test_create_hybrid_collection(self, connect):
        '''
        target: test create normal collection
        method: create collection with corrent params
        expected: create status return ok
        '''
        collection_name = gen_unique_str("test_collection")
        collection_fields = [
            {"field_name": "A", "data_type": DataType.INT64},
            {"field_name": "B", "data_type": DataType.INT64},
            {"field_name": "C", "data_type": DataType.INT64},
            {"field_name": "Vec", "dimension": 128,
             "extra_params": {"index_file_size": 100, "metric_type": MetricType.L2}}
        ]
        status = connect.create_hybrid_collection(collection_name, collection_fields)
        print(status)

    def test_create_hybrid_collection_ip(self, connect):
        '''
        target: test create normal collection
        method: create collection with corrent params
        expected: create status return ok
        '''
        collection_name = gen_unique_str("test_collection")
        collection_fields = [
            {"field_name": "A", "data_type": DataType.INT64},
            {"field_name": "B", "data_type": DataType.INT64},
            {"field_name": "C", "data_type": DataType.INT64},
            {"field_name": "Vec", "dimension": 128,
             "extra_params": {"index_file_size": 100, "metric_type": MetricType.IP}}
        ]
        status = connect.create_hybrid_collection(collection_name, collection_fields)
        print(status)
        assert status.OK()

    def test_create_hybrid_collection_jaccard(self, connect):
        '''
        target: test create normal collection
        method: create collection with corrent params
        expected: create status return ok
        '''
        collection_name = gen_unique_str("test_collection")
        collection_fields = [
            {"field_name": "A", "data_type": DataType.INT64},
            {"field_name": "B", "data_type": DataType.INT64},
            {"field_name": "C", "data_type": DataType.INT64},
            {"field_name": "Vec", "dimension": 128,
             "extra_params": {"index_file_size": 100, "metric_type": MetricType.JACCARD}}
        ]
        status = connect.create_hybrid_collection(collection_name, collection_fields)
        print(status)
        assert status.OK()

    def test_create_hybrid_collection_existed(self, connect):
        '''
        target: test create collection but the collection name have already existed
        method: create collection with the same collection_name
        expected: create status return not ok
        '''
        collection_name = gen_unique_str("test_collection")
        collection_fields = [
            {"field_name": "A", "data_type": DataType.INT32},
            {"field_name": "B", "data_type": DataType.INT64},
            {"field_name": "C", "data_type": DataType.FLOAT},
            {"field_name": "Vec", "dimension": 128,
             "extra_params": {"index_file_size": 100, "metric_type": MetricType.L2}}
        ]
        status = connect.create_hybrid_collection(collection_name, collection_fields)
        status = connect.create_hybrid_collection(collection_name, collection_fields)
        assert not status.OK()
