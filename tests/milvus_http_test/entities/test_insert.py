import logging
import time
import pdb
import copy
import threading
from multiprocessing import Pool, Process
import pytest
from utils import *
from constants import *

ADD_TIMEOUT = 60
uid = "test_insert"
field_name = default_float_vec_field_name
binary_field_name = default_binary_vec_field_name
default_single_query = {
    "bool": {
        "must": [
            {"vector": {field_name: {"topk": 10, "query": gen_vectors(1, default_dim), "metric_type": "L2",
                                     "params": {"nprobe": 10}}}}
        ]
    }
}


class TestInsertBase:
    """
    ******************************************************************
      The following cases are used to test `insert` function
    ******************************************************************
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, client):
        if str(client.system_cmd("mode")) == "CPU":
            if request.param["index_type"] in index_cpu_not_support():
                pytest.skip("CPU not support index_type: ivf_sq8h")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_single_filter_fields()
    )
    def get_filter_field(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_single_vector_fields()
    )
    def get_vector_field(self, request):
        yield request.param

    def test_insert_with_empty_entity(self, client, collection):
        '''
        target: test add vectors with empty vectors list
        method: set empty vectors list as add method params
        expected: raises a Exception
        '''
        assert not client.insert(collection, [])

    def test_insert_with_entity(self, client, collection):
        '''
        target: test add vectors with empty vectors list
        method: set empty vectors list as add method params
        expected: raises a Exception
        '''
        client.insert(collection, default_entity)
        client.flush()