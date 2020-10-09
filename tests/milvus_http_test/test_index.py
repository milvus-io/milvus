import logging
import time
import pdb
import copy
import threading
from multiprocessing import Pool, Process
import pytest
from utils import *
from constants import *

INDEX_TIMEOUT = 120
uid = "test_index"
default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}

class TestIndexBase:
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

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, client, collection):
        '''
        target: test create index interface
        method: create collection and add entities in it, create index
        expected: return search success
        '''
        ids = connect.insert(collection, default_entities)
        assert client.create_index(collection, default_float_vec_field_name, default_index)
        # get index info
        

    def test_create_index_on_field_not_existed(self, client, collection):
        '''
        target: test create index interface
        method: create collection and add entities in it, create index on field not existed
        expected: create failed
        '''
        tmp_field_name = gen_unique_str()
        ids = client.insert(collection, default_entities)
        assert not client.create_index(collection, tmp_field_name, default_index)

    @pytest.mark.level(2)
    def test_create_index_on_field(self, client, collection):
        '''
        target: test create index interface
        method: create collection and add entities in it, create index on other field
        expected: create failed
        '''
        tmp_field_name = "int64"
        ids = client.insert(collection, default_entities)
        assert not create_index(collection, tmp_field_name, default_index)

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition(self, client, collection):
        '''
        target: test create index interface
        method: create collection, create partition, and add entities in it, create index
        expected: return search success
        '''
        client.create_partition(collection, default_tag)
        ids = client.insert(collection, default_entities, partition_tag=default_tag)
        client.flush([collection])
        client.create_index(collection, field_name, default_index)