import logging
import time
import pdb
import copy
import threading
from multiprocessing import Pool, Process
import pytest
from utils import *
from constants import *

BUILD_TIMEOUT = 120
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
        ids = client.insert(collection, default_entities)
        res = client.create_index(collection, default_float_vec_field_name, default_index)
        # get index info
        logging.getLogger().info(res)
        res_info_index = client.describe_index(collection, default_float_vec_field_name)
        assert res_info_index == default_index

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
        assert not client.create_index(collection, tmp_field_name, default_index)

    def test_create_index_collection_not_existed(self, client):
        '''
        target: test create index interface when collection name not existed
        method: create collection and add entities in it, create index
            , make sure the collection name not in index
        expected: create index failed
        '''
        collection_name = gen_unique_str(uid)
        assert not client.create_index(collection_name, default_float_vec_field_name, default_index)

    def test_drop_index(self, client, collection):
        '''
        target: test drop index interface
        method: create collection and add entities in it, create index, call drop index
        expected: return code 0, and default index param
        '''
        # ids = connect.insert(collection, entities)
        client.create_index(collection, default_float_vec_field_name, default_index)
        client.drop_index(collection, default_float_vec_field_name)
        res_info_index = client.describe_index(collection, default_float_vec_field_name)
        assert not res_info_index

    def test_drop_index_collection_not_existed(self, client):
        '''
        target: test drop index interface when collection name not existed
        method: create collection and add entities in it, create index
            , make sure the collection name not in index, and then drop it
        expected: return code not equals to 0, drop index failed
        '''
        collection_name = gen_unique_str(uid)
        assert not client.drop_index(collection_name, default_float_vec_field_name)
