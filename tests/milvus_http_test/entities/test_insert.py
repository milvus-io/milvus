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
        target: test add vectors with an entity
        method: insert with an entity
        expected: count correct
        '''
        assert client.insert(collection, default_entity)
        res_flush = client.flush([collection])
        count = client.count_collection(collection)
        assert count == 1

    def test_insert_with_entities(self, client, collection):
        '''
        target: test add vectors with entities
        method: insert entities
        expected: count correct
        '''
        assert client.insert(collection, default_entities)
        res_flush = client.flush([collection])
        count = client.count_collection(collection)
        assert count == default_nb

    def test_insert_with_field_not_match(self, client, collection):
        '''
        target: insert field not match with collection schema
        method: pop a field(int64)
        expected: insert failed
        '''
        entity = copy.deepcopy(default_entity)
        entity[0].pop("int64")
        logging.getLogger().info(entity)
        assert not client.insert(collection, entity)

    def test_insert_with_tag_not_existed(self, client, collection):
        '''
        target: test add vectors with an entity
        method: insert an entity with tag, which not created
        expected: insert failed
        '''
        assert not client.insert(collection, default_entity, tag=default_tag)

    def test_insert_with_tag_not_existed(self, client, collection):
        '''
        target: test add vectors with an entity
        method: insert an entity with tag
        expected: count correct
        '''
        client.create_partition(collection, default_tag)
        assert client.insert(collection, default_entity, tag=default_tag)
        res_flush = client.flush([collection])
        count = client.count_collection(collection)
        assert count == 1


class TestInsertID:
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

    def test_insert_with_entity_id_not_matched(self, client, id_collection):
        '''
        target: test add vectors with an entity
        method: insert with an entity
        expected: insert failed
        '''
        assert not client.insert(id_collection, default_entity)

    def test_insert_with_entity(self, client, id_collection):
        '''
        target: test add vectors with an entity, in a id_collection
        method: insert with an entity, with customized ids
        expected: insert success
        '''
        entity = copy.deepcopy(default_entity)
        entity[0].update({"__id": 1})
        logging.getLogger().info(entity)
        assert client.insert(id_collection, entity)
        res_flush = client.flush([id_collection])
        count = client.count_collection(id_collection)
        assert count == 1
