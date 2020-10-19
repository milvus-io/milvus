import logging
import time
import pdb
import copy
import threading
from multiprocessing import Pool, Process
import pytest
from utils import *
from constants import *

GET_TIMEOUT = 120
uid = "test_get"


class TestGetBase:
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

    def test_get_entity_id_not_exised(self, client, collection):
        '''
        target: test delete entity, params entity_id not existed
        method: add entity and delete
        expected: entities empty
        '''
        ids = client.insert(collection, default_entity)
        client.flush([collection])
        entities = client.get_entities(collection, [0,1])
        assert entities

    def test_get_empty_collection(self, client, collection):
        '''
        target: test hry entity, params collection_name not existed
        method: add entity and get
        expected: entities empty
        '''
        entities = client.get_entities(collection, [0])
        assert entities

    def test_get_entity_collection_not_existed(self, client, collection):
        '''
        target: test get entity, params collection_name not existed
        method: add entity and get
        expected: code error
        '''
        collection_new = gen_unique_str()
        entities = client.get_entities(collection_new, [0])
        assert not entities

    def test_insert_get(self, client, collection):
        '''
        target: test get entity
        method: add entities and get
        expected: entity returned
        '''
        ids = client.insert(collection, default_entities)
        client.flush([collection])
        delete_ids = [ids[0]]
        entities = client.get_entities(collection, delete_ids)
        assert len(entities) == 1

    def test_insert_get_batch(self, client, collection):
        '''
        target: test get entity
        method: add entities and get
        expected: entity returned
        '''
        get_length = 10
        ids = client.insert(collection, default_entities)
        client.flush([collection])
        delete_ids = ids[:get_length]
        entities = client.get_entities(collection, delete_ids)
        assert len(entities) == get_length