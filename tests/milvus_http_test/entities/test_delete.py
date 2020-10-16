import logging
import time
import pdb
import copy
import threading
from multiprocessing import Pool, Process
import pytest
from utils import *
from constants import *

DELETE_TIMEOUT = 60
uid = "test_delete"


class TestDeleteBase:
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

    def test_delete_entity_id_not_exised(self, client, collection):
        '''
        target: test get entity, params entity_id not existed
        method: get entity 
        expected: result empty
        '''
        assert client.insert(collection, default_entity)
        res_flush = client.flush([collection])
        entities = client.get_entities(collection, 1)
        assert entities

    def test_delete_empty_collection(self, client, collection):
        '''
        target: test delete entity, params collection_name not existed
        method: add entity and delete
        expected: status DELETED
        '''
        status = client.delete(collection, ["0"])
        assert status

    def test_delete_entity_collection_not_existed(self, client, collection):
        '''
        target: test delete entity, params collection_name not existed
        method: add entity and delete
        expected: error raised
        '''
        collection_new = gen_unique_str()
        status = client.delete(collection_new, ["0"])
        assert not status

    def test_insert_delete(self, client, collection):
        '''
        target: test delete entity
        method: add entities and delete
        expected: no error raised
        '''
        ids = client.insert(collection, default_entities)
        client.flush([collection])
        delete_ids = [ids[0]]
        status = client.delete(collection, delete_ids)
        assert status
        client.flush([collection])
        res_count = client.count_collection(collection)
        assert res_count == default_nb - 1