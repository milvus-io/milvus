import pdb
import pytest
import logging
import itertools
from time import sleep
from multiprocessing import Process
from milvus import IndexType, MetricType
from utils import *

uniq_id = "test_load_collection"
index_name = "load_index_name"
default_fields = gen_default_fields() 
entities = gen_entities(6000)


class TestLoadCollection:

    """
    ******************************************************************
      The following cases are used to test `load_collection` function
    ******************************************************************
    """
    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in cpu mode")
        if request.param["index_type"] == IndexType.IVF_PQ:
            pytest.skip("Skip PQ Temporary")
        return request.param

    def test_load_collection_after_index(self, connect, collection, get_simple_index):
        '''
        target: test load collection, after index created
        method: insert and create index, load collection with correct params
        expected: describe raise exception
        ''' 
        connect.insert(collection, entities)
        connect.flush([collection])
        field_name = "fload_vector"
        connect.create_index(collection, field_name, index_name, get_simple_index)
        connect.load_collection(collection)

    def load_empty_collection(self, connect, collection):
        '''
        target: test load collection
        method: no entities in collection, load collection with correct params
        expected: load success
        '''
        connect.load_collection(collection)

    @pytest.mark.level(1)
    def test_load_collection_dis_connect(self, dis_connect, collection):
        '''
        target: test load collection, without connection
        method: load collection with correct params, with a disconnected instance
        expected: load raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.load_collection(collection)

    @pytest.mark.level(2)
    def test_load_collection_not_existed(self, connect, collection):
        collection_name = gen_unique_str()
        with pytest.raises(Exception) as e:
            connect.load_collection(collection_name)


class TestLoadCollectionInvalid(object):
    """
    Test load collection with invalid params
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_collection_names()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_load_collection_with_invalid_collectionname(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.has_collection(collection_name)

    @pytest.mark.level(2)
    def test_load_collection_with_empty_collectionname(self, connect):
        collection_name = ''
        with pytest.raises(Exception) as e:
            connect.has_collection(collection_name)

    @pytest.mark.level(2)
    def test_load_collection_with_none_collectionname(self, connect):
        collection_name = None
        with pytest.raises(Exception) as e:
            connect.has_collection(collection_name)
