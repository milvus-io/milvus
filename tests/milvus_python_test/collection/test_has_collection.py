import pdb
import pytest
import logging
import itertools
import threading
from time import sleep
from multiprocessing import Process
from milvus import IndexType, MetricType
from utils import *

collection_id = "has_collection"
default_fields = gen_default_fields() 


class TestHasCollection:

    """
    ******************************************************************
      The following cases are used to test `has_collection` function
    ******************************************************************
    """
    def test_has_collection(self, connect, collection):
        '''
        target: test if the created collection existed
        method: create collection, assert the value returned by has_collection method
        expected: True
        '''
        assert connect.has_collection(collection)

    @pytest.mark.level(2)
    def test_has_collection_without_connection(self, collection, dis_connect):
        '''
        target: test has collection, without connection
        method: calling has collection with correct params, with a disconnected instance
        expected: has collection raise exception
        '''
        with pytest.raises(Exception) as e:
            assert connect.has_collection(collection)

    def test_has_collection_not_existed(self, connect):
        '''
        target: test if collection not created
        method: random a collection name, which not existed in db, 
            assert the value returned by has_collection method
        expected: False
        '''
        collection_name = gen_unique_str("test_collection")
        assert not connect.has_collection(collection_name)

    @pytest.mark.level(2)
    def test_has_collection_multithread(self, connect):
        '''
        target: test create collection with multithread
        method: create collection using multithread, 
        expected: collections are created
        '''
        threads_num = 4 
        threads = []
        collection_name = gen_unique_str(collection_id)
        connect.create_collection(collection_name, default_fields)

        def has():
            assert not assert_collection(connect, collection_name)
        for i in range(threads_num):
            t = threading.Thread(target=has, args=())
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()


class TestHasCollectionInvalid(object):
    """
    Test has collection with invalid params
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_has_collection_with_invalid_collectionname(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.has_collection(collection_name)

    @pytest.mark.level(2)
    def test_has_collection_with_empty_collectionname(self, connect):
        collection_name = ''
        with pytest.raises(Exception) as e:
            connect.has_collection(collection_name)

    @pytest.mark.level(2)
    def test_has_collection_with_none_collectionname(self, connect):
        collection_name = None
        with pytest.raises(Exception) as e:
            connect.has_collection(collection_name)
