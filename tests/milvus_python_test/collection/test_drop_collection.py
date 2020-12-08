import pdb
import pytest
import logging
import itertools
from time import sleep
import threading
from multiprocessing import Process
from utils import *
from constants import *

uniq_id = "drop_collection"

class TestDropCollection:
    """
    ******************************************************************
      The following cases are used to test `drop_collection` function
    ******************************************************************
    """
    def test_drop_collection(self, connect, collection):
        '''
        target: test delete collection created with correct params 
        method: create collection and then delete, 
            assert the value returned by delete method
        expected: status ok, and no collection in collections
        '''
        connect.drop_collection(collection)
        time.sleep(2)
        assert not connect.has_collection(collection)

    def test_drop_collection_without_connection(self, collection, dis_connect):
        '''
        target: test describe collection, without connection
        method: drop collection with correct params, with a disconnected instance
        expected: drop raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.drop_collection(collection)

    def test_drop_collection_not_existed(self, connect):
        '''
        target: test if collection not created
        method: random a collection name, which not existed in db, 
            assert the exception raised returned by drp_collection method
        expected: False
        '''
        collection_name = gen_unique_str(uniq_id)
        with pytest.raises(Exception) as e:
            connect.drop_collection(collection_name)

    @pytest.mark.level(2)
    def test_create_drop_collection_multithread(self, connect):
        '''
        target: test create and drop collection with multithread
        method: create and drop collection using multithread, 
        expected: collections are created, and dropped
        '''
        threads_num = 8 
        threads = []
        collection_names = []

        def create():
            collection_name = gen_unique_str(uniq_id)
            collection_names.append(collection_name)
            connect.create_collection(collection_name, default_fields)
            connect.drop_collection(collection_name)
        for i in range(threads_num):
            t = TestThread(target=create, args=())
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()
        
        for item in collection_names:
            assert not connect.has_collection(item)


class TestDropCollectionInvalid(object):
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
    def test_drop_collection_with_invalid_collectionname(self, connect, get_collection_name):
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            connect.has_collection(collection_name)

    def test_drop_collection_with_empty_collectionname(self, connect):
        collection_name = ''
        with pytest.raises(Exception) as e:
            connect.has_collection(collection_name)

    def test_drop_collection_with_none_collectionname(self, connect):
        collection_name = None
        with pytest.raises(Exception) as e:
            connect.has_collection(collection_name)
