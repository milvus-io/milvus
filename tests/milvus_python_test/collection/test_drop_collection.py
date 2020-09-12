import pdb
import pytest
import logging
import itertools
from time import sleep
from multiprocessing import Process
from utils import *

uniq_id = "drop_collection"
default_fields = gen_default_fields() 


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
