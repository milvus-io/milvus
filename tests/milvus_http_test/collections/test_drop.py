import pytest
import time
from utils import *
from constants import *


class TestDropCollection:
    """
    ******************************************************************
      The following cases are used to test `drop_collection` function
    ******************************************************************
    """
    def test_drop_collection(self, client, collection):
        '''
        target: test delete collection created with correct params
        method: create collection and then delete,
            assert the value returned by delete method
        expected: status ok, and no collection in collections
        '''
        client.drop_collection(collection)
        time.sleep(2)
        assert not client.has_collection(collection)

    def test_drop_collection_not_existed(self, client):
        '''
        target: test if collection not created
        method: random a collection name, which not existed in db, assert the exception raised returned by drp_collection method
        expected: raise exception
        '''
        collection_name = gen_unique_str()
        assert not client.drop_collection(collection_name)

    @pytest.fixture(
        scope="function",
        params=[
        1,
        "12-s",
        " ",
        "12 s",
        " siede ",
        "(mn)",
        "中文",
        "a".join("a" for i in range(256))
    ]
    )
    def get_invalid_collection_name(self, request):
        yield request.param

    def test_drop_collection_with_invalid_collection(self, client, get_invalid_collection_name):
        '''
        target: test if collection not created
        method: random a collection name, which not existed in db, assert the exception raised returned by drp_collection method
        expected: raise exception
        '''
        assert not client.drop_collection(get_invalid_collection_name)