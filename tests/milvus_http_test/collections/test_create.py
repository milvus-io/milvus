import pdb
import copy
import logging
import itertools
from time import sleep
import threading
from multiprocessing import Process
import sklearn.preprocessing

import pytest
from utils import *
from constants import *

uid = "create_collection"

class TestCreateCollection:
    """
    ******************************************************************
      The following cases are used to test `create_collection` function
    ******************************************************************
    """
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

    @pytest.fixture(
        scope="function",
        params=gen_segment_row_limits()
    )
    def get_segment_row_limit(self, request):
        yield request.param
        
    def test_create_collection_segment_row_limit(self, client, get_segment_row_limit):
        '''
        target: test create normal collection with different fields
        method: create collection with diff segment_row_limit
        expected: no exception raised
        '''
        collection_name = gen_unique_str(uid)
        fields = copy.deepcopy(default_fields)
        fields["segment_row_limit"] = get_segment_row_limit
        assert client.create_collection(collection_name, fields)
        assert client.has_collection(collection_name)

    def test_create_collection_exceed_segment_row_limit(self, client):
        '''
        target: test create normal collection with different fields
        method: create collection with diff segment_row_limit
        expected: no exception raised
        '''
        segment_row_limit = 10000000
        collection_name = gen_unique_str(uid)
        fields = copy.deepcopy(default_fields)
        fields["segment_row_limit"] = segment_row_limit
        client.create_collection(collection_name, fields)
        assert not client.has_collection(collection_name)