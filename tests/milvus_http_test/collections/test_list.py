import logging

import pytest
import time
import copy
from utils import *
from constants import *

uid = "list_collection"


class TestListCollections:
    """
    ******************************************************************
      The following cases are used to test `list_collections` function
    ******************************************************************
    """

    def test_list_collections(self, client, collection):
        '''
        target: test list collections
        method: create collection, assert the value returned by list_collections method
        expected: True
        '''
        collections = map(lambda x: x['collection_name'], client.list_collections())
        assert collection in collections

    def test_list_collections_not_existed(self, client):
        '''
        target: test if collection not created
        method: random a collection name, which not existed in db, assert the value returned by list_collections method
        expected: False
        '''
        collection_name = gen_unique_str(uid)
        collections = map(lambda x: x['collection_name'], client.list_collections())
        assert collection_name not in collections

    def test_list_collections_no_collection(self, client):
        '''
        target: test list collections when no collection in db
        method: delete all collections and list collections
        expected: status is ok and len of result is 0
        '''
        client.clear_db()
        result = client.list_collections()
        assert len(result) == 0

    def test_list_collections_multi_collections(self, client, collection):
        '''
        target: test list collections with multi collections
        method: create multi collections and list them
        expected: len of list results is equal collection nums
        '''
        collection_name = gen_unique_str(uid)
        fields = copy.deepcopy(default_fields)
        assert client.create_collection(collection_name, fields)
        collections = map(lambda x: x['collection_name'], client.list_collections())
        assert collection_name in collections
        assert collection in collections

    def test_list_collections_offset(self, client, collection):
        '''
        target: test list collections with offset parameter
        method: create multi collections and list them with offset
        expected: first collection with offset=1 equal to second collection with offset=0
        '''
        collection_num = 2
        fields = copy.deepcopy(default_fields)
        for i in range(collection_num):
            collection_name = gen_unique_str(uid)
            assert client.create_collection(collection_name, fields)
        collections = list(map(lambda x: x['collection_name'], client.list_collections()))
        collections_new = list(map(lambda x: x['collection_name'], client.list_collections(offset=1)))
        assert collections[1] == collections_new[0]

    def test_list_collections_page_size(self, client, collection):
        '''
        target: test list collections with page_size parameter
        method: create multi collections and list them with page_size
        expected: collection num equal to page_size
        '''
        collection_num = 6
        page_size = 5
        fields = copy.deepcopy(default_fields)
        for i in range(collection_num):
            collection_name = gen_unique_str(uid)
            assert client.create_collection(collection_name, fields)
        collections = list(map(lambda x: x['collection_name'], client.list_collections(page_size=page_size)))
        c = list(map(lambda x: x['collection_name'], client.list_collections()))
        assert len(collections) == page_size

