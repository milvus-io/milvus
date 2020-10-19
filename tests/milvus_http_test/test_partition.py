import time
import random
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from utils import *
from constants import *

class TestCreateBase:
    """
    ******************************************************************
      The following cases are used to test `create_partition` function 
    ******************************************************************
    """
    def test_create_partition(self, client, collection):
        '''
        target: test create partition, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        client.create_partition(collection, default_tag)

    def test_create_partition_repeat(self, client, collection):
        '''
        target: test create partition, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        client.create_partition(collection, default_tag)
        ret = client.create_partition(collection, default_tag)
        assert(not ret)

    def test_create_partition_collection_not_existed(self, client):
        '''
        target: test create partition, its owner collection name not existed in db, check status returned
        method: call function: create_partition
        expected: status not ok
        '''
        collection_name = gen_unique_str()
        ret = client.create_partition(collection_name, default_tag)
        assert(not ret)

    def test_create_partition_tag_name_None(self, client, collection):
        '''
        target: test create partition, tag name set None, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        tag_name = None
        ret = client.create_partition(collection, tag_name)
        assert(not ret)

    def test_create_different_partition_tags(self, client, collection):
        '''
        target: test create partition twice with different names
        method: call function: create_partition, and again
        expected: status ok
        '''
        client.create_partition(collection, default_tag)
        tag_name = gen_unique_str()
        client.create_partition(collection, tag_name)
        ret = client.list_partitions(collection)
        tag_list = []
        for item in ret:
            tag_list.append(item['partition_tag'])
        assert default_tag in tag_list
        assert tag_name in tag_list
        assert "_default" in tag_list


class TestShowBase:
    """
    ******************************************************************
      The following cases are used to test `list_partitions` function
    ******************************************************************
    """
    def test_list_partitions(self, client, collection):
        '''
        target: test show partitions, check status and partitions returned
        method: create partition first, then call function: list_partitions
        expected: status ok, partition correct
        '''
        client.create_partition(collection, default_tag)
        ret = client.list_partitions(collection)
        tag_list = []
        for item in ret:
            tag_list.append(item['partition_tag'])
        assert default_tag in tag_list

    def test_list_partitions_no_partition(self, client, collection):
        '''
        target: test show partitions with collection name, check status and partitions returned
        method: call function: list_partitions
        expected: status ok, partitions correct
        '''
        res = client.list_partitions(collection)
        assert len(res) == 1


class TestDropBase:
    """
    ******************************************************************
      The following cases are used to test `drop_partition` function
    ******************************************************************
    """
    def test_drop_partition(self, client, collection):
        '''
        target: test drop partition, check status and partition if existed
        method: create partitions first, then call function: drop_partition
        expected: status ok, no partitions in db
        '''
        client.create_partition(collection, default_tag)
        client.drop_partition(collection, default_tag)
        res = client.list_partitions(collection)
        tag_list = []
        for item in res:
            tag_list.append(item['partition_tag'])
        assert default_tag not in tag_list

    def test_drop_partition_tag_not_existed(self, client, collection):
        '''
        target: test drop partition, but tag not existed
        method: create partitions first, then call function: drop_partition
        expected: status not ok
        '''
        client.create_partition(collection, default_tag)
        new_tag = "new_tag"
        ret = client.drop_partition(collection, new_tag)
        assert(not ret)

    def test_drop_partition_tag_not_existed_A(self, client, collection):
        '''
        target: test drop partition, but collection not existed
        method: create partitions first, then call function: drop_partition
        expected: status not ok
        '''
        client.create_partition(collection, default_tag)
        new_collection = gen_unique_str()
        ret = client.drop_partition(new_collection, default_tag)
        assert(not ret)


class TestNameInvalid(object):
    @pytest.fixture(
        scope="function",
        params=[
            "12-s",
            "(mn)",
            "中文",
            "a".join("a" for i in range(256))
        ]
    )
    def get_tag_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=[
            "12-s",
            "(mn)",
            "中文",
            "a".join("a" for i in range(256))
        ]
    )
    def get_collection_name(self, request):
        yield request.param

    def test_drop_partition_with_invalid_collection_name(self, client, collection, get_collection_name):
        '''
        target: test drop partition, with invalid collection name, check status returned
        method: call function: drop_partition
        expected: status not ok
        '''
        collection_name = get_collection_name
        client.create_partition(collection, default_tag)
        ret = client.drop_partition(collection_name, default_tag)
        assert(not ret)

    def test_drop_partition_with_invalid_tag_name(self, client, collection, get_tag_name):
        '''
        target: test drop partition, with invalid tag name, check status returned
        method: call function: drop_partition
        expected: status not ok
        '''
        tag_name = get_tag_name
        client.create_partition(collection, default_tag)
        ret = client.drop_partition(collection, tag_name)
        assert(not ret)

    def test_list_partitions_with_invalid_collection_name(self, client, collection, get_collection_name):
        '''
        target: test show partitions, with invalid collection name, check status returned
        method: call function: list_partitions
        expected: status not ok
        '''
        collection_name = get_collection_name
        client.create_partition(collection, default_tag)
        ret = client.list_partitions(collection_name)
        assert(not ret)
