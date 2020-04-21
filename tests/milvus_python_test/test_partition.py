import time
import random
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from milvus import IndexType, MetricType
from utils import *


dim = 128
index_file_size = 10
collection_id = "test_partition"
ADD_TIMEOUT = 60
nprobe = 1
tag = "1970-01-01"


class TestCreateBase:

    """
    ******************************************************************
      The following cases are used to test `create_partition` function 
    ******************************************************************
    """
    def test_create_partition(self, connect, collection):
        '''
        target: test create partition, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        status = connect.create_partition(collection, tag)
        assert status.OK()

    def _test_create_partition_limit(self, connect, collection):
        '''
        target: test create partitions, check status returned
        method: call function: create_partition for 4097 times
        expected: status not ok
        '''
        for i in range(4096):
            tag_tmp = gen_unique_str()
            status = connect.create_partition(collection, tag_tmp)
            assert status.OK()
        status = connect.create_partition(collection, tag)
        assert not status.OK()

    def test_create_partition_repeat(self, connect, collection):
        '''
        target: test create partition, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        status = connect.create_partition(collection, tag)
        assert status.OK()
        status = connect.create_partition(collection, tag)
        assert not status.OK()

    def test_create_partition_collection_not_existed(self, connect):
        '''
        target: test create partition, its owner collection name not existed in db, check status returned
        method: call function: create_partition
        expected: status not ok
        '''
        collection_name = gen_unique_str()
        status = connect.create_partition(collection_name, tag)
        assert not status.OK()

    def test_create_partition_tag_name_None(self, connect, collection):
        '''
        target: test create partition, tag name set None, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        tag_name = None
        status = connect.create_partition(collection, tag_name)
        assert not status.OK()

    def test_create_different_partition_tags(self, connect, collection):
        '''
        target: test create partition twice with different names
        method: call function: create_partition, and again
        expected: status ok
        '''
        status = connect.create_partition(collection, tag)
        assert status.OK()
        tag_name = gen_unique_str()
        status = connect.create_partition(collection, tag_name)
        assert status.OK()
        status, res = connect.show_partitions(collection)
        assert status.OK()
        tag_list = []
        for item in res:
            tag_list.append(item.tag)
        assert tag in tag_list
        assert tag_name in tag_list
        assert "_default" in tag_list

    def test_create_partition_add_vectors_default(self, connect, collection):
        '''
        target: test create partition, and insert vectors, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        status = connect.create_partition(collection, tag)
        assert status.OK()
        nq = 100
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.insert(collection, vectors, ids)
        assert status.OK()

    def test_create_partition_insert_with_tag(self, connect, collection):
        '''
        target: test create partition, and insert vectors, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        status = connect.create_partition(collection, tag)
        assert status.OK()
        nq = 100
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.insert(collection, vectors, ids, partition_tag=tag)
        assert status.OK()

    def test_create_partition_insert_with_tag_not_existed(self, connect, collection):
        '''
        target: test create partition, and insert vectors, check status returned
        method: call function: create_partition
        expected: status not ok
        '''
        tag_new = "tag_new"
        status = connect.create_partition(collection, tag)
        assert status.OK()
        nq = 100
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.insert(collection, vectors, ids, partition_tag=tag_new)
        assert not status.OK()

    def test_create_partition_insert_same_tags(self, connect, collection):
        '''
        target: test create partition, and insert vectors, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        status = connect.create_partition(collection, tag)
        assert status.OK()
        nq = 100
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.insert(collection, vectors, ids, partition_tag=tag)
        ids = [(i+100) for i in range(nq)]
        status, ids = connect.insert(collection, vectors, ids, partition_tag=tag)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, res = connect.count_collection(collection)
        assert res == nq * 2

    def test_create_partition_insert_same_tags_two_collections(self, connect, collection):
        '''
        target: test create two partitions, and insert vectors with the same tag to each collection, check status returned
        method: call function: create_partition
        expected: status ok, collection length is correct
        '''
        status = connect.create_partition(collection, tag)
        assert status.OK()
        collection_new = gen_unique_str()
        param = {'collection_name': collection_new,
            'dimension': dim,
            'index_file_size': index_file_size,
            'metric_type': MetricType.L2}
        status = connect.create_collection(param)
        status = connect.create_partition(collection_new, tag)
        nq = 100
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.insert(collection, vectors, ids, partition_tag=tag)
        ids = [(i+100) for i in range(nq)]
        status, ids = connect.insert(collection_new, vectors, ids, partition_tag=tag)
        status = connect.flush([collection, collection_new])
        assert status.OK()
        status, res = connect.count_collection(collection)
        assert res == nq
        status, res = connect.count_collection(collection_new)
        assert res == nq


class TestShowBase:

    """
    ******************************************************************
      The following cases are used to test `show_partitions` function 
    ******************************************************************
    """
    def test_show_partitions(self, connect, collection):
        '''
        target: test show partitions, check status and partitions returned
        method: create partition first, then call function: show_partitions
        expected: status ok, partition correct
        '''
        status = connect.create_partition(collection, tag)
        status, res = connect.show_partitions(collection)
        assert status.OK()

    def test_show_partitions_no_partition(self, connect, collection):
        '''
        target: test show partitions with collection name, check status and partitions returned
        method: call function: show_partitions
        expected: status ok, partitions correct
        '''
        status, res = connect.show_partitions(collection)
        assert status.OK()

    def test_show_multi_partitions(self, connect, collection):
        '''
        target: test show partitions, check status and partitions returned
        method: create partitions first, then call function: show_partitions
        expected: status ok, partitions correct
        '''
        tag_new = gen_unique_str()
        status = connect.create_partition(collection, tag)
        status = connect.create_partition(collection, tag_new)
        status, res = connect.show_partitions(collection)
        assert status.OK()


class TestDropBase:

    """
    ******************************************************************
      The following cases are used to test `drop_partition` function 
    ******************************************************************
    """
    def test_drop_partition(self, connect, collection):
        '''
        target: test drop partition, check status and partition if existed
        method: create partitions first, then call function: drop_partition
        expected: status ok, no partitions in db
        '''
        status = connect.create_partition(collection, tag)
        status = connect.drop_partition(collection, tag)
        assert status.OK()
        status, res = connect.show_partitions(collection)
        tag_list = []
        for item in res:
            tag_list.append(item.tag)
        assert tag not in tag_list

    def test_drop_partition_tag_not_existed(self, connect, collection):
        '''
        target: test drop partition, but tag not existed
        method: create partitions first, then call function: drop_partition
        expected: status not ok
        '''
        status = connect.create_partition(collection, tag)
        new_tag = "new_tag"
        status = connect.drop_partition(collection, new_tag)
        assert not status.OK()

    def test_drop_partition_tag_not_existed_A(self, connect, collection):
        '''
        target: test drop partition, but collection not existed
        method: create partitions first, then call function: drop_partition
        expected: status not ok
        '''
        status = connect.create_partition(collection, tag)
        new_collection = gen_unique_str()
        status = connect.drop_partition(new_collection, tag)
        assert not status.OK()

    @pytest.mark.level(2)
    def test_drop_partition_repeatedly(self, connect, collection):
        '''
        target: test drop partition twice, check status and partition if existed
        method: create partitions first, then call function: drop_partition
        expected: status not ok, no partitions in db
        '''
        status = connect.create_partition(collection, tag)
        status = connect.drop_partition(collection, tag)
        status = connect.drop_partition(collection, tag)
        time.sleep(2)
        assert not status.OK()
        status, res = connect.show_partitions(collection)
        tag_list = []
        for item in res:
            tag_list.append(item.tag)
        assert tag not in tag_list

    def test_drop_partition_create(self, connect, collection):
        '''
        target: test drop partition, and create again, check status
        method: create partitions first, then call function: drop_partition, create_partition
        expected: status not ok, partition in db
        '''
        status = connect.create_partition(collection, tag)
        status = connect.drop_partition(collection, tag)
        time.sleep(2)
        status = connect.create_partition(collection, tag)
        assert status.OK()
        status, res = connect.show_partitions(collection)
        tag_list = []
        for item in res:
            tag_list.append(item.tag)
        assert tag in tag_list


class TestNameInvalid(object):
    @pytest.fixture(
        scope="function",
        params=gen_invalid_collection_names()
    )
    def get_tag_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_collection_names()
    )
    def get_collection_name(self, request):
        yield request.param

    def test_drop_partition_with_invalid_collection_name(self, connect, collection, get_collection_name):
        '''
        target: test drop partition, with invalid collection name, check status returned
        method: call function: drop_partition
        expected: status not ok
        '''
        collection_name = get_collection_name
        status = connect.create_partition(collection, tag)
        status = connect.drop_partition(collection_name, tag)
        assert not status.OK()

    def test_drop_partition_with_invalid_tag_name(self, connect, collection, get_tag_name):
        '''
        target: test drop partition, with invalid tag name, check status returned
        method: call function: drop_partition
        expected: status not ok
        '''
        tag_name = get_tag_name
        status = connect.create_partition(collection, tag)
        status = connect.drop_partition(collection, tag_name)
        assert not status.OK()

    def test_show_partitions_with_invalid_collection_name(self, connect, collection, get_collection_name):
        '''
        target: test show partitions, with invalid collection name, check status returned
        method: call function: show_partitions
        expected: status not ok
        '''
        collection_name = get_collection_name
        status = connect.create_partition(collection, tag)
        status, res = connect.show_partitions(collection_name)
        assert not status.OK()
