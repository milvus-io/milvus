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
table_id = "test_add"
ADD_TIMEOUT = 60
nprobe = 1
epsilon = 0.0001
tag = "1970-01-01"


class TestCreateBase:

    """
    ******************************************************************
      The following cases are used to test `create_partition` function 
    ******************************************************************
    """
    def test_create_partition(self, connect, table):
        '''
        target: test create partition, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        assert status.OK()

    def test_create_partition_repeat(self, connect, table):
        '''
        target: test create partition, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        status = connect.create_partition(table, partition_name, tag)
        assert not status.OK()

    def test_create_partition_recursively(self, connect, table):
        '''
        target: test create partition, and create partition in parent partition, check status returned
        method: call function: create_partition
        expected: status not ok
        '''
        partition_name = gen_unique_str()
        new_partition_name = gen_unique_str()
        new_tag = "new_tag"
        status = connect.create_partition(table, partition_name, tag)
        status = connect.create_partition(partition_name, new_partition_name, new_tag)
        assert not status.OK()

    def test_create_partition_table_not_existed(self, connect):
        '''
        target: test create partition, its owner table name not existed in db, check status returned
        method: call function: create_partition
        expected: status not ok
        '''
        table_name = gen_unique_str()
        partition_name = gen_unique_str()
        status = connect.create_partition(table_name, partition_name, tag)
        assert not status.OK()

    def test_create_partition_partition_name_existed(self, connect, table):
        '''
        target: test create partition, and create the same partition again, check status returned
        method: call function: create_partition
        expected: status not ok
        '''
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        assert status.OK()
        tag_new = "tag_new"
        status = connect.create_partition(table, partition_name, tag_new)
        assert not status.OK()

    def test_create_partition_partition_name_equals_table(self, connect, table):
        '''
        target: test create partition, the partition equals to table, check status returned
        method: call function: create_partition
        expected: status not ok
        '''
        status = connect.create_partition(table, table, tag)
        assert not status.OK()

    def test_create_partition_partition_name_None(self, connect, table):
        '''
        target: test create partition, partition name set None, check status returned
        method: call function: create_partition
        expected: status not ok
        '''
        partition_name = None
        status = connect.create_partition(table, partition_name, tag)
        assert not status.OK()

    def test_create_partition_tag_name_None(self, connect, table):
        '''
        target: test create partition, tag name set None, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        tag_name = None
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag_name)
        assert not status.OK()

    def test_create_different_partition_tag_name_existed(self, connect, table):
        '''
        target: test create partition, and create the same partition tag again, check status returned
        method: call function: create_partition with the same tag name
        expected: status not ok
        '''
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        assert status.OK()
        new_partition_name = gen_unique_str()
        status = connect.create_partition(table, new_partition_name, tag)
        assert not status.OK()

    def test_create_partition_add_vectors(self, connect, table):
        '''
        target: test create partition, and insert vectors, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        assert status.OK()
        nq = 100
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.insert(table, vectors, ids)
        assert status.OK()

    def test_create_partition_insert_with_tag(self, connect, table):
        '''
        target: test create partition, and insert vectors, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        assert status.OK()
        nq = 100
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.insert(table, vectors, ids, partition_tag=tag)
        assert status.OK()

    def test_create_partition_insert_with_tag_not_existed(self, connect, table):
        '''
        target: test create partition, and insert vectors, check status returned
        method: call function: create_partition
        expected: status not ok
        '''
        tag_new = "tag_new"
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        assert status.OK()
        nq = 100
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.insert(table, vectors, ids, partition_tag=tag_new)
        assert not status.OK()

    def test_create_partition_insert_same_tags(self, connect, table):
        '''
        target: test create partition, and insert vectors, check status returned
        method: call function: create_partition
        expected: status ok
        '''
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        assert status.OK()
        nq = 100
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.insert(table, vectors, ids, partition_tag=tag)
        ids = [(i+100) for i in range(nq)]
        status, ids = connect.insert(table, vectors, ids, partition_tag=tag)
        assert status.OK()
        time.sleep(1)
        status, res = connect.get_table_row_count(partition_name)
        assert res == nq * 2

    def test_create_partition_insert_same_tags_two_tables(self, connect, table):
        '''
        target: test create two partitions, and insert vectors with the same tag to each table, check status returned
        method: call function: create_partition
        expected: status ok, table length is correct
        '''
        partition_name = gen_unique_str()
        table_new = gen_unique_str()
        new_partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        assert status.OK()
        param = {'table_name': table_new,
         'dimension': dim,
         'index_file_size': index_file_size,
         'metric_type': MetricType.L2}
        status = connect.create_table(param)
        status = connect.create_partition(table_new, new_partition_name, tag)
        assert status.OK()
        nq = 100
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.insert(table, vectors, ids, partition_tag=tag)
        ids = [(i+100) for i in range(nq)]
        status, ids = connect.insert(table_new, vectors, ids, partition_tag=tag)
        assert status.OK()
        time.sleep(1)
        status, res = connect.get_table_row_count(new_partition_name)
        assert res == nq


class TestShowBase:

    """
    ******************************************************************
      The following cases are used to test `show_partitions` function 
    ******************************************************************
    """
    def test_show_partitions(self, connect, table):
        '''
        target: test show partitions, check status and partitions returned
        method: create partition first, then call function: show_partitions
        expected: status ok, partition correct
        '''
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        status, res = connect.show_partitions(table)
        assert status.OK()

    def test_show_partitions_no_partition(self, connect, table):
        '''
        target: test show partitions with table name, check status and partitions returned
        method: call function: show_partitions
        expected: status ok, partitions correct
        '''
        partition_name = gen_unique_str()
        status, res = connect.show_partitions(table)
        assert status.OK()

    def test_show_partitions_no_partition_recursive(self, connect, table):
        '''
        target: test show partitions with partition name, check status and partitions returned
        method: call function: show_partitions
        expected: status ok, no partitions
        '''
        partition_name = gen_unique_str()
        status, res = connect.show_partitions(partition_name)
        assert not status.OK()
        # assert len(res) == 0

    def test_show_multi_partitions(self, connect, table):
        '''
        target: test show partitions, check status and partitions returned
        method: create partitions first, then call function: show_partitions
        expected: status ok, partitions correct
        '''
        partition_name = gen_unique_str()
        new_partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        status = connect.create_partition(table, new_partition_name, tag)
        status, res = connect.show_partitions(table)
        assert status.OK()


class TestDropBase:

    """
    ******************************************************************
      The following cases are used to test `drop_partition` function 
    ******************************************************************
    """
    def test_drop_partition(self, connect, table):
        '''
        target: test drop partition, check status and partition if existed
        method: create partitions first, then call function: drop_partition
        expected: status ok, no partitions in db
        '''
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        status = connect.drop_partition(table, tag)
        assert status.OK()
        # check if the partition existed
        status, res = connect.show_partitions(table)
        assert partition_name not in res

    def test_drop_partition_tag_not_existed(self, connect, table):
        '''
        target: test drop partition, but tag not existed
        method: create partitions first, then call function: drop_partition
        expected: status not ok
        '''
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        new_tag = "new_tag"
        status = connect.drop_partition(table, new_tag)
        assert not status.OK()

    def test_drop_partition_tag_not_existed_A(self, connect, table):
        '''
        target: test drop partition, but table not existed
        method: create partitions first, then call function: drop_partition
        expected: status not ok
        '''
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        new_table = gen_unique_str()
        status = connect.drop_partition(new_table, tag)
        assert not status.OK()

    def test_drop_partition_repeatedly(self, connect, table):
        '''
        target: test drop partition twice, check status and partition if existed
        method: create partitions first, then call function: drop_partition
        expected: status not ok, no partitions in db
        '''
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        status = connect.drop_partition(table, tag)
        status = connect.drop_partition(table, tag)
        time.sleep(2)
        assert not status.OK()
        status, res = connect.show_partitions(table)
        assert partition_name not in res

    def test_drop_partition_create(self, connect, table):
        '''
        target: test drop partition, and create again, check status
        method: create partitions first, then call function: drop_partition, create_partition
        expected: status not ok, partition in db
        '''
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        status = connect.drop_partition(table, tag)
        time.sleep(2)
        status = connect.create_partition(table, partition_name, tag)
        assert status.OK()
        status, res = connect.show_partitions(table)
        assert partition_name == res[0].partition_name


class TestNameInvalid(object):
    @pytest.fixture(
        scope="function",
        params=gen_invalid_table_names()
    )
    def get_partition_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_table_names()
    )
    def get_tag_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_table_names()
    )
    def get_table_name(self, request):
        yield request.param

    def test_create_partition_with_invalid_partition_name(self, connect, table, get_partition_name):
        '''
        target: test create partition, with invalid partition name, check status returned
        method: call function: create_partition
        expected: status not ok
        '''
        partition_name = get_partition_name
        status = connect.create_partition(table, partition_name, tag)
        assert not status.OK()

    def test_create_partition_with_invalid_tag_name(self, connect, table):
        '''
        target: test create partition, with invalid partition name, check status returned
        method: call function: create_partition
        expected: status not ok
        '''
        tag_name = "  "
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag_name)
        assert not status.OK()

    def test_drop_partition_with_invalid_table_name(self, connect, table, get_table_name):
        '''
        target: test drop partition, with invalid table name, check status returned
        method: call function: drop_partition
        expected: status not ok
        '''
        table_name = get_table_name
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        status = connect.drop_partition(table_name, tag)
        assert not status.OK()

    def test_drop_partition_with_invalid_tag_name(self, connect, table, get_tag_name):
        '''
        target: test drop partition, with invalid tag name, check status returned
        method: call function: drop_partition
        expected: status not ok
        '''
        tag_name = get_tag_name
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        status = connect.drop_partition(table, tag_name)
        assert not status.OK()

    def test_show_partitions_with_invalid_table_name(self, connect, table, get_table_name):
        '''
        target: test show partitions, with invalid table name, check status returned
        method: call function: show_partitions
        expected: status not ok
        '''
        table_name = get_table_name
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        status, res = connect.show_partitions(table_name)
        assert not status.OK()
