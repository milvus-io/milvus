import time
import random
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from milvus import IndexType, MetricType
from utils import *

dim = 10000
index_file_size = 10
table_id = "test_compact"
COMPACT_TIMEOUT = 60
nprobe = 1
top_k = 1
epsilon = 0.0001
tag = "1970-01-01"


class TestCompactBase:
    """
    ******************************************************************
      The following cases are used to test `compact` function
    ******************************************************************
    """

    def test_compact_table_name_empty(self, connect, table):
        '''
        target: compact table with empty name
        method: compact with the table_name: ""
        expected: status not ok
        '''
        table_name = ""
        status = connect.compact(table_name)
        assert not status.OK()

    def test_compact_table_name_None(self, connect, table):
        '''
        target: compact table whose table name is None
        method: compact with the table_name: None
        expected: status not ok
        '''
        table_name = None
        status = connect.compact(table_name)
        assert not status.OK()

    def test_compact_table_name_not_existed(self, connect, table):
        '''
        target: compact table not existed
        method: compact with a random table_name, which is not in db
        expected: status not ok
        '''
        table_name = gen_unique_str("not_existed_table")
        status = connect.compact(table_name)
        assert not status.OK()
    
    @pytest.fixture(
        scope="function",
        params=gen_invalid_table_names()
    )
    def get_table_name(self, request):
        yield request.param

    def test_compact_table_name_invalid(self, connect, get_table_name):
        '''
        target: compact table with invalid name
        method: compact with invalid table_name
        expected: status not ok
        '''
        table_name = get_table_name
        status = connect.compact(table_name)
        assert not status.OK()
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_and_compact(self, connect, table):
        '''
        target: test add vector and compact 
        method: add vector and compact table
        expected: status ok, vector added
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.compact(table)
        assert status.OK()
        status = connect.flush([table])
        status, res = connect.search_vectors(table, top_k, nprobe, vector) 
        logging.getLogger().info(res)
        assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_delete_and_compact(self, connect, table):
        '''
        target: test add vector, delete it and compact 
        method: add vector, delete it and compact table
        expected: status ok, vectors added and deleted
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.delete_by_id(table, ids)
        assert status.OK()
        status = connect.compact(table)
        assert status.OK()
        status = connect.flush([table])
        status, res = connect.search_vectors(table, top_k, nprobe, vector) 
        logging.getLogger().info(res)
        assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    