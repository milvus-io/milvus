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
table_id = "test_compact"
WAL_TIMEOUT = 30
nprobe = 1
top_k = 1
epsilon = 0.0001
tag = "1970-01-01"
nb = 6000


class TestWalBase:
    """
    ******************************************************************
      The following cases are used to test WAL functionality
    ******************************************************************
    """
    @pytest.mark.timeout(WAL_TIMEOUT)
    def test_wal_add_vectors(self, connect, table):
        '''
        target: add vectors in WAL
        method: add vectors and flush when WAL is enabled
        expected: status ok, vectors added
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, res = connect.get_table_row_count(table)
        assert status.OK()
        assert res == nb
        status, res = connect.get_vector_by_id(table, ids[0]) 
        logging.getLogger().info(res)
        assert status.OK()
        assert_equal_vector(res, vectors[0])

    @pytest.mark.timeout(WAL_TIMEOUT)
    def test_wal_delete_vectors(self, connect, table):
        '''
        target: delete vectors in WAL
        method: delete vectors and flush when WAL is enabled
        expected: status ok, vectors deleted
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status, res = connect.get_table_row_count(table)
        assert status.OK()
        logging.getLogger().info(res)
        assert res == 0
        status = connect.delete_by_id(table, ids)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, res = connect.get_table_row_count(table)
        assert status.OK()
        assert res == 0

    @pytest.mark.timeout(WAL_TIMEOUT)
    def test_wal_invalid_operation(self, connect, table):
        '''
        target: invalid operation in WAL
        method: add vectors, delete with non-existent ids and flush when WAL is enabled
        expected: status ok, search with vector have result
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()
        status = connect.delete_by_id(table, [0])
        assert status.OK()
        status, res = connect.get_table_row_count(table)
        assert status.OK()
        assert res == 0
        status = connect.flush([table])
        assert status.OK()
        status, res = connect.get_table_row_count(table)
        assert status.OK()
        assert res == 1

    @pytest.mark.timeout(WAL_TIMEOUT)
    def test_wal_invalid_operation_B(self, connect, table):
        '''
        target: invalid operation in WAL
        method: add vectors, delete with not existed table name when WAL is enabled
        expected: status not ok
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status, res = connect.get_table_row_count(table)
        assert status.OK()
        assert res == 0
        table_new = gen_unique_str()
        status = connect.delete_by_id(table_new, ids)
        assert not status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, res = connect.get_table_row_count(table)
        assert status.OK()
        assert res == nb

    @pytest.mark.timeout(WAL_TIMEOUT)
    def test_wal_server_crashed_recovery(self, connect, table):
        '''
        target: test wal when server crashed unexpectedly and restarted
        method: add vectors, server killed before flush, restarted server and flush
        expected: status ok, add request is recovered and vectors added
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()
        status, res = connect.get_table_row_count(table)
        assert status.OK()
        logging.getLogger().info(res) # should be 0 because no auto flush
        logging.getLogger().info("Stop server and restart")
        # kill server and restart. auto flush should be set to 15 seconds.
        time.sleep(15)
        status = connect.flush([table])
        assert status.OK()
        status, res = connect.get_table_row_count(table)
        assert status.OK()
        assert res == 1
        status, res = connect.get_vector_by_id(table, ids[0]) 
        logging.getLogger().info(res)
        assert status.OK()
        assert_equal_vector(res, vector[0])