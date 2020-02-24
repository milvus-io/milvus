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
INFO_TIMEOUT = 30
nprobe = 1
top_k = 1
epsilon = 0.0001
tag = "1970-01-01"
nb = 6000


class TestTableInfoBase:
    """
    ******************************************************************
      The following cases are used to test `table_info` function
    ******************************************************************
    """
    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_table_info_name_None(self, connect, table):
        '''
        target: get table info where table name is None
        method: call table_info with the table_name: None
        expected: status not ok
        '''
        table_name = None
        status, info = connect.table_info(table_name)
        assert not status.OK()

    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_table_info_name_not_existed(self, connect, table):
        '''
        target: get table info where table name does not exist
        method: call table_info with a random table_name, which is not in db
        expected: status not ok
        '''
        table_name = gen_unique_str("not_existed_table")
        status, info = connect.table_info(table_name)
        assert not status.OK()
    
    @pytest.fixture(
        scope="function",
        params=gen_invalid_table_names()
    )
    def get_table_name(self, request):
        yield request.param

    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_table_info_name_invalid(self, connect, get_table_name):
        '''
        target: get table info where table name is invalid
        method: call table_info with invalid table_name
        expected: status not ok
        '''
        table_name = get_table_name
        status, info = connect.table_info(table_name)
        assert not status.OK()