import time
import random
import pdb
import logging
import threading
from builtins import Exception
from multiprocessing import Pool, Process
import pytest

from milvus import Milvus, IndexType
from utils import *


dim = 128
index_file_size = 10
table_id = "test_delete"
DELETE_TIMEOUT = 60
vectors = gen_vectors(100, dim)

class TestDeleteVectorsBase:
    """
    generate invalid query range params
    """
    @pytest.fixture(
        scope="function",
        params=[
            (get_current_day(), get_current_day()),
            (get_last_day(1), get_last_day(1)),
            (get_next_day(1), get_next_day(1))
        ]
    )
    def get_invalid_range(self, request):
        yield request.param

    @pytest.mark.timeout(DELETE_TIMEOUT)
    def test_delete_vectors_invalid_range(self, connect, table, get_invalid_range):
        '''
        target: test delete vectors, no index created
        method: call `delete_vectors_by_range`, with invalid date params
        expected: return code 0
        '''
        start_date = get_invalid_range[0]
        end_date = get_invalid_range[1]
        status, ids = connect.add_vectors(table, vectors)
        status = connect.delete_vectors_by_range(table, start_date, end_date)
        assert not status.OK()

    """
    generate valid query range params, no search result
    """
    @pytest.fixture(
        scope="function",
        params=[
            (get_last_day(2), get_last_day(1)),
            (get_last_day(2), get_current_day()),
            (get_next_day(1), get_next_day(2))
        ]
    )
    def get_valid_range_no_result(self, request):
        yield request.param

    @pytest.mark.timeout(DELETE_TIMEOUT)
    def test_delete_vectors_valid_range_no_result(self, connect, table, get_valid_range_no_result):
        '''
        target: test delete vectors, no index created
        method: call `delete_vectors_by_range`, with valid date params
        expected: return code 0
        '''
        start_date = get_valid_range_no_result[0]
        end_date = get_valid_range_no_result[1]
        status, ids = connect.add_vectors(table, vectors)
        time.sleep(2)
        status = connect.delete_vectors_by_range(table, start_date, end_date)
        assert status.OK()
        status, result = connect.get_table_row_count(table)
        assert result == 100

    """
    generate valid query range params, no search result
    """
    @pytest.fixture(
        scope="function",
        params=[
            (get_last_day(2), get_next_day(2)),
            (get_current_day(), get_next_day(2)),
        ]
    )
    def get_valid_range(self, request):
        yield request.param

    @pytest.mark.timeout(DELETE_TIMEOUT)
    def test_delete_vectors_valid_range(self, connect, table, get_valid_range):
        '''
        target: test delete vectors, no index created
        method: call `delete_vectors_by_range`, with valid date params
        expected: return code 0
        '''
        start_date = get_valid_range[0]
        end_date = get_valid_range[1]
        status, ids = connect.add_vectors(table, vectors)
        time.sleep(2)
        status = connect.delete_vectors_by_range(table, start_date, end_date)
        assert status.OK()
        status, result = connect.get_table_row_count(table)
        assert result == 0

    @pytest.fixture(
        scope="function",
        params=gen_index_params()
    )
    def get_index_params(self, request, args):
        if "internal" in args:
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in open source")
        return request.param

    @pytest.mark.timeout(DELETE_TIMEOUT)
    def test_delete_vectors_valid_range_index_created(self, connect, table, get_index_params):
        '''
        target: test delete vectors, no index created
        method: call `delete_vectors_by_range`, with valid date params
        expected: return code 0
        '''
        start_date = get_current_day()
        end_date = get_next_day(2)
        index_params = get_index_params
        logging.getLogger().info(index_params)
        status, ids = connect.add_vectors(table, vectors)
        status = connect.create_index(table, index_params)
        logging.getLogger().info(status)
        logging.getLogger().info("Start delete vectors by range: %s:%s" % (start_date, end_date))
        status = connect.delete_vectors_by_range(table, start_date, end_date)
        assert status.OK()
        status, result = connect.get_table_row_count(table)
        assert result == 0

    @pytest.mark.timeout(DELETE_TIMEOUT)
    def test_delete_vectors_no_data(self, connect, table):
        '''
        target: test delete vectors, no index created
        method: call `delete_vectors_by_range`, with valid date params, and no data in db
        expected: return code 0
        '''
        start_date = get_current_day()
        end_date = get_next_day(2)
        # status, ids = connect.add_vectors(table, vectors)
        status = connect.delete_vectors_by_range(table, start_date, end_date)
        assert status.OK()

    @pytest.mark.timeout(DELETE_TIMEOUT)
    def test_delete_vectors_table_not_existed(self, connect):
        '''
        target: test delete vectors, table not existed in db
        method: call `delete_vectors_by_range`, with table not existed
        expected: return code not 0
        '''
        start_date = get_current_day()
        end_date = get_next_day(2)
        table_name = gen_unique_str("not_existed_table")
        status = connect.delete_vectors_by_range(table_name, start_date, end_date)
        assert not status.OK()

    @pytest.mark.timeout(DELETE_TIMEOUT)
    def test_delete_vectors_table_None(self, connect, table):
        '''
        target: test delete vectors, table set Nope
        method: call `delete_vectors_by_range`, with table value is None
        expected: return code not 0
        '''
        start_date = get_current_day()
        end_date = get_next_day(2)
        table_name = None
        with pytest.raises(Exception) as e:
            status = connect.delete_vectors_by_range(table_name, start_date, end_date)

    @pytest.mark.timeout(DELETE_TIMEOUT)
    def test_delete_vectors_valid_range_multi_tables(self, connect, get_valid_range):
        '''
        target: test delete vectors is correct or not with multiple tables of L2
        method: create 50 tables and add vectors into them , then delete vectors
                in valid range
        expected: return code 0
        '''
        nq = 100
        vectors = gen_vectors(nq, dim)
        table_list = []
        for i in range(50):
            table_name = gen_unique_str('test_delete_vectors_valid_range_multi_tables')
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.L2}
            connect.create_table(param)
            status, ids = connect.add_vectors(table_name=table_name, records=vectors)
        time.sleep(2)
        start_date = get_valid_range[0]
        end_date = get_valid_range[1]
        for i in range(50):
            status = connect.delete_vectors_by_range(table_list[i], start_date, end_date)
            assert status.OK()
            status, result = connect.get_table_row_count(table_list[i])
            assert result == 0


class TestDeleteVectorsIP:
    """
    generate invalid query range params
    """
    @pytest.fixture(
        scope="function",
        params=[
            (get_current_day(), get_current_day()),
            (get_last_day(1), get_last_day(1)),
            (get_next_day(1), get_next_day(1))
        ]
    )
    def get_invalid_range(self, request):
        yield request.param

    @pytest.mark.timeout(DELETE_TIMEOUT)
    def test_delete_vectors_invalid_range(self, connect, ip_table, get_invalid_range):
        '''
        target: test delete vectors, no index created
        method: call `delete_vectors_by_range`, with invalid date params
        expected: return code 0
        '''
        start_date = get_invalid_range[0]
        end_date = get_invalid_range[1]
        status, ids = connect.add_vectors(ip_table, vectors)
        status = connect.delete_vectors_by_range(ip_table, start_date, end_date)
        assert not status.OK()

    """
    generate valid query range params, no search result
    """
    @pytest.fixture(
        scope="function",
        params=[
            (get_last_day(2), get_last_day(1)),
            (get_last_day(2), get_current_day()),
            (get_next_day(1), get_next_day(2))
        ]
    )
    def get_valid_range_no_result(self, request):
        yield request.param

    @pytest.mark.timeout(DELETE_TIMEOUT)
    def test_delete_vectors_valid_range_no_result(self, connect, ip_table, get_valid_range_no_result):
        '''
        target: test delete vectors, no index created
        method: call `delete_vectors_by_range`, with valid date params
        expected: return code 0
        '''
        start_date = get_valid_range_no_result[0]
        end_date = get_valid_range_no_result[1]
        status, ids = connect.add_vectors(ip_table, vectors)
        time.sleep(2)
        status = connect.delete_vectors_by_range(ip_table, start_date, end_date)
        assert status.OK()
        status, result = connect.get_table_row_count(ip_table)
        assert result == 100

    """
    generate valid query range params, no search result
    """
    @pytest.fixture(
        scope="function",
        params=[
            (get_last_day(2), get_next_day(2)),
            (get_current_day(), get_next_day(2)),
        ]
    )
    def get_valid_range(self, request):
        yield request.param

    @pytest.mark.timeout(DELETE_TIMEOUT)
    def test_delete_vectors_valid_range(self, connect, ip_table, get_valid_range):
        '''
        target: test delete vectors, no index created
        method: call `delete_vectors_by_range`, with valid date params
        expected: return code 0
        '''
        start_date = get_valid_range[0]
        end_date = get_valid_range[1]
        status, ids = connect.add_vectors(ip_table, vectors)
        time.sleep(2)
        status = connect.delete_vectors_by_range(ip_table, start_date, end_date)
        assert status.OK()
        status, result = connect.get_table_row_count(ip_table)
        assert result == 0

    @pytest.fixture(
        scope="function",
        params=gen_index_params()
    )
    def get_index_params(self, request, args):
        if "internal" in args:
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in open source")
        return request.param

    @pytest.mark.timeout(DELETE_TIMEOUT)
    def test_delete_vectors_valid_range_index_created(self, connect, ip_table, get_index_params):
        '''
        target: test delete vectors, no index created
        method: call `delete_vectors_by_range`, with valid date params
        expected: return code 0
        '''
        start_date = get_current_day()
        end_date = get_next_day(2)
        index_params = get_index_params
        logging.getLogger().info(index_params)
        status, ids = connect.add_vectors(ip_table, vectors)
        status = connect.create_index(ip_table, index_params)
        logging.getLogger().info(status)
        logging.getLogger().info("Start delete vectors by range: %s:%s" % (start_date, end_date))
        status = connect.delete_vectors_by_range(ip_table, start_date, end_date)
        assert status.OK()
        status, result = connect.get_table_row_count(ip_table)
        assert result == 0

    @pytest.mark.timeout(DELETE_TIMEOUT)
    def test_delete_vectors_no_data(self, connect, ip_table):
        '''
        target: test delete vectors, no index created
        method: call `delete_vectors_by_range`, with valid date params, and no data in db
        expected: return code 0
        '''
        start_date = get_current_day()
        end_date = get_next_day(2)
        # status, ids = connect.add_vectors(table, vectors)
        status = connect.delete_vectors_by_range(ip_table, start_date, end_date)
        assert status.OK()

    @pytest.mark.timeout(DELETE_TIMEOUT)
    def test_delete_vectors_table_None(self, connect, ip_table):
        '''
        target: test delete vectors, table set Nope
        method: call `delete_vectors_by_range`, with table value is None
        expected: return code not 0
        '''
        start_date = get_current_day()
        end_date = get_next_day(2)
        table_name = None
        with pytest.raises(Exception) as e:
            status = connect.delete_vectors_by_range(table_name, start_date, end_date)

    @pytest.mark.timeout(DELETE_TIMEOUT)
    def test_delete_vectors_valid_range_multi_tables(self, connect, get_valid_range):
        '''
        target: test delete vectors is correct or not with multiple tables of IP
        method: create 50 tables and add vectors into them , then delete vectors
                in valid range
        expected: return code 0
        '''
        nq = 100
        vectors = gen_vectors(nq, dim)
        table_list = []
        for i in range(50):
            table_name = gen_unique_str('test_delete_vectors_valid_range_multi_tables')
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.IP}
            connect.create_table(param)
            status, ids = connect.add_vectors(table_name=table_name, records=vectors)
        time.sleep(2)
        start_date = get_valid_range[0]
        end_date = get_valid_range[1]
        for i in range(50):
            status = connect.delete_vectors_by_range(table_list[i], start_date, end_date)
            assert status.OK()
            status, result = connect.get_table_row_count(table_list[i])
            assert result == 0

class TestDeleteVectorsParamsInvalid:

    """
    Test search table with invalid table names
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_table_names()
    )
    def get_table_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_delete_vectors_table_invalid_name(self, connect, get_table_name):
        '''
        '''
        start_date = get_current_day()
        end_date = get_next_day(2)
        table_name = get_table_name
        logging.getLogger().info(table_name)
        top_k = 1
        nprobe = 1 
        status = connect.delete_vectors_by_range(table_name, start_date, end_date)
        assert not status.OK()

    """
    Test search table with invalid query ranges
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_query_ranges()
    )
    def get_query_ranges(self, request):
        yield request.param

    @pytest.mark.timeout(DELETE_TIMEOUT)
    def test_delete_vectors_range_invalid(self, connect, table, get_query_ranges):
        '''
        target: test search fuction, with the wrong query_range
        method: search with query_range
        expected: raise an error, and the connection is normal
        '''
        start_date = get_query_ranges[0][0]
        end_date = get_query_ranges[0][1]
        status, ids = connect.add_vectors(table, vectors)
        logging.getLogger().info(get_query_ranges)
        with pytest.raises(Exception) as e:
            status = connect.delete_vectors_by_range(table, start_date, end_date)