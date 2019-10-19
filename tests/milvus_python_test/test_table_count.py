import random
import pdb

import pytest
import logging
import itertools

from time import sleep
from multiprocessing import Process
from milvus import Milvus
from utils import *
from milvus import IndexType, MetricType

dim = 128
index_file_size = 10
add_time_interval = 5


class TestTableCount:
    """
    params means different nb, the nb value may trigger merge, or not
    """
    @pytest.fixture(
        scope="function",
        params=[
            100,
            5000,
            100000,
        ],
    )
    def add_vectors_nb(self, request):
        yield request.param

    """
    generate valid create_index params
    """
    @pytest.fixture(
        scope="function",
        params=gen_index_params()
    )
    def get_index_params(self, request, args):
        if "internal" not in args:
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in open source")
        return request.param

    def test_table_rows_count(self, connect, table, add_vectors_nb):
        '''
        target: test table rows_count is correct or not
        method: create table and add vectors in it,
            assert the value returned by get_table_row_count method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nb = add_vectors_nb
        vectors = gen_vectors(nb, dim)
        res = connect.add_vectors(table_name=table, records=vectors)
        time.sleep(add_time_interval)
        status, res = connect.get_table_row_count(table)
        assert res == nb

    def test_table_rows_count_after_index_created(self, connect, table, get_index_params):
        '''
        target: test get_table_row_count, after index have been created
        method: add vectors in db, and create index, then calling get_table_row_count with correct params 
        expected: get_table_row_count raise exception
        '''
        nb = 100
        index_params = get_index_params
        vectors = gen_vectors(nb, dim)
        res = connect.add_vectors(table_name=table, records=vectors)
        time.sleep(add_time_interval)
        # logging.getLogger().info(index_params)
        connect.create_index(table, index_params)
        status, res = connect.get_table_row_count(table)
        assert res == nb

    @pytest.mark.level(2)
    def test_count_without_connection(self, table, dis_connect):
        '''
        target: test get_table_row_count, without connection
        method: calling get_table_row_count with correct params, with a disconnected instance
        expected: get_table_row_count raise exception
        '''
        with pytest.raises(Exception) as e:
            status = dis_connect.get_table_row_count(table)

    def test_table_rows_count_no_vectors(self, connect, table):
        '''
        target: test table rows_count is correct or not, if table is empty
        method: create table and no vectors in it,
            assert the value returned by get_table_row_count method is equal to 0
        expected: the count is equal to 0
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size}
        connect.create_table(param)        
        status, res = connect.get_table_row_count(table)
        assert res == 0

    # TODO: enable
    @pytest.mark.level(2)
    @pytest.mark.timeout(20)
    def _test_table_rows_count_multiprocessing(self, connect, table, args):
        '''
        target: test table rows_count is correct or not with multiprocess
        method: create table and add vectors in it,
            assert the value returned by get_table_row_count method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nq = 2
        uri = "tcp://%s:%s" % (args["ip"], args["port"])
        vectors = gen_vectors(nq, dim)
        res = connect.add_vectors(table_name=table, records=vectors)
        time.sleep(add_time_interval)

        def rows_count(milvus):
            status, res = milvus.get_table_row_count(table)
            logging.getLogger().info(status)
            assert res == nq

        process_num = 8
        processes = []
        for i in range(process_num):
            milvus = Milvus()
            milvus.connect(uri=uri)
            p = Process(target=rows_count, args=(milvus, ))
            processes.append(p)
            p.start()
            logging.getLogger().info(p)
        for p in processes:
            p.join()

    def test_table_rows_count_multi_tables(self, connect):
        '''
        target: test table rows_count is correct or not with multiple tables of L2
        method: create table and add vectors in it,
            assert the value returned by get_table_row_count method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nq = 100
        vectors = gen_vectors(nq, dim)
        table_list = []
        for i in range(50):
            table_name = gen_unique_str('test_table_rows_count_multi_tables')
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.L2}
            connect.create_table(param)
            res = connect.add_vectors(table_name=table_name, records=vectors)
        time.sleep(2)
        for i in range(50):
            status, res = connect.get_table_row_count(table_list[i])
            assert status.OK()
            assert res == nq


class TestTableCountIP:
    """
    params means different nb, the nb value may trigger merge, or not
    """

    @pytest.fixture(
        scope="function",
        params=[
            100,
            5000,
            100000,
        ],
    )
    def add_vectors_nb(self, request):
        yield request.param

    """
    generate valid create_index params
    """

    @pytest.fixture(
        scope="function",
        params=gen_index_params()
    )
    def get_index_params(self, request, args):
        if "internal" not in args:
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in open source")
        return request.param

    def test_table_rows_count(self, connect, ip_table, add_vectors_nb):
        '''
        target: test table rows_count is correct or not
        method: create table and add vectors in it,
            assert the value returned by get_table_row_count method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nb = add_vectors_nb
        vectors = gen_vectors(nb, dim)
        res = connect.add_vectors(table_name=ip_table, records=vectors)
        time.sleep(add_time_interval)
        status, res = connect.get_table_row_count(ip_table)
        assert res == nb

    def test_table_rows_count_after_index_created(self, connect, ip_table, get_index_params):
        '''
        target: test get_table_row_count, after index have been created
        method: add vectors in db, and create index, then calling get_table_row_count with correct params
        expected: get_table_row_count raise exception
        '''
        nb = 100
        index_params = get_index_params
        vectors = gen_vectors(nb, dim)
        res = connect.add_vectors(table_name=ip_table, records=vectors)
        time.sleep(add_time_interval)
        # logging.getLogger().info(index_params)
        connect.create_index(ip_table, index_params)
        status, res = connect.get_table_row_count(ip_table)
        assert res == nb

    @pytest.mark.level(2)
    def test_count_without_connection(self, ip_table, dis_connect):
        '''
        target: test get_table_row_count, without connection
        method: calling get_table_row_count with correct params, with a disconnected instance
        expected: get_table_row_count raise exception
        '''
        with pytest.raises(Exception) as e:
            status = dis_connect.get_table_row_count(ip_table)

    def test_table_rows_count_no_vectors(self, connect, ip_table):
        '''
        target: test table rows_count is correct or not, if table is empty
        method: create table and no vectors in it,
            assert the value returned by get_table_row_count method is equal to 0
        expected: the count is equal to 0
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size}
        connect.create_table(param)
        status, res = connect.get_table_row_count(ip_table)
        assert res == 0

    # TODO: enable
    @pytest.mark.level(2)
    @pytest.mark.timeout(20)
    def _test_table_rows_count_multiprocessing(self, connect, ip_table, args):
        '''
        target: test table rows_count is correct or not with multiprocess
        method: create table and add vectors in it,
            assert the value returned by get_table_row_count method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nq = 2
        uri = "tcp://%s:%s" % (args["ip"], args["port"])
        vectors = gen_vectors(nq, dim)
        res = connect.add_vectors(table_name=ip_table, records=vectors)
        time.sleep(add_time_interval)

        def rows_count(milvus):
            status, res = milvus.get_table_row_count(ip_table)
            logging.getLogger().info(status)
            assert res == nq

        process_num = 8
        processes = []
        for i in range(process_num):
            milvus = Milvus()
            milvus.connect(uri=uri)
            p = Process(target=rows_count, args=(milvus,))
            processes.append(p)
            p.start()
            logging.getLogger().info(p)
        for p in processes:
            p.join()

    def test_table_rows_count_multi_tables(self, connect):
        '''
        target: test table rows_count is correct or not with multiple tables of IP
        method: create table and add vectors in it,
            assert the value returned by get_table_row_count method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nq = 100
        vectors = gen_vectors(nq, dim)
        table_list = []
        for i in range(50):
            table_name = gen_unique_str('test_table_rows_count_multi_tables')
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.IP}
            connect.create_table(param)
            res = connect.add_vectors(table_name=table_name, records=vectors)
        time.sleep(2)
        for i in range(50):
            status, res = connect.get_table_row_count(table_list[i])
            assert status.OK()
            assert res == nq