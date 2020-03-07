import pdb
import pytest
import logging
import itertools
from time import sleep
import threading
from multiprocessing import Process
from milvus import IndexType, MetricType
from utils import *

dim = 128
index_file_size = 10
add_time_interval = 3
tag = "1970-01-01"
nb = 6000

class TestTableCount:
    """
    params means different nb, the nb value may trigger merge, or not
    """
    @pytest.fixture(
        scope="function",
        params=[
            1,
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
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in cpu mode")
        if request.param["index_type"] == IndexType.IVF_PQ:
            pytest.skip("Skip PQ Temporary")
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
        connect.flush([table])
        status, res = connect.get_table_row_count(table)
        assert res == nb

    def test_table_rows_count_partition(self, connect, table, add_vectors_nb):
        '''
        target: test table rows_count is correct or not
        method: create table, create partition and add vectors in it,
            assert the value returned by get_table_row_count method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nb = add_vectors_nb
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(table, tag)
        assert status.OK()
        res = connect.add_vectors(table_name=table, records=vectors, partition_tag=tag)
        connect.flush([table])
        status, res = connect.get_table_row_count(table)
        assert res == nb

    def test_table_rows_count_multi_partitions_A(self, connect, table, add_vectors_nb):
        '''
        target: test table rows_count is correct or not
        method: create table, create partitions and add vectors in it,
            assert the value returned by get_table_row_count method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        new_tag = "new_tag"
        nb = add_vectors_nb
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(table, tag)
        status = connect.create_partition(table, new_tag)
        assert status.OK()
        res = connect.add_vectors(table_name=table, records=vectors)
        connect.flush([table])
        status, res = connect.get_table_row_count(table)
        assert res == nb

    def test_table_rows_count_multi_partitions_B(self, connect, table, add_vectors_nb):
        '''
        target: test table rows_count is correct or not
        method: create table, create partitions and add vectors in one of the partitions,
            assert the value returned by get_table_row_count method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        new_tag = "new_tag"
        nb = add_vectors_nb
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(table, tag)
        status = connect.create_partition(table, new_tag)
        assert status.OK()
        res = connect.add_vectors(table_name=table, records=vectors, partition_tag=tag)
        connect.flush([table])
        status, res = connect.get_table_row_count(table)
        assert res == nb

    def test_table_rows_count_multi_partitions_C(self, connect, table, add_vectors_nb):
        '''
        target: test table rows_count is correct or not
        method: create table, create partitions and add vectors in one of the partitions,
            assert the value returned by get_table_row_count method is equal to length of vectors
        expected: the table count is equal to the length of vectors
        '''
        new_tag = "new_tag"
        nb = add_vectors_nb
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(table, tag)
        status = connect.create_partition(table, new_tag)
        assert status.OK()
        res = connect.add_vectors(table_name=table, records=vectors, partition_tag=tag)
        res = connect.add_vectors(table_name=table, records=vectors, partition_tag=new_tag)
        connect.flush([table])
        status, res = connect.get_table_row_count(table)
        assert res == nb * 2

    def test_table_rows_count_after_index_created(self, connect, table, get_simple_index):
        '''
        target: test get_table_row_count, after index have been created
        method: add vectors in db, and create index, then calling get_table_row_count with correct params 
        expected: get_table_row_count raise exception
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        nb = 100
        vectors = gen_vectors(nb, dim)
        res = connect.add_vectors(table_name=table, records=vectors)
        connect.flush([table])
        connect.create_index(table, index_type, index_param)
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
        table_name = gen_unique_str()
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
            milvus = get_milvus(args["handler"])
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
        for i in range(20):
            table_name = gen_unique_str()
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.L2}
            connect.create_table(param)
            res = connect.add_vectors(table_name=table_name, records=vectors)
        connect.flush(table_list)
        for i in range(20):
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
            1,
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
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if request.param["index_type"] == IndexType.IVF_PQ:
            pytest.skip("Skip PQ Temporary")
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
        connect.flush([ip_table])
        status, res = connect.get_table_row_count(ip_table)
        assert res == nb

    def test_table_rows_count_after_index_created(self, connect, ip_table, get_simple_index):
        '''
        target: test get_table_row_count, after index have been created
        method: add vectors in db, and create index, then calling get_table_row_count with correct params
        expected: get_table_row_count raise exception
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        nb = 100
        vectors = gen_vectors(nb, dim)
        res = connect.add_vectors(table_name=ip_table, records=vectors)
        connect.flush([ip_table])
        connect.create_index(ip_table, index_type, index_param)
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
    @pytest.mark.timeout(60)
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
            milvus = get_milvus(args["handler"])
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
        for i in range(20):
            table_name = gen_unique_str('test_table_rows_count_multi_tables')
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.IP}
            connect.create_table(param)
            res = connect.add_vectors(table_name=table_name, records=vectors)
        connect.flush(table_list)
        for i in range(20):
            status, res = connect.get_table_row_count(table_list[i])
            assert status.OK()
            assert res == nq


class TestTableCountJAC:
    """
    params means different nb, the nb value may trigger merge, or not
    """

    @pytest.fixture(
        scope="function",
        params=[
            1,
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
        params=gen_simple_index()
    )
    def get_jaccard_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] == IndexType.IVFLAT or request.param["index_type"] == IndexType.FLAT:
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    def test_table_rows_count(self, connect, jac_table, add_vectors_nb):
        '''
        target: test table rows_count is correct or not
        method: create table and add vectors in it,
            assert the value returned by get_table_row_count method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nb = add_vectors_nb
        tmp, vectors = gen_binary_vectors(nb, dim)
        res = connect.add_vectors(table_name=jac_table, records=vectors)
        connect.flush([jac_table])
        status, res = connect.get_table_row_count(jac_table)
        assert res == nb

    def test_table_rows_count_after_index_created(self, connect, jac_table, get_jaccard_index):
        '''
        target: test get_table_row_count, after index have been created
        method: add vectors in db, and create index, then calling get_table_row_count with correct params
        expected: get_table_row_count raise exception
        '''
        nb = 100
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        tmp, vectors = gen_binary_vectors(nb, dim)
        res = connect.add_vectors(table_name=jac_table, records=vectors)
        connect.flush([jac_table])
        connect.create_index(jac_table, index_type, index_param)
        status, res = connect.get_table_row_count(jac_table)
        assert res == nb

    @pytest.mark.level(2)
    def test_count_without_connection(self, jac_table, dis_connect):
        '''
        target: test get_table_row_count, without connection
        method: calling get_table_row_count with correct params, with a disconnected instance
        expected: get_table_row_count raise exception
        '''
        with pytest.raises(Exception) as e:
            status = dis_connect.get_table_row_count(jac_table)

    def test_table_rows_count_no_vectors(self, connect, jac_table):
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
        status, res = connect.get_table_row_count(jac_table)
        assert res == 0

    def test_table_rows_count_multi_tables(self, connect):
        '''
        target: test table rows_count is correct or not with multiple tables of IP
        method: create table and add vectors in it,
            assert the value returned by get_table_row_count method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nq = 100
        tmp, vectors = gen_binary_vectors(nq, dim)
        table_list = []
        for i in range(20):
            table_name = gen_unique_str('test_table_rows_count_multi_tables')
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.JACCARD}
            connect.create_table(param)
            res = connect.add_vectors(table_name=table_name, records=vectors)
        connect.flush(table_list)
        for i in range(20):
            status, res = connect.get_table_row_count(table_list[i])
            assert status.OK()
            assert res == nq

class TestTableCountHAM:
    """
    params means different nb, the nb value may trigger merge, or not
    """

    @pytest.fixture(
        scope="function",
        params=[
            1,
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
        params=gen_simple_index()
    )
    def get_hamming_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] == IndexType.IVFLAT or request.param["index_type"] == IndexType.FLAT:
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    def test_table_rows_count(self, connect, ham_table, add_vectors_nb):
        '''
        target: test table rows_count is correct or not
        method: create table and add vectors in it,
            assert the value returned by get_table_row_count method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nb = add_vectors_nb
        tmp, vectors = gen_binary_vectors(nb, dim)
        res = connect.add_vectors(table_name=ham_table, records=vectors)
        connect.flush([ham_table])
        status, res = connect.get_table_row_count(ham_table)
        assert res == nb

    def test_table_rows_count_after_index_created(self, connect, ham_table, get_hamming_index):
        '''
        target: test get_table_row_count, after index have been created
        method: add vectors in db, and create index, then calling get_table_row_count with correct params
        expected: get_table_row_count raise exception
        '''
        nb = 100
        index_type = get_hamming_index["index_type"]
        index_param = get_hamming_index["index_param"]
        tmp, vectors = gen_binary_vectors(nb, dim)
        res = connect.add_vectors(table_name=ham_table, records=vectors)
        connect.flush([ham_table])
        connect.create_index(ham_table, index_type, index_param)
        status, res = connect.get_table_row_count(ham_table)
        assert res == nb

    @pytest.mark.level(2)
    def test_count_without_connection(self, ham_table, dis_connect):
        '''
        target: test get_table_row_count, without connection
        method: calling get_table_row_count with correct params, with a disconnected instance
        expected: get_table_row_count raise exception
        '''
        with pytest.raises(Exception) as e:
            status = dis_connect.get_table_row_count(ham_table)

    def test_table_rows_count_no_vectors(self, connect, ham_table):
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
        status, res = connect.get_table_row_count(ham_table)
        assert res == 0

    def test_table_rows_count_multi_tables(self, connect):
        '''
        target: test table rows_count is correct or not with multiple tables of IP
        method: create table and add vectors in it,
            assert the value returned by get_table_row_count method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nq = 100
        tmp, vectors = gen_binary_vectors(nq, dim)
        table_list = []
        for i in range(20):
            table_name = gen_unique_str('test_table_rows_count_multi_tables')
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.HAMMING}
            connect.create_table(param)
            res = connect.add_vectors(table_name=table_name, records=vectors)
        connect.flush(table_list)
        for i in range(20):
            status, res = connect.get_table_row_count(table_list[i])
            assert status.OK()
            assert res == nq


class TestTableCountTANIMOTO:
    """
    params means different nb, the nb value may trigger merge, or not
    """

    @pytest.fixture(
        scope="function",
        params=[
            1,
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
        params=gen_simple_index()
    )
    def get_tanimoto_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] == IndexType.IVFLAT or request.param["index_type"] == IndexType.FLAT:
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    def test_table_rows_count(self, connect, tanimoto_table, add_vectors_nb):
        '''
        target: test table rows_count is correct or not
        method: create table and add vectors in it,
            assert the value returned by get_table_row_count method is equal to length of vectors
        expected: the count is equal to the length of vectors
        '''
        nb = add_vectors_nb
        tmp, vectors = gen_binary_vectors(nb, dim)
        res = connect.add_vectors(table_name=tanimoto_table, records=vectors)
        connect.flush([tanimoto_table])
        status, res = connect.get_table_row_count(tanimoto_table)
        assert status.OK()
        assert res == nb
