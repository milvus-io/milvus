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


class TestAddBase:
    """
    ******************************************************************
      The following cases are used to test `add_vectors / index / search / delete` mixed function
    ******************************************************************
    """
    @pytest.fixture(
        scope="function",
        params=gen_simple_index_params()
    )
    def get_simple_index_params(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in cpu mode")
        if request.param["index_type"] == IndexType.IVF_PQ:
            pytest.skip("Skip PQ Temporary")
        return request.param

    def test_add_vector_create_table(self, connect, table):
        '''
        target: test add vector, then create table again
        method: add vector and create table
        expected: status not ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        param = {'table_name': table,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        assert not status.OK()

    def test_add_vector_has_table(self, connect, table):
        '''
        target: test add vector, then check table existence
        method: add vector and call HasTable
        expected: table exists, status ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert assert_has_table(connect, table)

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_delete_table_add_vector(self, connect, table):
        '''
        target: test add vector after table deleted
        method: delete table and add vector
        expected: status not ok
        '''
        status = connect.delete_table(table)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert not status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_delete_table_add_vector_another(self, connect, table):
        '''
        target: test add vector to table_1 after table_2 deleted
        method: delete table_2 and add vector to table_1
        expected: status ok
        '''
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        status = connect.delete_table(table)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(param['table_name'], vector)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_delete_table(self, connect, table):
        '''
        target: test delete table after add vector
        method: add vector and delete table
        expected: status ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        status = connect.delete_table(table)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_delete_another_table(self, connect, table):
        '''
        target: test delete table_1 table after add vector to table_2
        method: add vector and delete table
        expected: status ok
        '''
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_sleep_delete_table(self, connect, table):
        '''
        target: test delete table after add vector for a while
        method: add vector, sleep, and delete table
        expected: status ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        time.sleep(1)
        status = connect.delete_table(table)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_sleep_delete_another_table(self, connect, table):
        '''
        target: test delete table_1 table after add vector to table_2 for a while
        method: add vector , sleep, and delete table
        expected: status ok
        '''
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        time.sleep(1)
        status = connect.delete_table(param['table_name'])
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_create_index_add_vector(self, connect, table, get_simple_index_params):
        '''
        target: test add vector after build index
        method: build index and add vector
        expected: status ok
        '''
        index_param = get_simple_index_params
        status = connect.create_index(table, index_param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_create_index_add_vector_another(self, connect, table, get_simple_index_params):
        '''
        target: test add vector to table_2 after build index for table_1
        method: build index and add vector
        expected: status ok
        '''
        index_param = get_simple_index_params
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        status = connect.create_index(table, index_param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        connect.delete_table(param['table_name'])
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_create_index(self, connect, table, get_simple_index_params):
        '''
        target: test build index add after vector
        method: add vector and build index
        expected: status ok
        '''
        index_param = get_simple_index_params
        logging.getLogger().info(index_param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        status = connect.create_index(table, index_param)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_create_index_another(self, connect, table, get_simple_index_params):
        '''
        target: test add vector to table_2 after build index for table_1
        method: build index and add vector
        expected: status ok
        '''
        index_param = get_simple_index_params
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        status = connect.create_index(param['table_name'], index_param)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_sleep_create_index(self, connect, table, get_simple_index_params):
        '''
        target: test build index add after vector for a while
        method: add vector and build index
        expected: status ok
        '''
        index_param = get_simple_index_params
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        time.sleep(1)
        status = connect.create_index(table, index_param)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_sleep_create_index_another(self, connect, table, get_simple_index_params):
        '''
        target: test add vector to table_2 after build index for table_1 for a while
        method: build index and add vector
        expected: status ok
        '''
        index_param = get_simple_index_params
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        time.sleep(1)
        status = connect.create_index(param['table_name'], index_param)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_search_vector_add_vector(self, connect, table):
        '''
        target: test add vector after search table
        method: search table and add vector
        expected: status ok
        '''
        vector = gen_single_vector(dim)
        status, result = connect.search_vectors(table, 1, nprobe, vector)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_search_vector_add_vector_another(self, connect, table):
        '''
        target: test add vector to table_1 after search table_2
        method: search table and add vector
        expected: status ok
        '''
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        vector = gen_single_vector(dim)
        status, result = connect.search_vectors(table, 1, nprobe, vector)
        status, ids = connect.add_vectors(param['table_name'], vector)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_search_vector(self, connect, table):
        '''
        target: test search vector after add vector
        method: add vector and search table
        expected: status ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        status, result = connect.search_vectors(table, 1, nprobe, vector)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_search_vector_another(self, connect, table):
        '''
        target: test add vector to table_1 after search table_2
        method: search table and add vector
        expected: status ok
        '''
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        status, result = connect.search_vectors(param['table_name'], 1, nprobe, vector)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_sleep_search_vector(self, connect, table):
        '''
        target: test search vector after add vector after a while
        method: add vector, sleep, and search table
        expected: status ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        time.sleep(1)
        status, result = connect.search_vectors(table, 1, nprobe, vector)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_sleep_search_vector_another(self, connect, table):
        '''
        target: test add vector to table_1 after search table_2 a while
        method: search table , sleep, and add vector
        expected: status ok
        '''
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        time.sleep(1)
        status, result = connect.search_vectors(param['table_name'], 1, nprobe, vector)
        assert status.OK()

    """
    ******************************************************************
      The following cases are used to test `add_vectors` function
    ******************************************************************
    """

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vectors_ids(self, connect, table):
        '''
        target: test add vectors in table, use customize ids
        method: create table and add vectors in it, check the ids returned and the table length after vectors added
        expected: the length of ids and the table row count
        '''
        nq = 5; top_k = 1; nprobe = 1
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.add_vectors(table, vectors, ids)
        time.sleep(2)
        assert status.OK()
        assert len(ids) == nq
        # check search result
        status, result = connect.search_vectors(table, top_k, nprobe, vectors)
        logging.getLogger().info(result)
        assert len(result) == nq
        for i in range(nq):
            assert result[i][0].id == i

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vectors_twice_ids_no_ids(self, connect, table):
        '''
        target: check the result of add_vectors, with params ids and no ids
        method: test add vectors twice, use customize ids first, and then use no ids
        expected: status not OK 
        '''
        nq = 5; top_k = 1; nprobe = 1
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.add_vectors(table, vectors, ids)
        assert status.OK()
        status, ids = connect.add_vectors(table, vectors)
        logging.getLogger().info(status)
        logging.getLogger().info(ids)
        assert not status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vectors_twice_not_ids_ids(self, connect, table):
        '''
        target: check the result of add_vectors, with params ids and no ids
        method: test add vectors twice, use not ids first, and then use customize ids
        expected: status not OK 
        '''
        nq = 5; top_k = 1; nprobe = 1
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status, ids = connect.add_vectors(table, vectors, ids)
        logging.getLogger().info(status)
        logging.getLogger().info(ids)
        assert not status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vectors_ids_length_not_match(self, connect, table):
        '''
        target: test add vectors in table, use customize ids, len(ids) != len(vectors)
        method: create table and add vectors in it
        expected: raise an exception
        '''
        nq = 5
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(1, nq)]
        with pytest.raises(Exception) as e:
            status, ids = connect.add_vectors(table, vectors, ids)

    @pytest.fixture(
        scope="function",
        params=gen_invalid_vector_ids()
    )
    def get_vector_id(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_add_vectors_ids_invalid(self, connect, table, get_vector_id):
        '''
        target: test add vectors in table, use customize ids, which are not int64
        method: create table and add vectors in it
        expected: raise an exception
        '''
        nq = 5
        vectors = gen_vectors(nq, dim)
        vector_id = get_vector_id
        ids = [vector_id for _ in range(nq)]
        with pytest.raises(Exception):
            connect.add_vectors(table, vectors, ids)

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vectors(self, connect, table):
        '''
        target: test add vectors in table created before
        method: create table and add vectors in it, check the ids returned and the table length after vectors added
        expected: the length of ids and the table row count
        '''
        nq = 5
        vectors = gen_vectors(nq, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        assert len(ids) == nq

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vectors_tag(self, connect, table):
        '''
        target: test add vectors in table created before
        method: create table and add vectors in it, with the partition_tag param
        expected: the table row count equals to nq
        '''
        nq = 5
        partition_name = gen_unique_str()
        vectors = gen_vectors(nq, dim)
        status = connect.create_partition(table, partition_name, tag)
        status, ids = connect.add_vectors(table, vectors, partition_tag=tag)
        assert status.OK()
        assert len(ids) == nq

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vectors_tag_A(self, connect, table):
        '''
        target: test add vectors in table created before
        method: create partition and add vectors in it
        expected: the table row count equals to nq
        '''
        nq = 5
        partition_name = gen_unique_str()
        vectors = gen_vectors(nq, dim)
        status = connect.create_partition(table, partition_name, tag)
        status, ids = connect.add_vectors(partition_name, vectors)
        assert status.OK()
        assert len(ids) == nq

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vectors_tag_not_existed(self, connect, table):
        '''
        target: test add vectors in table created before
        method: create table and add vectors in it, with the not existed partition_tag param
        expected: status not ok
        '''
        nq = 5
        vectors = gen_vectors(nq, dim)
        status, ids = connect.add_vectors(table, vectors, partition_tag=tag)
        assert not status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vectors_tag_not_existed_A(self, connect, table):
        '''
        target: test add vectors in table created before
        method: create partition, add vectors with the not existed partition_tag param
        expected: status not ok
        '''
        nq = 5
        vectors = gen_vectors(nq, dim)
        new_tag = "new_tag"
        partition_name = gen_unique_str()
        status = connect.create_partition(table, partition_name, tag)
        status, ids = connect.add_vectors(table, vectors, partition_tag=new_tag)
        assert not status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vectors_tag_existed(self, connect, table):
        '''
        target: test add vectors in table created before
        method: create table and add vectors in it repeatly, with the partition_tag param
        expected: the table row count equals to nq
        '''
        nq = 5
        partition_name = gen_unique_str()
        vectors = gen_vectors(nq, dim)
        status = connect.create_partition(table, partition_name, tag)
        status, ids = connect.add_vectors(table, vectors, partition_tag=tag)
        for i in range(5):
            status, ids = connect.add_vectors(table, vectors, partition_tag=tag)
            assert status.OK()
            assert len(ids) == nq

    @pytest.mark.level(2)
    def test_add_vectors_without_connect(self, dis_connect, table):
        '''
        target: test add vectors without connection
        method: create table and add vectors in it, check if added successfully
        expected: raise exception
        '''
        nq = 5
        vectors = gen_vectors(nq, dim)
        with pytest.raises(Exception) as e:
            status, ids = dis_connect.add_vectors(table, vectors)

    def test_add_table_not_existed(self, connect):
        '''
        target: test add vectors in table, which not existed before
        method: add vectors table not existed, check the status
        expected: status not ok
        '''
        nq = 5
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(gen_unique_str("not_exist_table"), vector)
        assert not status.OK()
        assert not ids

    def test_add_vector_dim_not_matched(self, connect, table):
        '''
        target: test add vector, the vector dimension is not equal to the table dimension
        method: the vector dimension is half of the table dimension, check the status
        expected: status not ok
        '''
        vector = gen_single_vector(int(dim)//2)
        status, ids = connect.add_vectors(table, vector)
        assert not status.OK()

    def test_add_vectors_dim_not_matched(self, connect, table):
        '''
        target: test add vectors, the vector dimension is not equal to the table dimension
        method: the vectors dimension is half of the table dimension, check the status
        expected: status not ok
        '''
        nq = 5
        vectors = gen_vectors(nq, int(dim)//2)
        status, ids = connect.add_vectors(table, vectors)
        assert not status.OK()

    def test_add_vector_query_after_sleep(self, connect, table):
        '''
        target: test add vectors, and search it after sleep
        method: set vector[0][1] as query vectors
        expected: status ok and result length is 1
        '''
        nq = 5
        vectors = gen_vectors(nq, dim)
        status, ids = connect.add_vectors(table, vectors)
        time.sleep(3)
        status, result = connect.search_vectors(table, 1, nprobe, [vectors[0]])
        assert status.OK()
        assert len(result) == 1

    # TODO: enable
    # @pytest.mark.repeat(10)
    @pytest.mark.timeout(ADD_TIMEOUT)
    def _test_add_vector_with_multiprocessing(self, args):
        '''
        target: test add vectors, with multi processes
        method: 10 processed add vectors concurrently
        expected: status ok and result length is equal to the length off added vectors
        '''
        table = gen_unique_str()
        uri = "tcp://%s:%s" % (args["ip"], args["port"])
        param = {'table_name': table,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        milvus = get_milvus(args["handler"])
        milvus.connect(uri=uri)
        milvus.create_table(param)
        vector = gen_single_vector(dim)
        process_num = 4
        loop_num = 5
        processes = []
        def add():
            milvus = get_milvus(args["handler"])
            milvus.connect(uri=uri)
            i = 0
            while i < loop_num:
                status, ids = milvus.add_vectors(table, vector)
                i = i + 1
            milvus.disconnect()
        for i in range(process_num):
            p = Process(target=add, args=())
            processes.append(p)
            p.start()
            time.sleep(0.2)
        for p in processes:
            p.join()
        time.sleep(2)
        status, count = milvus.get_table_row_count(table)
        assert count == process_num * loop_num

    def test_add_vector_multi_tables(self, connect):
        '''
        target: test add vectors is correct or not with multiple tables of L2
        method: create 50 tables and add vectors into them in turn
        expected: status ok
        '''
        nq = 100
        vectors = gen_vectors(nq, dim)
        table_list = []
        for i in range(20):
            table_name = gen_unique_str('test_add_vector_multi_tables')
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.L2}
            connect.create_table(param)
        time.sleep(5)
        for j in range(5):
            for i in range(20):
                status, ids = connect.add_vectors(table_name=table_list[i], records=vectors)
                assert status.OK()

class TestAddIP:
    """
    ******************************************************************
      The following cases are used to test `add_vectors / index / search / delete` mixed function
    ******************************************************************
    """
    @pytest.fixture(
        scope="function",
        params=gen_simple_index_params()
    )
    def get_simple_index_params(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in cpu mode")
        if request.param["index_type"] == IndexType.IVF_PQ:
            pytest.skip("Skip PQ Temporary")
        return request.param

    def test_add_vector_create_table(self, connect, ip_table):
        '''
        target: test add vector, then create table again
        method: add vector and create table
        expected: status not ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        param = {'table_name': ip_table,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        assert not status.OK()

    def test_add_vector_has_table(self, connect, ip_table):
        '''
        target: test add vector, then check table existence
        method: add vector and call HasTable
        expected: table exists, status ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        assert assert_has_table(connect, ip_table)

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_delete_table_add_vector(self, connect, ip_table):
        '''
        target: test add vector after table deleted
        method: delete table and add vector
        expected: status not ok
        '''
        status = connect.delete_table(ip_table)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        assert not status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_delete_table_add_vector_another(self, connect, ip_table):
        '''
        target: test add vector to table_1 after table_2 deleted
        method: delete table_2 and add vector to table_1
        expected: status ok
        '''
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        status = connect.delete_table(ip_table)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(param['table_name'], vector)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_delete_table(self, connect, ip_table):
        '''
        target: test delete table after add vector
        method: add vector and delete table
        expected: status ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        status = connect.delete_table(ip_table)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_delete_another_table(self, connect, ip_table):
        '''
        target: test delete table_1 table after add vector to table_2
        method: add vector and delete table
        expected: status ok
        '''
        param = {'table_name': 'test_add_vector_delete_another_table',
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        status = connect.delete_table(param['table_name'])
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_sleep_delete_table(self, connect, ip_table):
        '''
        target: test delete table after add vector for a while
        method: add vector, sleep, and delete table
        expected: status ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        time.sleep(1)
        status = connect.delete_table(ip_table)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_sleep_delete_another_table(self, connect, ip_table):
        '''
        target: test delete table_1 table after add vector to table_2 for a while
        method: add vector , sleep, and delete table
        expected: status ok
        '''
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        time.sleep(1)
        status = connect.delete_table(param['table_name'])
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_create_index_add_vector(self, connect, ip_table, get_simple_index_params):
        '''
        target: test add vector after build index
        method: build index and add vector
        expected: status ok
        '''
        index_param = get_simple_index_params
        status = connect.create_index(ip_table, index_param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_create_index_add_vector_another(self, connect, ip_table, get_simple_index_params):
        '''
        target: test add vector to table_2 after build index for table_1
        method: build index and add vector
        expected: status ok
        '''
        index_param = get_simple_index_params
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        status = connect.create_index(ip_table, index_param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_create_index(self, connect, ip_table, get_simple_index_params):
        '''
        target: test build index add after vector
        method: add vector and build index
        expected: status ok
        '''
        index_param = get_simple_index_params
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        status, mode = connect._cmd("mode")
        assert status.OK()
        status = connect.create_index(ip_table, index_param)
        if str(mode) == "GPU" and (index_param["index_type"] == IndexType.IVF_PQ):
            assert not status.OK()
        else:
            assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_create_index_another(self, connect, ip_table, get_simple_index_params):
        '''
        target: test add vector to table_2 after build index for table_1
        method: build index and add vector
        expected: status ok
        '''
        index_param = get_simple_index_params
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        status = connect.create_index(param['table_name'], index_param)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_sleep_create_index(self, connect, ip_table, get_simple_index_params):
        '''
        target: test build index add after vector for a while
        method: add vector and build index
        expected: status ok
        '''
        index_param = get_simple_index_params
        if index_param["index_type"] == IndexType.IVF_PQ:
            pytest.skip("Skip some PQ cases")
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        time.sleep(1)
        status = connect.create_index(ip_table, index_param)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_sleep_create_index_another(self, connect, ip_table, get_simple_index_params):
        '''
        target: test add vector to table_2 after build index for table_1 for a while
        method: build index and add vector
        expected: status ok
        '''
        index_param = get_simple_index_params
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        time.sleep(1)
        status = connect.create_index(param['table_name'], index_param)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_search_vector_add_vector(self, connect, ip_table):
        '''
        target: test add vector after search table
        method: search table and add vector
        expected: status ok
        '''
        vector = gen_single_vector(dim)
        status, result = connect.search_vectors(ip_table, 1, nprobe, vector)
        status, ids = connect.add_vectors(ip_table, vector)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_search_vector_add_vector_another(self, connect, ip_table):
        '''
        target: test add vector to table_1 after search table_2
        method: search table and add vector
        expected: status ok
        '''
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        vector = gen_single_vector(dim)
        status, result = connect.search_vectors(ip_table, 1, nprobe, vector)
        status, ids = connect.add_vectors(param['table_name'], vector)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_search_vector(self, connect, ip_table):
        '''
        target: test search vector after add vector
        method: add vector and search table
        expected: status ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        status, result = connect.search_vectors(ip_table, 1, nprobe, vector)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_search_vector_another(self, connect, ip_table):
        '''
        target: test add vector to table_1 after search table_2
        method: search table and add vector
        expected: status ok
        '''
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        status, result = connect.search_vectors(param['table_name'], 1, nprobe, vector)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_sleep_search_vector(self, connect, ip_table):
        '''
        target: test search vector after add vector after a while
        method: add vector, sleep, and search table
        expected: status ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        time.sleep(1)
        status, result = connect.search_vectors(ip_table, 1, nprobe, vector)
        assert status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vector_sleep_search_vector_another(self, connect, ip_table):
        '''
        target: test add vector to table_1 after search table_2 a while
        method: search table , sleep, and add vector
        expected: status ok
        '''
        param = {'table_name': gen_unique_str(),
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        time.sleep(1)
        status, result = connect.search_vectors(param['table_name'], 1, nprobe, vector)
        assert status.OK()

    """
    ******************************************************************
      The following cases are used to test `add_vectors` function
    ******************************************************************
    """

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vectors_ids(self, connect, ip_table):
        '''
        target: test add vectors in table, use customize ids
        method: create table and add vectors in it, check the ids returned and the table length after vectors added
        expected: the length of ids and the table row count
        '''
        nq = 5; top_k = 1; nprobe = 1
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.add_vectors(ip_table, vectors, ids)
        time.sleep(2)
        assert status.OK()
        assert len(ids) == nq
        # check search result
        status, result = connect.search_vectors(ip_table, top_k, nprobe, vectors)
        logging.getLogger().info(result)
        assert len(result) == nq
        for i in range(nq):
            assert result[i][0].id == i

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vectors_twice_ids_no_ids(self, connect, ip_table):
        '''
        target: check the result of add_vectors, with params ids and no ids
        method: test add vectors twice, use customize ids first, and then use no ids
        expected: status not OK 
        '''
        nq = 5; top_k = 1; nprobe = 1
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.add_vectors(ip_table, vectors, ids)
        assert status.OK()
        status, ids = connect.add_vectors(ip_table, vectors)
        logging.getLogger().info(status)
        logging.getLogger().info(ids)
        assert not status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vectors_twice_not_ids_ids(self, connect, ip_table):
        '''
        target: check the result of add_vectors, with params ids and no ids
        method: test add vectors twice, use not ids first, and then use customize ids
        expected: status not OK 
        '''
        nq = 5; top_k = 1; nprobe = 1
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(nq)]
        status, ids = connect.add_vectors(ip_table, vectors)
        assert status.OK()
        status, ids = connect.add_vectors(ip_table, vectors, ids)
        logging.getLogger().info(status)
        logging.getLogger().info(ids)
        assert not status.OK()

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vectors_ids_length_not_match(self, connect, ip_table):
        '''
        target: test add vectors in table, use customize ids, len(ids) != len(vectors)
        method: create table and add vectors in it
        expected: raise an exception
        '''
        nq = 5
        vectors = gen_vectors(nq, dim)
        ids = [i for i in range(1, nq)]
        with pytest.raises(Exception) as e:
            status, ids = connect.add_vectors(ip_table, vectors, ids)

    @pytest.fixture(
        scope="function",
        params=gen_invalid_vector_ids()
    )
    def get_vector_id(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_add_vectors_ids_invalid(self, connect, ip_table, get_vector_id):
        '''
        target: test add vectors in table, use customize ids, which are not int64
        method: create table and add vectors in it
        expected: raise an exception
        '''
        nq = 5
        vectors = gen_vectors(nq, dim)
        vector_id = get_vector_id
        ids = [vector_id for i in range(nq)]
        with pytest.raises(Exception) as e:
            status, ids = connect.add_vectors(ip_table, vectors, ids)

    @pytest.mark.timeout(ADD_TIMEOUT)
    def test_add_vectors(self, connect, ip_table):
        '''
        target: test add vectors in table created before
        method: create table and add vectors in it, check the ids returned and the table length after vectors added
        expected: the length of ids and the table row count
        '''
        nq = 5
        vectors = gen_vectors(nq, dim)
        status, ids = connect.add_vectors(ip_table, vectors)
        assert status.OK()
        assert len(ids) == nq

    @pytest.mark.level(2)
    def test_add_vectors_without_connect(self, dis_connect, ip_table):
        '''
        target: test add vectors without connection
        method: create table and add vectors in it, check if added successfully
        expected: raise exception
        '''
        nq = 5
        vectors = gen_vectors(nq, dim)
        with pytest.raises(Exception) as e:
            status, ids = dis_connect.add_vectors(ip_table, vectors)

    def test_add_vector_dim_not_matched(self, connect, ip_table):
        '''
        target: test add vector, the vector dimension is not equal to the table dimension
        method: the vector dimension is half of the table dimension, check the status
        expected: status not ok
        '''
        vector = gen_single_vector(int(dim)//2)
        status, ids = connect.add_vectors(ip_table, vector)
        assert not status.OK()

    def test_add_vectors_dim_not_matched(self, connect, ip_table):
        '''
        target: test add vectors, the vector dimension is not equal to the table dimension
        method: the vectors dimension is half of the table dimension, check the status
        expected: status not ok
        '''
        nq = 5
        vectors = gen_vectors(nq, int(dim)//2)
        status, ids = connect.add_vectors(ip_table, vectors)
        assert not status.OK()

    def test_add_vector_query_after_sleep(self, connect, ip_table):
        '''
        target: test add vectors, and search it after sleep
        method: set vector[0][1] as query vectors
        expected: status ok and result length is 1
        '''
        nq = 5
        vectors = gen_vectors(nq, dim)
        status, ids = connect.add_vectors(ip_table, vectors)
        time.sleep(3)
        status, result = connect.search_vectors(ip_table, 1, nprobe, [vectors[0]])
        assert status.OK()
        assert len(result) == 1

    def test_add_vector_multi_tables(self, connect):
        '''
        target: test add vectors is correct or not with multiple tables of IP
        method: create 50 tables and add vectors into them in turn
        expected: status ok
        '''
        nq = 100
        vectors = gen_vectors(nq, dim)
        table_list = []
        for i in range(20):
            table_name = gen_unique_str('test_add_vector_multi_tables')
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.IP}
            connect.create_table(param)
        time.sleep(2)
        for j in range(10):
            for i in range(20):
                status, ids = connect.add_vectors(table_name=table_list[i], records=vectors)
                assert status.OK()

class TestAddAdvance:
    @pytest.fixture(
        scope="function",
        params=[
            1,
            10,
            100,
            1000,
            pytest.param(5000 - 1, marks=pytest.mark.xfail),
            pytest.param(5000, marks=pytest.mark.xfail),
            pytest.param(5000 + 1, marks=pytest.mark.xfail),
        ],
    )
    def insert_count(self, request):
        yield request.param

    def test_insert_much(self, connect, table, insert_count):
        '''
        target: test add vectors with different length of vectors
        method: set different vectors as add method params
        expected: length of ids is equal to the length of vectors
        '''
        nb = insert_count
        insert_vec_list = gen_vectors(nb, dim)
        status, ids = connect.add_vectors(table, insert_vec_list)
        assert len(ids) == nb
        assert status.OK()

    def test_insert_much_ip(self, connect, ip_table, insert_count):
        '''
        target: test add vectors with different length of vectors
        method: set different vectors as add method params
        expected: length of ids is equal to the length of vectors
        '''
        nb = insert_count
        insert_vec_list = gen_vectors(nb, dim)
        status, ids = connect.add_vectors(ip_table, insert_vec_list)
        assert len(ids) == nb
        assert status.OK()

    def test_insert_much_jaccard(self, connect, jac_table, insert_count):
        '''
        target: test add vectors with different length of vectors
        method: set different vectors as add method params
        expected: length of ids is equal to the length of vectors
        '''
        nb = insert_count
        tmp, insert_vec_list = gen_binary_vectors(nb, dim)
        status, ids = connect.add_vectors(jac_table, insert_vec_list)
        assert len(ids) == nb
        assert status.OK()

    def test_insert_much_hamming(self, connect, ham_table, insert_count):
        '''
        target: test add vectors with different length of vectors
        method: set different vectors as add method params
        expected: length of ids is equal to the length of vectors
        '''
        nb = insert_count
        tmp, insert_vec_list = gen_binary_vectors(nb, dim)
        status, ids = connect.add_vectors(ham_table, insert_vec_list)
        assert len(ids) == nb
        assert status.OK()

    def test_insert_much_tanimoto(self, connect, tanimoto_table, insert_count):
        '''
        target: test add vectors with different length of vectors
        method: set different vectors as add method params
        expected: length of ids is equal to the length of vectors
        '''
        nb = insert_count
        tmp, insert_vec_list = gen_binary_vectors(nb, dim)
        status, ids = connect.add_vectors(tanimoto_table, insert_vec_list)
        assert len(ids) == nb
        assert status.OK()


class TestNameInvalid(object):
    """
    Test adding vectors with invalid table names
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_table_names()
    )
    def get_table_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_invalid_table_names()
    )
    def get_tag_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_add_vectors_with_invalid_table_name(self, connect, get_table_name):
        table_name = get_table_name
        vectors = gen_vectors(1, dim)
        status, result = connect.add_vectors(table_name, vectors)
        assert not status.OK()

    @pytest.mark.level(2)
    def test_add_vectors_with_invalid_tag_name(self, connect, get_table_name, get_tag_name):
        table_name = get_table_name
        tag_name = get_tag_name
        vectors = gen_vectors(1, dim)
        status, result = connect.add_vectors(table_name, vectors, partition_tag=tag_name)
        assert not status.OK()


class TestAddTableVectorsInvalid(object):
    single_vector = gen_single_vector(dim)
    vectors = gen_vectors(2, dim)

    """
    Test adding vectors with invalid vectors
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_vectors()
    )
    def gen_vector(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_add_vector_with_invalid_vectors(self, connect, table, gen_vector):
        tmp_single_vector = copy.deepcopy(self.single_vector)
        tmp_single_vector[0][1] = gen_vector
        with pytest.raises(Exception) as e:
            status, result = connect.add_vectors(table, tmp_single_vector)

    @pytest.mark.level(2)
    def test_add_vectors_with_invalid_vectors(self, connect, table, gen_vector):
        tmp_vectors = copy.deepcopy(self.vectors)
        tmp_vectors[1][1] = gen_vector
        with pytest.raises(Exception) as e:
            status, result = connect.add_vectors(table, tmp_vectors)

    @pytest.mark.level(2)
    def test_add_vectors_with_invalid_vectors_jaccard(self, connect, jac_table, gen_vector):
        tmp_vectors = copy.deepcopy(self.vectors)
        tmp_vectors[1][1] = gen_vector
        with pytest.raises(Exception) as e:
            status, result = connect.add_vectors(jac_table, tmp_vectors)

    @pytest.mark.level(2)
    def test_add_vectors_with_invalid_vectors_hamming(self, connect, ham_table, gen_vector):
        tmp_vectors = copy.deepcopy(self.vectors)
        tmp_vectors[1][1] = gen_vector
        with pytest.raises(Exception) as e:
            status, result = connect.add_vectors(ham_table, tmp_vectors)