"""
   For testing index operations, including `create_index`, `describe_index` and `drop_index` interfaces
"""
import logging
import pytest
import time
import pdb
import threading
from multiprocessing import Pool, Process
import numpy
from milvus import Milvus, IndexType, MetricType
from utils import *

nb = 10000
dim = 128
index_file_size = 10
vectors = gen_vectors(nb, dim)
vectors /= numpy.linalg.norm(vectors)
vectors = vectors.tolist()
BUILD_TIMEOUT = 60
nprobe = 1


class TestIndexBase:
    @pytest.fixture(
        scope="function",
        params=gen_index_params()
    )
    def get_index_params(self, request, args):
        if "internal" not in args:
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in open source")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index_params()
    )
    def get_simple_index_params(self, request):
        yield request.param

    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, table, get_index_params):
        '''
        target: test create index interface
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_params = get_index_params
        logging.getLogger().info(index_params)
        status, ids = connect.add_vectors(table, vectors)
        status = connect.create_index(table, index_params)
        assert status.OK()

    @pytest.mark.level(2)
    def test_create_index_without_connect(self, dis_connect, table):
        '''
        target: test create index without connection
        method: create table and add vectors in it, check if added successfully
        expected: raise exception
        '''
        with pytest.raises(Exception) as e:
            status = dis_connect.create_index(table, random.choice(gen_index_params()))

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_search_with_query_vectors(self, connect, table, get_index_params):
        '''
        target: test create index interface, search with more query vectors
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_params = get_index_params
        logging.getLogger().info(index_params)
        status, ids = connect.add_vectors(table, vectors)
        status = connect.create_index(table, index_params)
        logging.getLogger().info(connect.describe_index(table))
        query_vecs = [vectors[0], vectors[1], vectors[2]]
        top_k = 5
        status, result = connect.search_vectors(table, top_k, nprobe, query_vecs)
        assert status.OK()
        assert len(result) == len(query_vecs)
        logging.getLogger().info(result)

    # TODO: enable
    @pytest.mark.timeout(BUILD_TIMEOUT)
    @pytest.mark.level(2)
    def _test_create_index_multiprocessing(self, connect, table, args):
        '''
        target: test create index interface with multiprocess
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        status, ids = connect.add_vectors(table, vectors)

        def build(connect):
            status = connect.create_index(table)
            assert status.OK()

        process_num = 8
        processes = []
        uri = "tcp://%s:%s" % (args["ip"], args["port"])

        for i in range(process_num):
            m = Milvus()
            m.connect(uri=uri)
            p = Process(target=build, args=(m,))
            processes.append(p)
            p.start()
            time.sleep(0.2)
        for p in processes:
            p.join()

        query_vec = [vectors[0]]
        top_k = 1
        status, result = connect.search_vectors(table, top_k, nprobe, query_vec)
        assert len(result) == 1
        assert len(result[0]) == top_k
        assert result[0][0].distance == 0.0

    # TODO: enable
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def _test_create_index_multiprocessing_multitable(self, connect, args):
        '''
        target: test create index interface with multiprocess
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        process_num = 8
        loop_num = 8
        processes = []

        table = []
        j = 0
        while j < (process_num*loop_num):
            table_name = gen_unique_str("test_create_index_multiprocessing")
            table.append(table_name)
            param = {'table_name': table_name,
                    'dimension': dim,
                    'index_type': IndexType.FLAT,
                    'store_raw_vector': False}
            connect.create_table(param)
            j = j + 1

        def create_index():
            i = 0
            while i < loop_num:
                # assert connect.has_table(table[ids*process_num+i])
                status, ids = connect.add_vectors(table[ids*process_num+i], vectors)

                status = connect.create_index(table[ids*process_num+i])
                assert status.OK()
                query_vec = [vectors[0]]
                top_k = 1
                status, result = connect.search_vectors(table[ids*process_num+i], top_k, nprobe, query_vec)
                assert len(result) == 1
                assert len(result[0]) == top_k
                assert result[0][0].distance == 0.0
                i = i + 1

        uri = "tcp://%s:%s" % (args["ip"], args["port"])

        for i in range(process_num):
            m = Milvus()
            m.connect(uri=uri)
            ids = i
            p = Process(target=create_index, args=(m,ids))
            processes.append(p)
            p.start()
            time.sleep(0.2)
        for p in processes:
            p.join()

    def test_create_index_table_not_existed(self, connect):
        '''
        target: test create index interface when table name not existed
        method: create table and add vectors in it, create index with an random table_name
            , make sure the table name not in index
        expected: return code not equals to 0, create index failed
        '''
        table_name = gen_unique_str(self.__class__.__name__)
        status = connect.create_index(table_name, random.choice(gen_index_params()))
        assert not status.OK()

    def test_create_index_table_None(self, connect):
        '''
        target: test create index interface when table name is None
        method: create table and add vectors in it, create index with an table_name: None
        expected: return code not equals to 0, create index failed
        '''
        table_name = None
        with pytest.raises(Exception) as e:
            status = connect.create_index(table_name, random.choice(gen_index_params()))

    def test_create_index_no_vectors(self, connect, table):
        '''
        target: test create index interface when there is no vectors in table
        method: create table and add no vectors in it, and then create index
        expected: return code equals to 0
        '''
        status = connect.create_index(table, random.choice(gen_index_params()))
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_no_vectors_then_add_vectors(self, connect, table):
        '''
        target: test create index interface when there is no vectors in table, and does not affect the subsequent process
        method: create table and add no vectors in it, and then create index, add vectors in it
        expected: return code equals to 0
        '''
        status = connect.create_index(table, random.choice(gen_index_params()))
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_same_index_repeatedly(self, connect, table):
        '''
        target: check if index can be created repeatedly, with the same create_index params
        method: create index after index have been built
        expected: return code success, and search ok
        '''
        status, ids = connect.add_vectors(table, vectors)
        index_params = random.choice(gen_index_params())
        # index_params = get_index_params
        status = connect.create_index(table, index_params)
        status = connect.create_index(table, index_params)
        assert status.OK()
        query_vec = [vectors[0]]
        top_k = 1
        status, result = connect.search_vectors(table, top_k, nprobe, query_vec)
        assert len(result) == 1
        assert len(result[0]) == top_k

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_different_index_repeatedly(self, connect, table):
        '''
        target: check if index can be created repeatedly, with the different create_index params
        method: create another index with different index_params after index have been built
        expected: return code 0, and describe index result equals with the second index params
        '''
        status, ids = connect.add_vectors(table, vectors)
        index_params = random.sample(gen_index_params(), 2)
        logging.getLogger().info(index_params)
        status = connect.create_index(table, index_params[0])
        status = connect.create_index(table, index_params[1])
        assert status.OK()
        status, result = connect.describe_index(table)
        assert result._nlist == index_params[1]["nlist"]
        assert result._table_name == table
        assert result._index_type == index_params[1]["index_type"]

    """
    ******************************************************************
      The following cases are used to test `describe_index` function
    ******************************************************************
    """

    def test_describe_index(self, connect, table, get_index_params):
        '''
        target: test describe index interface
        method: create table and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_params = get_index_params
        logging.getLogger().info(index_params)
        status, ids = connect.add_vectors(table, vectors)
        status = connect.create_index(table, index_params)
        status, result = connect.describe_index(table)
        logging.getLogger().info(result)
        assert result._nlist == index_params["nlist"]
        assert result._table_name == table
        assert result._index_type == index_params["index_type"]

    def test_describe_and_drop_index_multi_tables(self, connect, get_simple_index_params):
        '''
        target: test create, describe and drop index interface with multiple tables of L2
        method: create tables and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        nq = 100
        vectors = gen_vectors(nq, dim)
        table_list = []
        for i in range(10):
            table_name = gen_unique_str('test_create_index_multi_tables')
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.L2}
            connect.create_table(param)
            index_params = get_simple_index_params
            logging.getLogger().info(index_params)
            status, ids = connect.add_vectors(table_name=table_name, records=vectors)
            status = connect.create_index(table_name, index_params)
            assert status.OK()

        for i in range(10):
            status, result = connect.describe_index(table_list[i])
            logging.getLogger().info(result)
            assert result._nlist == index_params["nlist"]
            assert result._table_name == table_list[i]
            assert result._index_type == index_params["index_type"]

        for i in range(10):
            status = connect.drop_index(table_list[i])
            assert status.OK()
            status, result = connect.describe_index(table_list[i])
            logging.getLogger().info(result)
            assert result._nlist == 16384
            assert result._table_name == table_list[i]
            assert result._index_type == IndexType.FLAT

    @pytest.mark.level(2)
    def test_describe_index_without_connect(self, dis_connect, table):
        '''
        target: test describe index without connection
        method: describe index, and check if describe successfully
        expected: raise exception
        '''
        with pytest.raises(Exception) as e:
            status = dis_connect.describe_index(table)

    def test_describe_index_table_not_existed(self, connect):
        '''
        target: test describe index interface when table name not existed
        method: create table and add vectors in it, create index with an random table_name
            , make sure the table name not in index
        expected: return code not equals to 0, describe index failed
        '''
        table_name = gen_unique_str(self.__class__.__name__)
        status, result = connect.describe_index(table_name)
        assert not status.OK()

    def test_describe_index_table_None(self, connect):
        '''
        target: test describe index interface when table name is None
        method: create table and add vectors in it, create index with an table_name: None
        expected: return code not equals to 0, describe index failed
        '''
        table_name = None
        with pytest.raises(Exception) as e:
            status = connect.describe_index(table_name)

    def test_describe_index_not_create(self, connect, table):
        '''
        target: test describe index interface when index not created
        method: create table and add vectors in it, create index with an random table_name
            , make sure the table name not in index
        expected: return code not equals to 0, describe index failed
        '''
        status, ids = connect.add_vectors(table, vectors)
        status, result = connect.describe_index(table)
        logging.getLogger().info(result)
        assert status.OK()
        # assert result._nlist == index_params["nlist"]
        # assert result._table_name == table
        # assert result._index_type == index_params["index_type"]

    """
    ******************************************************************
      The following cases are used to test `drop_index` function
    ******************************************************************
    """

    def test_drop_index(self, connect, table, get_index_params):
        '''
        target: test drop index interface
        method: create table and add vectors in it, create index, call drop index
        expected: return code 0, and default index param
        '''
        index_params = get_index_params
        status, ids = connect.add_vectors(table, vectors)
        status = connect.create_index(table, index_params)
        assert status.OK()
        status, result = connect.describe_index(table)
        logging.getLogger().info(result)
        status = connect.drop_index(table)
        assert status.OK()
        status, result = connect.describe_index(table)
        logging.getLogger().info(result)
        assert result._nlist == 16384
        assert result._table_name == table
        assert result._index_type == IndexType.FLAT

    def test_drop_index_repeatly(self, connect, table, get_simple_index_params):
        '''
        target: test drop index repeatly
        method: create index, call drop index, and drop again
        expected: return code 0
        '''
        index_params = get_simple_index_params
        status, ids = connect.add_vectors(table, vectors)
        status = connect.create_index(table, index_params)
        assert status.OK()
        status, result = connect.describe_index(table)
        logging.getLogger().info(result)
        status = connect.drop_index(table)
        assert status.OK()
        status = connect.drop_index(table)
        assert status.OK()
        status, result = connect.describe_index(table)
        logging.getLogger().info(result)
        assert result._nlist == 16384
        assert result._table_name == table
        assert result._index_type == IndexType.FLAT

    @pytest.mark.level(2)
    def test_drop_index_without_connect(self, dis_connect, table):
        '''
        target: test drop index without connection
        method: drop index, and check if drop successfully
        expected: raise exception
        '''
        with pytest.raises(Exception) as e:
            status = dis_connect.drop_index(table)

    def test_drop_index_table_not_existed(self, connect):
        '''
        target: test drop index interface when table name not existed
        method: create table and add vectors in it, create index with an random table_name
            , make sure the table name not in index, and then drop it
        expected: return code not equals to 0, drop index failed
        '''
        table_name = gen_unique_str(self.__class__.__name__)
        status = connect.drop_index(table_name)
        assert not status.OK()

    def test_drop_index_table_None(self, connect):
        '''
        target: test drop index interface when table name is None
        method: create table and add vectors in it, create index with an table_name: None
        expected: return code not equals to 0, drop index failed
        '''
        table_name = None
        with pytest.raises(Exception) as e:
            status = connect.drop_index(table_name)

    def test_drop_index_table_not_create(self, connect, table):
        '''
        target: test drop index interface when index not created
        method: create table and add vectors in it, create index
        expected: return code not equals to 0, drop index failed
        '''
        index_params = random.choice(gen_index_params())
        logging.getLogger().info(index_params)
        status, ids = connect.add_vectors(table, vectors)
        status, result = connect.describe_index(table)
        logging.getLogger().info(result)
        # no create index
        status = connect.drop_index(table)
        logging.getLogger().info(status)
        assert status.OK()

    def test_create_drop_index_repeatly(self, connect, table, get_simple_index_params):
        '''
        target: test create / drop index repeatly, use the same index params
        method: create index, drop index, four times
        expected: return code 0
        '''
        index_params = get_simple_index_params
        status, ids = connect.add_vectors(table, vectors)
        for i in range(2):
            status = connect.create_index(table, index_params)
            assert status.OK()
            status, result = connect.describe_index(table)
            logging.getLogger().info(result)
            status = connect.drop_index(table)
            assert status.OK()
            status, result = connect.describe_index(table)
            logging.getLogger().info(result)
            assert result._nlist == 16384
            assert result._table_name == table
            assert result._index_type == IndexType.FLAT

    def test_create_drop_index_repeatly_different_index_params(self, connect, table):
        '''
        target: test create / drop index repeatly, use the different index params
        method: create index, drop index, four times, each tme use different index_params to create index
        expected: return code 0
        '''
        index_params = random.sample(gen_index_params(), 2)
        status, ids = connect.add_vectors(table, vectors)
        for i in range(2):
            status = connect.create_index(table, index_params[i])
            assert status.OK()
            status, result = connect.describe_index(table)
            logging.getLogger().info(result)
            status = connect.drop_index(table)
            assert status.OK()
            status, result = connect.describe_index(table)
            logging.getLogger().info(result)
            assert result._nlist == 16384
            assert result._table_name == table
            assert result._index_type == IndexType.FLAT


class TestIndexIP:
    @pytest.fixture(
        scope="function",
        params=gen_index_params()
    )
    def get_index_params(self, request, args):
        if "internal" not in args:
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in open source")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index_params()
    )
    def get_simple_index_params(self, request):
        yield request.param

    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, ip_table, get_index_params):
        '''
        target: test create index interface
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_params = get_index_params
        logging.getLogger().info(index_params)
        status, ids = connect.add_vectors(ip_table, vectors)
        status = connect.create_index(ip_table, index_params)
        assert status.OK()

    @pytest.mark.level(2)
    def test_create_index_without_connect(self, dis_connect, ip_table):
        '''
        target: test create index without connection
        method: create table and add vectors in it, check if added successfully
        expected: raise exception
        '''
        with pytest.raises(Exception) as e:
            status = dis_connect.create_index(ip_table, random.choice(gen_index_params()))

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_search_with_query_vectors(self, connect, ip_table, get_index_params):
        '''
        target: test create index interface, search with more query vectors
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_params = get_index_params
        logging.getLogger().info(index_params)
        status, ids = connect.add_vectors(ip_table, vectors)
        status = connect.create_index(ip_table, index_params)
        logging.getLogger().info(connect.describe_index(ip_table))
        query_vecs = [vectors[0], vectors[1], vectors[2]]
        top_k = 5
        status, result = connect.search_vectors(ip_table, top_k, nprobe, query_vecs)
        assert status.OK()
        assert len(result) == len(query_vecs)
        # logging.getLogger().info(result)

    # TODO: enable
    @pytest.mark.timeout(BUILD_TIMEOUT)
    @pytest.mark.level(2)
    def _test_create_index_multiprocessing(self, connect, ip_table, args):
        '''
        target: test create index interface with multiprocess
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        status, ids = connect.add_vectors(ip_table, vectors)

        def build(connect):
            status = connect.create_index(ip_table)
            assert status.OK()

        process_num = 8
        processes = []
        uri = "tcp://%s:%s" % (args["ip"], args["port"])

        for i in range(process_num):
            m = Milvus()
            m.connect(uri=uri)
            p = Process(target=build, args=(m,))
            processes.append(p)
            p.start()
            time.sleep(0.2)
        for p in processes:
            p.join()

        query_vec = [vectors[0]]
        top_k = 1
        status, result = connect.search_vectors(ip_table, top_k, nprobe, query_vec)
        assert len(result) == 1
        assert len(result[0]) == top_k
        assert result[0][0].distance == 0.0

    # TODO: enable
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def _test_create_index_multiprocessing_multitable(self, connect, args):
        '''
        target: test create index interface with multiprocess
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        process_num = 8
        loop_num = 8
        processes = []

        table = []
        j = 0
        while j < (process_num*loop_num):
            table_name = gen_unique_str("test_create_index_multiprocessing")
            table.append(table_name)
            param = {'table_name': table_name,
                    'dimension': dim}
            connect.create_table(param)
            j = j + 1

        def create_index():
            i = 0
            while i < loop_num:
                # assert connect.has_table(table[ids*process_num+i])
                status, ids = connect.add_vectors(table[ids*process_num+i], vectors)

                status = connect.create_index(table[ids*process_num+i])
                assert status.OK()
                query_vec = [vectors[0]]
                top_k = 1
                status, result = connect.search_vectors(table[ids*process_num+i], top_k, nprobe, query_vec)
                assert len(result) == 1
                assert len(result[0]) == top_k
                assert result[0][0].distance == 0.0
                i = i + 1

        uri = "tcp://%s:%s" % (args["ip"], args["port"])

        for i in range(process_num):
            m = Milvus()
            m.connect(uri=uri)
            ids = i
            p = Process(target=create_index, args=(m,ids))
            processes.append(p)
            p.start()
            time.sleep(0.2)
        for p in processes:
            p.join()

    def test_create_index_no_vectors(self, connect, ip_table):
        '''
        target: test create index interface when there is no vectors in table
        method: create table and add no vectors in it, and then create index
        expected: return code equals to 0
        '''
        status = connect.create_index(ip_table, random.choice(gen_index_params()))
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_no_vectors_then_add_vectors(self, connect, ip_table):
        '''
        target: test create index interface when there is no vectors in table, and does not affect the subsequent process
        method: create table and add no vectors in it, and then create index, add vectors in it
        expected: return code equals to 0
        '''
        status = connect.create_index(ip_table, random.choice(gen_index_params()))
        status, ids = connect.add_vectors(ip_table, vectors)
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_same_index_repeatedly(self, connect, ip_table):
        '''
        target: check if index can be created repeatedly, with the same create_index params
        method: create index after index have been built
        expected: return code success, and search ok
        '''
        status, ids = connect.add_vectors(ip_table, vectors)
        index_params = random.choice(gen_index_params())
        # index_params = get_index_params
        status = connect.create_index(ip_table, index_params)
        status = connect.create_index(ip_table, index_params)
        assert status.OK()
        query_vec = [vectors[0]]
        top_k = 1
        status, result = connect.search_vectors(ip_table, top_k, nprobe, query_vec)
        assert len(result) == 1
        assert len(result[0]) == top_k

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_different_index_repeatedly(self, connect, ip_table):
        '''
        target: check if index can be created repeatedly, with the different create_index params
        method: create another index with different index_params after index have been built
        expected: return code 0, and describe index result equals with the second index params
        '''
        status, ids = connect.add_vectors(ip_table, vectors)
        index_params = random.sample(gen_index_params(), 2)
        logging.getLogger().info(index_params)
        status = connect.create_index(ip_table, index_params[0])
        status = connect.create_index(ip_table, index_params[1])
        assert status.OK()
        status, result = connect.describe_index(ip_table)
        assert result._nlist == index_params[1]["nlist"]
        assert result._table_name == ip_table
        assert result._index_type == index_params[1]["index_type"]

    """
    ******************************************************************
      The following cases are used to test `describe_index` function
    ******************************************************************
    """

    def test_describe_index(self, connect, ip_table, get_index_params):
        '''
        target: test describe index interface
        method: create table and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_params = get_index_params
        logging.getLogger().info(index_params)
        status, ids = connect.add_vectors(ip_table, vectors)
        status = connect.create_index(ip_table, index_params)
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        assert result._nlist == index_params["nlist"]
        assert result._table_name == ip_table
        assert result._index_type == index_params["index_type"]

    def test_describe_and_drop_index_multi_tables(self, connect, get_simple_index_params):
        '''
        target: test create, describe and drop index interface with multiple tables of IP
        method: create tables and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        nq = 100
        vectors = gen_vectors(nq, dim)
        table_list = []
        for i in range(10):
            table_name = gen_unique_str('test_create_index_multi_tables')
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.IP}
            connect.create_table(param)
            index_params = get_simple_index_params
            logging.getLogger().info(index_params)
            status, ids = connect.add_vectors(table_name=table_name, records=vectors)
            status = connect.create_index(table_name, index_params)
            assert status.OK()

        for i in range(10):
            status, result = connect.describe_index(table_list[i])
            logging.getLogger().info(result)
            assert result._nlist == index_params["nlist"]
            assert result._table_name == table_list[i]
            assert result._index_type == index_params["index_type"]

        for i in range(10):
            status = connect.drop_index(table_list[i])
            assert status.OK()
            status, result = connect.describe_index(table_list[i])
            logging.getLogger().info(result)
            assert result._nlist == 16384
            assert result._table_name == table_list[i]
            assert result._index_type == IndexType.FLAT

    @pytest.mark.level(2)
    def test_describe_index_without_connect(self, dis_connect, ip_table):
        '''
        target: test describe index without connection
        method: describe index, and check if describe successfully
        expected: raise exception
        '''
        with pytest.raises(Exception) as e:
            status = dis_connect.describe_index(ip_table)

    def test_describe_index_not_create(self, connect, ip_table):
        '''
        target: test describe index interface when index not created
        method: create table and add vectors in it, create index with an random table_name
            , make sure the table name not in index
        expected: return code not equals to 0, describe index failed
        '''
        status, ids = connect.add_vectors(ip_table, vectors)
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        assert status.OK()
        # assert result._nlist == index_params["nlist"]
        # assert result._table_name == table
        # assert result._index_type == index_params["index_type"]

    """
    ******************************************************************
      The following cases are used to test `drop_index` function
    ******************************************************************
    """

    def test_drop_index(self, connect, ip_table, get_index_params):
        '''
        target: test drop index interface
        method: create table and add vectors in it, create index, call drop index
        expected: return code 0, and default index param
        '''
        index_params = get_index_params
        status, ids = connect.add_vectors(ip_table, vectors)
        status = connect.create_index(ip_table, index_params)
        assert status.OK()
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        status = connect.drop_index(ip_table)
        assert status.OK()
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        assert result._nlist == 16384
        assert result._table_name == ip_table
        assert result._index_type == IndexType.FLAT

    def test_drop_index_repeatly(self, connect, ip_table, get_simple_index_params):
        '''
        target: test drop index repeatly
        method: create index, call drop index, and drop again
        expected: return code 0
        '''
        index_params = get_simple_index_params
        status, ids = connect.add_vectors(ip_table, vectors)
        status = connect.create_index(ip_table, index_params)
        assert status.OK()
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        status = connect.drop_index(ip_table)
        assert status.OK()
        status = connect.drop_index(ip_table)
        assert status.OK()
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        assert result._nlist == 16384
        assert result._table_name == ip_table
        assert result._index_type == IndexType.FLAT

    @pytest.mark.level(2)
    def test_drop_index_without_connect(self, dis_connect, ip_table):
        '''
        target: test drop index without connection
        method: drop index, and check if drop successfully
        expected: raise exception
        '''
        with pytest.raises(Exception) as e:
            status = dis_connect.drop_index(ip_table, random.choice(gen_index_params()))

    def test_drop_index_table_not_create(self, connect, ip_table):
        '''
        target: test drop index interface when index not created
        method: create table and add vectors in it, create index
        expected: return code not equals to 0, drop index failed
        '''
        index_params = random.choice(gen_index_params())
        logging.getLogger().info(index_params)
        status, ids = connect.add_vectors(ip_table, vectors)
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        # no create index
        status = connect.drop_index(ip_table)
        logging.getLogger().info(status)
        assert status.OK()

    def test_create_drop_index_repeatly(self, connect, ip_table, get_simple_index_params):
        '''
        target: test create / drop index repeatly, use the same index params
        method: create index, drop index, four times
        expected: return code 0
        '''
        index_params = get_simple_index_params
        status, ids = connect.add_vectors(ip_table, vectors)
        for i in range(2):
            status = connect.create_index(ip_table, index_params)
            assert status.OK()
            status, result = connect.describe_index(ip_table)
            logging.getLogger().info(result)
            status = connect.drop_index(ip_table)
            assert status.OK()
            status, result = connect.describe_index(ip_table)
            logging.getLogger().info(result)
            assert result._nlist == 16384
            assert result._table_name == ip_table
            assert result._index_type == IndexType.FLAT

    def test_create_drop_index_repeatly_different_index_params(self, connect, ip_table):
        '''
        target: test create / drop index repeatly, use the different index params
        method: create index, drop index, four times, each tme use different index_params to create index
        expected: return code 0
        '''
        index_params = random.sample(gen_index_params(), 2)
        status, ids = connect.add_vectors(ip_table, vectors)
        for i in range(2):
            status = connect.create_index(ip_table, index_params[i])
            assert status.OK()
            status, result = connect.describe_index(ip_table)
            assert result._nlist == index_params[i]["nlist"]
            assert result._table_name == ip_table
            assert result._index_type == index_params[i]["index_type"]
            status, result = connect.describe_index(ip_table)
            logging.getLogger().info(result)
            status = connect.drop_index(ip_table)
            assert status.OK()
            status, result = connect.describe_index(ip_table)
            logging.getLogger().info(result)
            assert result._nlist == 16384
            assert result._table_name == ip_table
            assert result._index_type == IndexType.FLAT


class TestIndexTableInvalid(object):
    """
    Test create / describe / drop index interfaces with invalid table names
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_table_names()
    )
    def get_table_name(self, request):
        yield request.param

    # @pytest.mark.level(1)
    def test_create_index_with_invalid_tablename(self, connect, get_table_name):
        table_name = get_table_name
        status = connect.create_index(table_name, random.choice(gen_index_params()))
        assert not status.OK()

    # @pytest.mark.level(1)
    def test_describe_index_with_invalid_tablename(self, connect, get_table_name):
        table_name = get_table_name
        status, result = connect.describe_index(table_name)
        assert not status.OK()   

    # @pytest.mark.level(1)
    def test_drop_index_with_invalid_tablename(self, connect, get_table_name):
        table_name = get_table_name
        status = connect.drop_index(table_name)
        assert not status.OK()


class TestCreateIndexParamsInvalid(object):
    """
    Test Building index with invalid table names, table names not in db
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_index_params()
    )
    def get_index_params(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_create_index_with_invalid_index_params(self, connect, table, get_index_params):
        index_params = get_index_params
        index_type = index_params["index_type"]
        nlist = index_params["nlist"]
        logging.getLogger().info(index_params)
        status, ids = connect.add_vectors(table, vectors)
        # if not isinstance(index_type, int) or not isinstance(nlist, int):
        with pytest.raises(Exception) as e:
            status = connect.create_index(table, index_params)
        # else:
        #     status = connect.create_index(table, index_params)
        #     assert not status.OK()
