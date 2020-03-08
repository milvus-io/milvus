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
import sklearn.preprocessing
from milvus import IndexType, MetricType
from utils import *

nb = 6000
dim = 128
index_file_size = 10
vectors = gen_vectors(nb, dim)
vectors = sklearn.preprocessing.normalize(vectors, axis=1, norm='l2')
vectors = vectors.tolist()
BUILD_TIMEOUT = 300
nprobe = 1
tag = "1970-01-01"
NLIST = 4046
INVALID_NLIST = 100000000


class TestIndexBase:
    @pytest.fixture(
        scope="function",
        params=gen_index()
    )
    def get_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] == IndexType.IVF_PQ:
                pytest.skip("ivfpq not support in GPU mode")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] == IndexType.IVF_PQ:
                pytest.skip("ivfpq not support in GPU mode")
        return request.param

    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, table, get_simple_index):
        '''
        target: test create index interface
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status, ids = connect.add_vectors(table, vectors)
        status = connect.create_index(table, index_type, index_param)
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_no_vectors(self, connect, table, get_simple_index):
        '''
        target: test create index interface
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status = connect.create_index(table, index_type, index_param)
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition(self, connect, table, get_simple_index):
        '''
        target: test create index interface
        method: create table, create partition, and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(table, tag)
        status, ids = connect.add_vectors(table, vectors, partition_tag=tag)
        status = connect.create_index(table, index_type, index_param)
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition_flush(self, connect, table, get_simple_index):
        '''
        target: test create index interface
        method: create table, create partition, and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(table, tag)
        status, ids = connect.add_vectors(table, vectors, partition_tag=tag)
        connect.flush()
        status = connect.create_index(table, index_type, index_param)
        assert status.OK()

    @pytest.mark.level(2)
    def test_create_index_without_connect(self, dis_connect, table):
        '''
        target: test create index without connection
        method: create table and add vectors in it, check if added successfully
        expected: raise exception
        '''
        nlist = NLIST
        index_type = IndexType.IVF_SQ8
        index_param = {"nlist": nlist}
        with pytest.raises(Exception) as e:
            status = dis_connect.create_index(table, index_type, index_param)

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_search_with_query_vectors(self, connect, table, get_simple_index):
        '''
        target: test create index interface, search with more query vectors
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        status, ids = connect.add_vectors(table, vectors)
        status = connect.create_index(table, index_type, index_param)
        logging.getLogger().info(connect.describe_index(table))
        query_vecs = [vectors[0], vectors[1], vectors[2]]
        top_k = 5
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(table, top_k, query_vecs, params=search_param)
        assert status.OK()
        assert len(result) == len(query_vecs)
        logging.getLogger().info(result)

    @pytest.mark.timeout(BUILD_TIMEOUT)
    @pytest.mark.level(2)
    def test_create_index_multithread(self, connect, table, args):
        '''
        target: test create index interface with multiprocess
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        status, ids = connect.add_vectors(table, vectors)

        def build(connect):
            status = connect.create_index(table, IndexType.IVFLAT, {"nlist": NLIST})
            assert status.OK()

        threads_num = 8
        threads = []
        uri = "tcp://%s:%s" % (args["ip"], args["port"])

        for i in range(threads_num):
            m = get_milvus(args["handler"])
            m.connect(uri=uri)
            t = threading.Thread(target=build, args=(m,))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

        query_vec = [vectors[0]]
        top_k = 1
        search_param = {"nprobe": nprobe}
        status, result = connect.search_vectors(table, top_k, query_vec, params=search_param)
        assert len(result) == 1
        assert len(result[0]) == top_k
        assert result[0][0].distance == 0.0

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_multithread_multitable(self, connect, args):
        '''
        target: test create index interface with multiprocess
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        threads_num = 8
        loop_num = 8
        threads = []

        table = []
        j = 0
        while j < (threads_num*loop_num):
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
                status, ids = connect.add_vectors(table[ids*threads_num+i], vectors)

                status = connect.create_index(table[ids*threads_num+i], IndexType.IVFLAT, {"nlist": NLIST})
                assert status.OK()
                query_vec = [vectors[0]]
                top_k = 1
                search_param = {"nprobe": nprobe}
                status, result = connect.search_vectors(table[ids*threads_num+i], top_k, query_vec, params=search_param)
                assert len(result) == 1
                assert len(result[0]) == top_k
                assert result[0][0].distance == 0.0
                i = i + 1

        uri = "tcp://%s:%s" % (args["ip"], args["port"])

        for i in range(threads_num):
            m = get_milvus(args["handler"])
            m.connect(uri=uri)
            ids = i
            t = threading.Thread(target=create_index, args=(m,ids))
            threads.append(t)
            t.start()
            time.sleep(0.2)
        for t in threads:
            t.join()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    @pytest.mark.level(2)
    def test_create_index_a_multithreads(self, connect, table, args):
        status, ids = connect.add_vectors(table, vectors)
        def build(connect):
            status = connect.create_index(table, IndexType.IVFLAT, {"nlist": NLIST})
            assert status.OK()
        def count(connect):
            status, count = connect.get_table_row_count(table)
            assert status.OK()
            assert count == nb

        threads_num = 8
        threads = []
        uri = "tcp://%s:%s" % (args["ip"], args["port"])
        for i in range(threads_num):
            m = get_milvus(args["handler"])
            m.connect(uri=uri)
            if(i % 2 == 0):
                p = threading.Thread(target=build, args=(m,))
            else:
                p = threading.Thread(target=count, args=(m,))
            threads.append(p)
            p.start()
            time.sleep(0.2)
        for p in threads:
            p.join()


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
            status = connect.create_index(table, IndexType.IVFLAT, {"nlist": NLIST})
            assert status.OK()

        process_num = 8
        processes = []
        uri = "tcp://%s:%s" % (args["ip"], args["port"])

        for i in range(process_num):
            m = get_milvus(args["handler"])
            m.connect(uri=uri)
            p = Process(target=build, args=(m,))
            processes.append(p)
            p.start()
            time.sleep(0.2)
        for p in processes:
            p.join()

        query_vec = [vectors[0]]
        top_k = 1
        search_param = {"nprobe": nprobe}
        status, result = connect.search_vectors(table, top_k, query_vec, params=search_param)
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

                status = connect.create_index(table[ids*process_num+i], IndexType.IVFLAT, {"nlist": NLIST})
                assert status.OK()
                query_vec = [vectors[0]]
                top_k = 1
                search_param = {"nprobe": nprobe}
                status, result = connect.search_vectors(table[ids*process_num+i], top_k, query_vec, params=search_param)
                assert len(result) == 1
                assert len(result[0]) == top_k
                assert result[0][0].distance == 0.0
                i = i + 1

        uri = "tcp://%s:%s" % (args["ip"], args["port"])

        for i in range(process_num):
            m = get_milvus(args["handler"])
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
        method: create table and add vectors in it, create index
            , make sure the table name not in index
        expected: return code not equals to 0, create index failed
        '''
        table_name = gen_unique_str(self.__class__.__name__)
        nlist = NLIST
        index_type = IndexType.IVF_SQ8
        index_param = {"nlist": nlist}
        status = connect.create_index(table_name, index_type, index_param)
        assert not status.OK()

    def test_create_index_table_None(self, connect):
        '''
        target: test create index interface when table name is None
        method: create table and add vectors in it, create index with an table_name: None
        expected: return code not equals to 0, create index failed
        '''
        table_name = None
        nlist = NLIST
        index_type = IndexType.IVF_SQ8
        index_param = {"nlist": nlist}
        with pytest.raises(Exception) as e:
            status = connect.create_index(table_name, index_type, index_param)

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_no_vectors_then_add_vectors(self, connect, table, get_simple_index):
        '''
        target: test create index interface when there is no vectors in table, and does not affect the subsequent process
        method: create table and add no vectors in it, and then create index, add vectors in it
        expected: return code equals to 0
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status = connect.create_index(table, index_type, index_param)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_same_index_repeatedly(self, connect, table, get_simple_index):
        '''
        target: check if index can be created repeatedly, with the same create_index params
        method: create index after index have been built
        expected: return code success, and search ok
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status = connect.create_index(table, index_type, index_param)
        status = connect.create_index(table, index_type, index_param)
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_different_index_repeatedly(self, connect, table):
        '''
        target: check if index can be created repeatedly, with the different create_index params
        method: create another index with different index_params after index have been built
        expected: return code 0, and describe index result equals with the second index params
        '''
        nlist = NLIST
        status, ids = connect.add_vectors(table, vectors)
        index_type_1 = IndexType.IVF_SQ8
        index_type_2 = IndexType.IVFLAT
        indexs = [{"index_type": index_type_1, "index_param": {"nlist": nlist}}, {"index_type": index_type_2, "index_param": {"nlist": nlist}}]
        logging.getLogger().info(indexs)
        for index in indexs:
            status = connect.create_index(table, index["index_type"], index["index_param"])
            assert status.OK()
        status, result = connect.describe_index(table)
        assert result._params["nlist"] == nlist
        assert result._table_name == table
        assert result._index_type == index_type_2

    """
    ******************************************************************
      The following cases are used to test `describe_index` function
    ******************************************************************
    """

    def test_describe_index(self, connect, table, get_index):
        '''
        target: test describe index interface
        method: create table and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_param = get_index["index_param"]
        index_type = get_index["index_type"]
        logging.getLogger().info(get_index)
        # status, ids = connect.add_vectors(table, vectors)
        status = connect.create_index(table, index_type, index_param)
        status, result = connect.describe_index(table)
        logging.getLogger().info(result)
        assert result._params == index_param
        assert result._table_name == table
        assert result._index_type == index_type

    def test_describe_and_drop_index_multi_tables(self, connect, get_simple_index):
        '''
        target: test create, describe and drop index interface with multiple tables of L2
        method: create tables and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        nq = 100
        vectors = gen_vectors(nq, dim)
        table_list = []
        for i in range(10):
            table_name = gen_unique_str()
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.L2}
            connect.create_table(param)
            index_param = get_simple_index["index_param"]
            index_type = get_simple_index["index_type"]
            logging.getLogger().info(get_simple_index)
            status, ids = connect.add_vectors(table_name=table_name, records=vectors)
            status = connect.create_index(table_name, index_type, index_param)
            assert status.OK()

        for i in range(10):
            status, result = connect.describe_index(table_list[i])
            logging.getLogger().info(result)
            assert result._params == index_param
            assert result._table_name == table_list[i]
            assert result._index_type == index_type

        for i in range(10):
            status = connect.drop_index(table_list[i])
            assert status.OK()
            status, result = connect.describe_index(table_list[i])
            logging.getLogger().info(result)
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
        method: create table and add vectors in it, create index
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
        method: create table and add vectors in it, create index
            , make sure the table name not in index
        expected: return code not equals to 0, describe index failed
        '''
        status, ids = connect.add_vectors(table, vectors)
        status, result = connect.describe_index(table)
        logging.getLogger().info(result)
        assert status.OK()
        # assert result._params["nlist"] == index_params["nlist"]
        # assert result._table_name == table
        # assert result._index_type == index_params["index_type"]

    """
    ******************************************************************
      The following cases are used to test `drop_index` function
    ******************************************************************
    """

    def test_drop_index(self, connect, table, get_simple_index):
        '''
        target: test drop index interface
        method: create table and add vectors in it, create index, call drop index
        expected: return code 0, and default index param
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        # status, ids = connect.add_vectors(table, vectors)
        status = connect.create_index(table, index_type, index_param)
        assert status.OK()
        status, result = connect.describe_index(table)
        logging.getLogger().info(result)
        status = connect.drop_index(table)
        assert status.OK()
        status, result = connect.describe_index(table)
        logging.getLogger().info(result)
        assert result._table_name == table
        assert result._index_type == IndexType.FLAT

    def test_drop_index_repeatly(self, connect, table, get_simple_index):
        '''
        target: test drop index repeatly
        method: create index, call drop index, and drop again
        expected: return code 0
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        # status, ids = connect.add_vectors(table, vectors)
        status = connect.create_index(table, index_type, index_param)
        assert status.OK()
        status, result = connect.describe_index(table)
        logging.getLogger().info(result)
        status = connect.drop_index(table)
        assert status.OK()
        status = connect.drop_index(table)
        assert status.OK()
        status, result = connect.describe_index(table)
        logging.getLogger().info(result)
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
        method: create table and add vectors in it, create index
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
        status, ids = connect.add_vectors(table, vectors)
        status, result = connect.describe_index(table)
        logging.getLogger().info(result)
        # no create index
        status = connect.drop_index(table)
        logging.getLogger().info(status)
        assert status.OK()

    def test_create_drop_index_repeatly(self, connect, table, get_simple_index):
        '''
        target: test create / drop index repeatly, use the same index params
        method: create index, drop index, four times
        expected: return code 0
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        # status, ids = connect.add_vectors(table, vectors)
        for i in range(2):
            status = connect.create_index(table, index_type, index_param)
            assert status.OK()
            status, result = connect.describe_index(table)
            logging.getLogger().info(result)
            status = connect.drop_index(table)
            assert status.OK()
            status, result = connect.describe_index(table)
            logging.getLogger().info(result)
            assert result._table_name == table
            assert result._index_type == IndexType.FLAT

    def test_create_drop_index_repeatly_different_index_params(self, connect, table):
        '''
        target: test create / drop index repeatly, use the different index params
        method: create index, drop index, four times, each tme use different index_params to create index
        expected: return code 0
        '''
        nlist = NLIST
        indexs = [{"index_type": IndexType.IVFLAT, "index_param": {"nlist": nlist}}, {"index_type": IndexType.IVF_SQ8, "index_param": {"nlist": nlist}}]
        # status, ids = connect.add_vectors(table, vectors)
        for i in range(2):
            status = connect.create_index(table, indexs[i]["index_type"], indexs[i]["index_param"])
            assert status.OK()
            status, result = connect.describe_index(table)
            logging.getLogger().info(result)
            status = connect.drop_index(table)
            assert status.OK()
            status, result = connect.describe_index(table)
            logging.getLogger().info(result)
            assert result._table_name == table
            assert result._index_type == IndexType.FLAT


class TestIndexIP:
    @pytest.fixture(
        scope="function",
        params=gen_index()
    )
    def get_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] == IndexType.IVF_PQ:
                pytest.skip("ivfpq not support in GPU mode")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] == IndexType.IVF_PQ:
                pytest.skip("ivfpq not support in GPU mode")
        return request.param
    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """
    @pytest.mark.level(2)
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, ip_table, get_simple_index):
        '''
        target: test create index interface
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        if index_type in [IndexType.RNSG]:
            pytest.skip("Skip some RNSG cases")
        status, ids = connect.add_vectors(ip_table, vectors)
        status = connect.create_index(ip_table, index_type, index_param)
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_table(self, connect, ip_table, get_simple_index):
        '''
        target: test create index interface
        method: create table, create partition, and add vectors in it, create index on table
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        if index_type in [IndexType.RNSG]:
            pytest.skip("Skip some RNSG cases")
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(ip_table, tag)
        status, ids = connect.add_vectors(ip_table, vectors, partition_tag=tag)
        status = connect.create_index(ip_table, index_type, index_param)
        assert status.OK()

    @pytest.mark.level(2)
    def test_create_index_without_connect(self, dis_connect, ip_table):
        '''
        target: test create index without connection
        method: create table and add vectors in it, check if added successfully
        expected: raise exception
        '''
        nlist = NLIST
        index_type = IndexType.IVF_SQ8
        index_param = {"nlist": nlist}
        with pytest.raises(Exception) as e:
            status = dis_connect.create_index(ip_table, index_type, index_param)

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_search_with_query_vectors(self, connect, ip_table, get_simple_index):
        '''
        target: test create index interface, search with more query vectors
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        if index_type in [IndexType.RNSG]:
            pytest.skip("Skip some RNSG cases")
        status, ids = connect.add_vectors(ip_table, vectors)
        status = connect.create_index(ip_table, index_type, index_param)
        logging.getLogger().info(connect.describe_index(ip_table))
        query_vecs = [vectors[0], vectors[1], vectors[2]]
        top_k = 5
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(ip_table, top_k, query_vecs, params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result) == len(query_vecs)

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
            status = connect.create_index(ip_table, IndexType.IVFLAT, {"nlist": NLIST})
            assert status.OK()

        process_num = 8
        processes = []
        uri = "tcp://%s:%s" % (args["ip"], args["port"])

        for i in range(process_num):
            m = get_milvus(args["handler"])
            m.connect(uri=uri)
            p = Process(target=build, args=(m,))
            processes.append(p)
            p.start()
            time.sleep(0.2)
        for p in processes:
            p.join()

        query_vec = [vectors[0]]
        top_k = 1
        search_param = {"nprobe": nprobe}
        status, result = connect.search_vectors(ip_table, top_k, query_vec, params=search_param)
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

                status = connect.create_index(table[ids*process_num+i], IndexType.IVFLAT, {"nlist": NLIST})
                assert status.OK()
                query_vec = [vectors[0]]
                top_k = 1
                search_param = {"nprobe": nprobe}
                status, result = connect.search_vectors(table[ids*process_num+i], top_k, query_vec, params=search_param)
                assert len(result) == 1
                assert len(result[0]) == top_k
                assert result[0][0].distance == 0.0
                i = i + 1

        uri = "tcp://%s:%s" % (args["ip"], args["port"])

        for i in range(process_num):
            m = get_milvus(args["handler"])
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
        nlist = NLIST
        index_type = IndexType.IVF_SQ8
        index_param = {"nlist": nlist}
        status = connect.create_index(ip_table, index_type, index_param)
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_no_vectors_then_add_vectors(self, connect, ip_table, get_simple_index):
        '''
        target: test create index interface when there is no vectors in table, and does not affect the subsequent process
        method: create table and add no vectors in it, and then create index, add vectors in it
        expected: return code equals to 0
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        if index_type in [IndexType.RNSG]:
            pytest.skip("Skip some RNSG cases")
        status = connect.create_index(ip_table, index_type, index_param)
        status, ids = connect.add_vectors(ip_table, vectors)
        assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_same_index_repeatedly(self, connect, ip_table):
        '''
        target: check if index can be created repeatedly, with the same create_index params
        method: create index after index have been built
        expected: return code success, and search ok
        '''
        nlist = NLIST
        status, ids = connect.add_vectors(ip_table, vectors)
        index_type = IndexType.IVF_SQ8
        index_param = {"nlist": nlist}
        status = connect.create_index(ip_table, index_type, index_param)
        status = connect.create_index(ip_table, index_type, index_param)
        assert status.OK()
        query_vec = [vectors[0]]
        top_k = 1
        search_param = {"nprobe": nprobe}
        status, result = connect.search_vectors(ip_table, top_k, query_vec, params=search_param)
        assert len(result) == 1
        assert len(result[0]) == top_k

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_different_index_repeatedly(self, connect, ip_table):
        '''
        target: check if index can be created repeatedly, with the different create_index params
        method: create another index with different index_params after index have been built
        expected: return code 0, and describe index result equals with the second index params
        '''
        nlist = NLIST
        status, ids = connect.add_vectors(ip_table, vectors)
        index_type_1 = IndexType.IVF_SQ8
        index_type_2 = IndexType.IVFLAT
        indexs = [{"index_type": index_type_1, "index_param": {"nlist": nlist}}, {"index_type": index_type_2, "index_param": {"nlist": nlist}}]
        logging.getLogger().info(indexs)
        for index in indexs:
            status = connect.create_index(ip_table, index["index_type"], index["index_param"])
            assert status.OK()
        status, result = connect.describe_index(ip_table)
        assert result._params["nlist"] == nlist
        assert result._table_name == ip_table
        assert result._index_type == index_type_2

    """
    ******************************************************************
      The following cases are used to test `describe_index` function
    ******************************************************************
    """

    def test_describe_index(self, connect, ip_table, get_simple_index):
        '''
        target: test describe index interface
        method: create table and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        logging.getLogger().info(get_simple_index)
        if index_type in [IndexType.RNSG]:
            pytest.skip()
    # status, ids = connect.add_vectors(ip_table, vectors[:5000])
        status = connect.create_index(ip_table, index_type, index_param)
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        assert result._table_name == ip_table
        status, mode = connect._cmd("mode")
        if str(mode) == "GPU" and index_type == IndexType.IVF_PQ:
            assert result._index_type == IndexType.FLAT
            assert result._params["nlist"] == NLIST
        else:
            assert result._index_type == index_type
            assert result._params == index_param

    def test_describe_index_partition(self, connect, ip_table, get_simple_index):
        '''
        target: test describe index interface
        method: create table, create partition and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        if index_type in [IndexType.RNSG]:
            pytest.skip("Skip some RNSG cases")
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(ip_table, tag)
        status, ids = connect.add_vectors(ip_table, vectors, partition_tag=tag)
        status = connect.create_index(ip_table, index_type, index_param)
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        assert result._params == index_param
        assert result._table_name == ip_table
        assert result._index_type == index_type

    def test_describe_index_partition_A(self, connect, ip_table, get_simple_index):
        '''
        target: test describe index interface
        method: create table, create partitions and add vectors in it, create index on partitions, call describe index
        expected: return code 0, and index instructure
        '''
        new_tag = "new_tag"
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        if index_type in [IndexType.RNSG]:
            pytest.skip("Skip some RNSG cases")
        logging.getLogger().info(get_simple_index)
        status = connect.create_partition(ip_table, tag)
        status = connect.create_partition(ip_table, new_tag)
        status, ids = connect.add_vectors(ip_table, vectors, partition_tag=tag)
        status, ids = connect.add_vectors(ip_table, vectors, partition_tag=new_tag)
        status = connect.create_index(ip_table, index_type, index_param)
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        assert result._params == index_param
        assert result._table_name == ip_table
        assert result._index_type == index_type

    def test_describe_and_drop_index_multi_tables(self, connect, get_simple_index):
        '''
        target: test create, describe and drop index interface with multiple tables of IP
        method: create tables and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        nq = 100
        vectors = gen_vectors(nq, dim)
        table_list = []
        for i in range(10):
            table_name = gen_unique_str()
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.IP}
            connect.create_table(param)
            index_param = get_simple_index["index_param"]
            index_type = get_simple_index["index_type"]
            if index_type in [IndexType.RNSG]:
                pytest.skip("Skip some RNSG cases")
            logging.getLogger().info(get_simple_index)
            status, ids = connect.add_vectors(table_name=table_name, records=vectors)
            status = connect.create_index(table_name, index_type, index_param)
            assert status.OK()
        for i in range(10):
            status, result = connect.describe_index(table_list[i])
            logging.getLogger().info(result)
            assert result._params == index_param
            assert result._table_name == table_list[i]
            assert result._index_type == index_type
        for i in range(10):
            status = connect.drop_index(table_list[i])
            assert status.OK()
            status, result = connect.describe_index(table_list[i])
            logging.getLogger().info(result)
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
        method: create table and add vectors in it, create index
            , make sure the table name not in index
        expected: return code not equals to 0, describe index failed
        '''
        status, ids = connect.add_vectors(ip_table, vectors)
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        assert status.OK()
        # assert result._params["nlist"] == index_params["nlist"]
        # assert result._table_name == table
        # assert result._index_type == index_params["index_type"]

    """
    ******************************************************************
      The following cases are used to test `drop_index` function
    ******************************************************************
    """

    def test_drop_index(self, connect, ip_table, get_simple_index):
        '''
        target: test drop index interface
        method: create table and add vectors in it, create index, call drop index
        expected: return code 0, and default index param
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status, mode = connect._cmd("mode")
        assert status.OK()
        if index_type in [IndexType.RNSG]:
            pytest.skip()
        # status, ids = connect.add_vectors(ip_table, vectors)
        status = connect.create_index(ip_table, index_type, index_param)
        if str(mode) == "GPU" and (index_type == IndexType.IVF_PQ):
            assert not status.OK()
        else:
            assert status.OK()
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        status = connect.drop_index(ip_table)
        assert status.OK()
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        assert result._table_name == ip_table
        assert result._index_type == IndexType.FLAT

    def test_drop_index_partition(self, connect, ip_table, get_simple_index):
        '''
        target: test drop index interface
        method: create table, create partition and add vectors in it, create index on table, call drop table index
        expected: return code 0, and default index param
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        if index_type in [IndexType.RNSG]:
            pytest.skip("Skip some RNSG cases")
        status = connect.create_partition(ip_table, tag)
        status, ids = connect.add_vectors(ip_table, vectors, partition_tag=tag)
        status = connect.create_index(ip_table, index_type, index_param)
        assert status.OK()
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        status = connect.drop_index(ip_table)
        assert status.OK()
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        assert result._table_name == ip_table
        assert result._index_type == IndexType.FLAT

    def test_drop_index_partition_C(self, connect, ip_table, get_simple_index):
        '''
        target: test drop index interface
        method: create table, create partitions and add vectors in it, create index on partitions, call drop partition index
        expected: return code 0, and default index param
        '''
        new_tag = "new_tag"
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        if index_type in [IndexType.RNSG]:
            pytest.skip("Skip some RNSG cases")
        status = connect.create_partition(ip_table, tag)
        status = connect.create_partition(ip_table, new_tag)
        status, ids = connect.add_vectors(ip_table, vectors)
        status = connect.create_index(ip_table, index_type, index_param)
        assert status.OK()
        status = connect.drop_index(ip_table)
        assert status.OK()
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        assert result._table_name == ip_table
        assert result._index_type == IndexType.FLAT

    def test_drop_index_repeatly(self, connect, ip_table, get_simple_index):
        '''
        target: test drop index repeatly
        method: create index, call drop index, and drop again
        expected: return code 0
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        # status, ids = connect.add_vectors(ip_table, vectors)
        status, mode = connect._cmd("mode")
        assert status.OK()
        if index_type in [IndexType.RNSG]:
            pytest.skip()
        # status, ids = connect.add_vectors(ip_table, vectors)
        status = connect.create_index(ip_table, index_type, index_param)
        if str(mode) == "GPU" and (index_type == IndexType.IVF_PQ):
            assert not status.OK()
        else:
            assert status.OK()        
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        status = connect.drop_index(ip_table)
        assert status.OK()
        status = connect.drop_index(ip_table)
        assert status.OK()
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        assert result._table_name == ip_table
        assert result._index_type == IndexType.FLAT

    @pytest.mark.level(2)
    def test_drop_index_without_connect(self, dis_connect, ip_table):
        '''
        target: test drop index without connection
        method: drop index, and check if drop successfully
        expected: raise exception
        '''
        nlist = NLIST
        index_type = IndexType.IVFLAT
        index_param = {"nlist": nlist}
        with pytest.raises(Exception) as e:
            status = dis_connect.drop_index(ip_table, index_type, index_param)

    def test_drop_index_table_not_create(self, connect, ip_table):
        '''
        target: test drop index interface when index not created
        method: create table and add vectors in it, create index
        expected: return code not equals to 0, drop index failed
        '''
        status, ids = connect.add_vectors(ip_table, vectors)
        status, result = connect.describe_index(ip_table)
        logging.getLogger().info(result)
        # no create index
        status = connect.drop_index(ip_table)
        logging.getLogger().info(status)
        assert status.OK()

    def test_create_drop_index_repeatly(self, connect, ip_table, get_simple_index):
        '''
        target: test create / drop index repeatly, use the same index params
        method: create index, drop index, four times
        expected: return code 0
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        if index_type in [IndexType.RNSG]:
            pytest.skip("Skip some RNSG cases")
        status, ids = connect.add_vectors(ip_table, vectors)
        for i in range(2):
            status = connect.create_index(ip_table, index_type, index_param)
            assert status.OK()
            status, result = connect.describe_index(ip_table)
            logging.getLogger().info(result)
            status = connect.drop_index(ip_table)
            assert status.OK()
            status, result = connect.describe_index(ip_table)
            logging.getLogger().info(result)
            assert result._table_name == ip_table
            assert result._index_type == IndexType.FLAT

    def test_create_drop_index_repeatly_different_index_params(self, connect, ip_table):
        '''
        target: test create / drop index repeatly, use the different index params
        method: create index, drop index, four times, each tme use different index_params to create index
        expected: return code 0
        '''
        nlist = NLIST
        indexs = [{"index_type": IndexType.IVFLAT, "index_param": {"nlist": nlist}}, {"index_type": IndexType.IVF_SQ8, "index_param": {"nlist": nlist}}]
        status, ids = connect.add_vectors(ip_table, vectors)
        for i in range(2):
            status = connect.create_index(ip_table, indexs[i]["index_type"], indexs[i]["index_param"])
            assert status.OK()
            status, result = connect.describe_index(ip_table)
            assert result._params == indexs[i]["index_param"]
            assert result._table_name == ip_table
            assert result._index_type == indexs[i]["index_type"]
            status, result = connect.describe_index(ip_table)
            logging.getLogger().info(result)
            status = connect.drop_index(ip_table)
            assert status.OK()
            status, result = connect.describe_index(ip_table)
            logging.getLogger().info(result)
            assert result._table_name == ip_table
            assert result._index_type == IndexType.FLAT


class TestIndexJAC:
    tmp, vectors = gen_binary_vectors(nb, dim)

    @pytest.fixture(
        scope="function",
        params=gen_index()
    )
    def get_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] == IndexType.IVF_PQ:
                pytest.skip("ivfpq not support in GPU mode")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if str(connect._cmd("mode")[1]) == "GPU":
            if request.param["index_type"] == IndexType.IVF_PQ:
                pytest.skip("ivfpq not support in GPU mode")
        return request.param

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

    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, jac_table, get_jaccard_index):
        '''
        target: test create index interface
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        logging.getLogger().info(get_jaccard_index)
        status, ids = connect.add_vectors(jac_table, self.vectors)
        status = connect.create_index(jac_table, index_type, index_param)
        if index_type != IndexType.FLAT and index_type != IndexType.IVFLAT:
            assert not status.OK()
        else:
            assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition(self, connect, jac_table, get_jaccard_index):
        '''
        target: test create index interface
        method: create table, create partition, and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        logging.getLogger().info(get_jaccard_index)
        status = connect.create_partition(jac_table, tag)
        status, ids = connect.add_vectors(jac_table, self.vectors, partition_tag=tag)
        status = connect.create_index(jac_table, index_type, index_param)
        assert status.OK()

    @pytest.mark.level(2)
    def test_create_index_without_connect(self, dis_connect, jac_table):
        '''
        target: test create index without connection
        method: create table and add vectors in it, check if added successfully
        expected: raise exception
        '''
        nlist = NLIST
        index_param = {"nlist": nlist}
        with pytest.raises(Exception) as e:
            status = dis_connect.create_index(jac_table, IndexType.IVF_SQ8, index_param)

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_search_with_query_vectors(self, connect, jac_table, get_jaccard_index):
        '''
        target: test create index interface, search with more query vectors
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        logging.getLogger().info(get_jaccard_index)
        status, ids = connect.add_vectors(jac_table, self.vectors)
        status = connect.create_index(jac_table, index_type, index_param)
        logging.getLogger().info(connect.describe_index(jac_table))
        query_vecs = [self.vectors[0], self.vectors[1], self.vectors[2]]
        top_k = 5
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(jac_table, top_k, query_vecs, params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result) == len(query_vecs)

    """
    ******************************************************************
      The following cases are used to test `describe_index` function
    ******************************************************************
    """

    def test_describe_index(self, connect, jac_table, get_jaccard_index):
        '''
        target: test describe index interface
        method: create table and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        logging.getLogger().info(get_jaccard_index)
        # status, ids = connect.add_vectors(jac_table, vectors[:5000])
        status = connect.create_index(jac_table, index_type, index_param)
        status, result = connect.describe_index(jac_table)
        logging.getLogger().info(result)
        assert result._table_name == jac_table
        assert result._index_type == index_type
        assert result._params == index_param

    def test_describe_index_partition(self, connect, jac_table, get_jaccard_index):
        '''
        target: test describe index interface
        method: create table, create partition and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        logging.getLogger().info(get_jaccard_index)
        status = connect.create_partition(jac_table, tag)
        status, ids = connect.add_vectors(jac_table, vectors, partition_tag=tag)
        status = connect.create_index(jac_table, index_type, index_param)
        status, result = connect.describe_index(jac_table)
        logging.getLogger().info(result)
        assert result._params == index_param
        assert result._table_name == jac_table
        assert result._index_type == index_type

    """
    ******************************************************************
      The following cases are used to test `drop_index` function
    ******************************************************************
    """

    def test_drop_index(self, connect, jac_table, get_jaccard_index):
        '''
        target: test drop index interface
        method: create table and add vectors in it, create index, call drop index
        expected: return code 0, and default index param
        '''
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        status, mode = connect._cmd("mode")
        assert status.OK()
        # status, ids = connect.add_vectors(ip_table, vectors)
        status = connect.create_index(jac_table, index_type, index_param)
        assert status.OK()
        status, result = connect.describe_index(jac_table)
        logging.getLogger().info(result)
        status = connect.drop_index(jac_table)
        assert status.OK()
        status, result = connect.describe_index(jac_table)
        logging.getLogger().info(result)
        assert result._table_name == jac_table
        assert result._index_type == IndexType.FLAT

    def test_drop_index_partition(self, connect, jac_table, get_jaccard_index):
        '''
        target: test drop index interface
        method: create table, create partition and add vectors in it, create index on table, call drop table index
        expected: return code 0, and default index param
        '''
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        status = connect.create_partition(jac_table, tag)
        status, ids = connect.add_vectors(jac_table, vectors, partition_tag=tag)
        status = connect.create_index(jac_table, index_type, index_param)
        assert status.OK()
        status, result = connect.describe_index(jac_table)
        logging.getLogger().info(result)
        status = connect.drop_index(jac_table)
        assert status.OK()
        status, result = connect.describe_index(jac_table)
        logging.getLogger().info(result)
        assert result._table_name == jac_table
        assert result._index_type == IndexType.FLAT


class TestIndexHAM:
    tmp, vectors = gen_binary_vectors(nb, dim)

    @pytest.fixture(
        scope="function",
        params=gen_index()
    )
    def get_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if request.param["index_type"] == IndexType.IVF_PQ or request.param["index_type"] == IndexType.HNSW:
            pytest.skip("Skip PQ Temporary")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in CPU mode")
        if request.param["index_type"] == IndexType.IVF_PQ or request.param["index_type"] == IndexType.HNSW:
            pytest.skip("Skip PQ Temporary")
        return request.param

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

    """
    ******************************************************************
      The following cases are used to test `create_index` function
    ******************************************************************
    """
    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index(self, connect, ham_table, get_hamming_index):
        '''
        target: test create index interface
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_hamming_index["index_param"]
        index_type = get_hamming_index["index_type"]
        logging.getLogger().info(get_hamming_index)
        status, ids = connect.add_vectors(ham_table, self.vectors)
        status = connect.create_index(ham_table, index_type, index_param)
        if index_type != IndexType.FLAT and index_type != IndexType.IVFLAT:
            assert not status.OK()
        else:
            assert status.OK()

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_partition(self, connect, ham_table, get_hamming_index):
        '''
        target: test create index interface
        method: create table, create partition, and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_hamming_index["index_param"]
        index_type = get_hamming_index["index_type"]
        logging.getLogger().info(get_hamming_index)
        status = connect.create_partition(ham_table, tag)
        status, ids = connect.add_vectors(ham_table, self.vectors, partition_tag=tag)
        status = connect.create_index(ham_table, index_type, index_param)
        assert status.OK()
        status, res = connect.get_table_row_count(ham_table)
        assert res == len(self.vectors)

    @pytest.mark.level(2)
    def test_create_index_without_connect(self, dis_connect, ham_table):
        '''
        target: test create index without connection
        method: create table and add vectors in it, check if added successfully
        expected: raise exception
        '''
        nlist = NLIST
        index_param = {"nlist": nlist}
        with pytest.raises(Exception) as e:
            status = dis_connect.create_index(ham_table, IndexType.IVF_SQ8, index_param)

    @pytest.mark.timeout(BUILD_TIMEOUT)
    def test_create_index_search_with_query_vectors(self, connect, ham_table, get_hamming_index):
        '''
        target: test create index interface, search with more query vectors
        method: create table and add vectors in it, create index
        expected: return code equals to 0, and search success
        '''
        index_param = get_hamming_index["index_param"]
        index_type = get_hamming_index["index_type"]
        logging.getLogger().info(get_hamming_index)
        status, ids = connect.add_vectors(ham_table, self.vectors)
        status = connect.create_index(ham_table,  index_type, index_param)
        logging.getLogger().info(connect.describe_index(ham_table))
        query_vecs = [self.vectors[0], self.vectors[1], self.vectors[2]]
        top_k = 5
        search_param = get_search_param(index_type)
        status, result = connect.search_vectors(ham_table, top_k, query_vecs, params=search_param)
        logging.getLogger().info(result)
        assert status.OK()
        assert len(result) == len(query_vecs)

    """
    ******************************************************************
      The following cases are used to test `describe_index` function
    ******************************************************************
    """

    def test_describe_index(self, connect, ham_table, get_hamming_index):
        '''
        target: test describe index interface
        method: create table and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_param = get_hamming_index["index_param"]
        index_type = get_hamming_index["index_type"]
        logging.getLogger().info(get_hamming_index)
        # status, ids = connect.add_vectors(jac_table, vectors[:5000])
        status = connect.create_index(ham_table, index_type, index_param)
        status, result = connect.describe_index(ham_table)
        logging.getLogger().info(result)
        assert result._table_name == ham_table
        assert result._index_type == index_type
        assert result._params == index_param

    def test_describe_index_partition(self, connect, ham_table, get_hamming_index):
        '''
        target: test describe index interface
        method: create table, create partition and add vectors in it, create index, call describe index
        expected: return code 0, and index instructure
        '''
        index_param = get_hamming_index["index_param"]
        index_type = get_hamming_index["index_type"]
        logging.getLogger().info(get_hamming_index)
        status = connect.create_partition(ham_table, tag)
        status, ids = connect.add_vectors(ham_table, vectors, partition_tag=tag)
        status = connect.create_index(ham_table, index_type, index_param)
        status, result = connect.describe_index(ham_table)
        logging.getLogger().info(result)
        assert result._params == index_param
        assert result._table_name == ham_table
        assert result._index_type == index_type

    """
    ******************************************************************
      The following cases are used to test `drop_index` function
    ******************************************************************
    """

    def test_drop_index(self, connect, ham_table, get_hamming_index):
        '''
        target: test drop index interface
        method: create table and add vectors in it, create index, call drop index
        expected: return code 0, and default index param
        '''
        index_param = get_hamming_index["index_param"]
        index_type = get_hamming_index["index_type"]
        status, mode = connect._cmd("mode")
        assert status.OK()
        # status, ids = connect.add_vectors(ip_table, vectors)
        status = connect.create_index(ham_table, index_type, index_param)
        assert status.OK()
        status, result = connect.describe_index(ham_table)
        logging.getLogger().info(result)
        status = connect.drop_index(ham_table)
        assert status.OK()
        status, result = connect.describe_index(ham_table)
        logging.getLogger().info(result)
        assert result._table_name == ham_table
        assert result._index_type == IndexType.FLAT

    def test_drop_index_partition(self, connect, ham_table, get_hamming_index):
        '''
        target: test drop index interface
        method: create table, create partition and add vectors in it, create index on table, call drop table index
        expected: return code 0, and default index param
        '''
        index_param = get_hamming_index["index_param"]
        index_type = get_hamming_index["index_type"]
        status = connect.create_partition(ham_table, tag)
        status, ids = connect.add_vectors(ham_table, vectors, partition_tag=tag)
        status = connect.create_index(ham_table, index_type, index_param)
        assert status.OK()
        status, result = connect.describe_index(ham_table)
        logging.getLogger().info(result)
        status = connect.drop_index(ham_table)
        assert status.OK()
        status, result = connect.describe_index(ham_table)
        logging.getLogger().info(result)
        assert result._table_name == ham_table
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

    @pytest.mark.level(1)
    def test_create_index_with_invalid_tablename(self, connect, get_table_name):
        table_name = get_table_name
        nlist = NLIST
        index_param = {"nlist": nlist}
        status = connect.create_index(table_name, IndexType.IVF_SQ8, index_param)
        assert not status.OK()

    @pytest.mark.level(1)
    def test_describe_index_with_invalid_tablename(self, connect, get_table_name):
        table_name = get_table_name
        status, result = connect.describe_index(table_name)
        assert not status.OK()   

    @pytest.mark.level(1)
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
        params=gen_invalid_index()
    )
    def get_index(self, request):
        yield request.param

    @pytest.mark.level(1)
    def test_create_index_with_invalid_index_params(self, connect, table, get_index):
        index_param = get_index["index_param"]
        index_type = get_index["index_type"]
        logging.getLogger().info(get_index)
        # status, ids = connect.add_vectors(table, vectors)
        if (not index_type) or (not isinstance(index_type, IndexType)):
            with pytest.raises(Exception) as e:
                status = connect.create_index(table, index_type, index_param)
        else:
            status = connect.create_index(table, index_type, index_param)
            assert not status.OK()

    """
    Test Building index with invalid nlist
    """
    @pytest.fixture(
        scope="function",
        params=[IndexType.FLAT,IndexType.IVFLAT,IndexType.IVF_SQ8,IndexType.IVF_SQ8H]
    )
    def get_index_type(self, request):
        yield request.param

    def test_create_index_with_invalid_nlist(self, connect, table, get_index_type):
        status, ids = connect.add_vectors(table, vectors)
        status = connect.create_index(table, get_index_type, {"nlist": INVALID_NLIST})
        if get_index_type != IndexType.FLAT:
            assert not status.OK()

    '''
    Test Building index with empty params
    '''
    def test_create_index_with_empty_param(self, connect, table, get_index_type):
        logging.getLogger().info(get_index_type)
        status = connect.create_index(table, get_index_type, {})
        if get_index_type != IndexType.FLAT :
            assert not status.OK()
        status, result = connect.describe_index(table)
        logging.getLogger().info(result)
        assert result._table_name == table
        assert result._index_type == IndexType.FLAT

