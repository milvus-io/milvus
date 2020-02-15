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
table_id = "test_flush"
DELETE_TIMEOUT = 60
nprobe = 1
epsilon = 0.0001
tag = "1970-01-01"
top_k = 1
nb = 6000


class TestFlushBase:
    """
    ******************************************************************
      The following cases are used to test `flush` function
    ******************************************************************
    """

    @pytest.fixture(
        scope="function",
        params=gen_simple_index_params()
    )
    def get_simple_index_params(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] != IndexType.IVF_SQ8 or request.param["index_type"] != IndexType.IVFLAT or request.param["index_type"] != IndexType.FLAT:
                pytest.skip("Only support index_type: flat/ivf_flat/ivf_sq8")
        else:
            pytest.skip("Only support CPU mode")
        return request.param

    def test_flush_table_not_existed(self, connect, table):
        '''
        target: test delete vector, params table_name not existed
        method: add vector and delete
        expected: status not ok
        '''
        table_new = gen_unique_str()
        status = connect.flush([table_new])
        assert not status.OK()

    def test_flush_empty_table(self, connect, table):
        '''
        method: flush table with no vectors
        expected: status ok
        '''
        status = connect.flush([table])
        assert status.OK()

    def test_add_flush_multiable_times(self, connect, table):
        '''
        method: add vectors, flush serveral times
        expected: status ok
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        for i in range(10):
            status = connect.flush([table])
            assert status.OK()
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status, res = connect.search_vectors(table, top_k, nprobe, query_vecs)
        assert status.OK()

    def test_delete_flush_multiable_times(self, connect, table):
        '''
        method: delete vectors, flush serveral times
        expected: status ok
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.delete_by_id(table, [ids[-1]])
        assert status.OK()
        for i in range(10):
            status = connect.flush([table])
            assert status.OK()
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status, res = connect.search_vectors(table, top_k, nprobe, query_vecs)
        assert status.OK()

    def test_table_count_during_flush(self, connect, args):
        '''
        method: flush table at background, call `get_table_row_count`
        expected: status ok
        '''
        table = gen_unique_str() 
        uri = "tcp://%s:%s" % (args["ip"], args["port"])
        param = {'table_name': table,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        milvus = get_milvus()
        milvus.connect(uri=uri)
        milvus.create_table(param)
        vectors = gen_vector(nb, dim)
        status, ids = milvus.add_vectors(table, vectors, ids=[i for i in range(nb)])
        def flush(table_name):
            milvus = get_milvus()
            milvus.connect(uri=uri)
            status = milvus.delete_by_id(table_name, [i for i in range(100000)])
            assert status.OK()
            status = milvus.flush([table_name])
            assert status.OK()
        p = Process(target=flush, args=(table, ))
        p.start()
        status, res = milvus.get_table_row_count(table)
        p.join()
        status, res = milvus.get_table_row_count(table)
        assert status.OK()
        logging.getLogger().info(res)


class TestTableNameInvalid(object):
    """
    Test adding vectors with invalid table names
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_table_names()
    )
    def get_table_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_flush_with_invalid_table_name(self, connect, get_table_name):
        table_name = get_table_name
        with pytest.raises(Exception) as e:
            status, result = connect.flush(table_name)
