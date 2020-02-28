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
tag = "1970-01-01"
top_k = 1
nb = 6000
tag = "partition_tag"


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

    def test_add_partition_flush(self, connect, table):
        '''
        method: add vectors into partition in table, flush serveral times
        expected: status ok
        '''
        vectors = gen_vector(nb, dim)
        status = connect.create_partition(table, tag)
        vectors = gen_vectors(nb, dim)
        ids = [i for i in range(nb)]
        status, ids = connect.insert(table, vectors, ids)
        status = connect.flush([table])
        result, res = connect.get_table_row_count(table)
        assert res == nb
        status, ids = connect.insert(table, vectors, ids, partition_tag=tag)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        result, res = connect.get_table_row_count(table)
        assert res == 2 * nb

    def test_add_partitions_flush(self, connect, table):
        '''
        method: add vectors into partitions in table, flush one
        expected: status ok
        '''
        vectors = gen_vectors(nb, dim)
        tag_new = gen_unique_str()
        status = connect.create_partition(table, tag)
        status = connect.create_partition(table, tag_new)
        ids = [i for i in range(nb)]
        status, ids = connect.insert(table, vectors, ids, partition_tag=tag)
        status = connect.flush([table])
        assert status.OK()
        status, ids = connect.insert(table, vectors, ids, partition_tag=tag_new)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        result, res = connect.get_table_row_count(table)
        assert res == 2 * nb

    def test_add_tables_flush(self, connect, table):
        '''
        method: add vectors into tables, flush one
        expected: status ok
        '''
        vectors = gen_vectors(nb, dim)
        table_new = gen_unique_str()
        param = {'table_name': table_new,
            'dimension': dim,
            'index_file_size': index_file_size,
            'metric_type': MetricType.L2}
        status = connect.create_table(param)
        status = connect.create_partition(table, tag)
        status = connect.create_partition(table_new, tag)
        vectors = gen_vectors(nb, dim)
        ids = [i for i in range(nb)]
        status, ids = connect.insert(table, vectors, ids, partition_tag=tag)
        status, ids = connect.insert(table_new, vectors, ids, partition_tag=tag)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        result, res = connect.get_table_row_count(table)
        assert res == nb
        result, res = connect.get_table_row_count(table_new)
        assert res == 0
       
    def test_add_flush_multiable_times(self, connect, table):
        '''
        method: add vectors, flush serveral times
        expected: status ok
        '''
        vectors = gen_vectors(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        for i in range(10):
            status = connect.flush([table])
            assert status.OK()
        query_vecs = [vectors[0], vectors[1], vectors[-1]]
        status, res = connect.search_vectors(table, top_k, nprobe, query_vecs)
        assert status.OK()

    def test_add_flush_auto(self, connect, table):
        '''
        method: add vectors
        expected: status ok
        '''
        vectors = gen_vectors(nb, dim)
        ids = [i for i in range(nb)]
        status, ids = connect.add_vectors(table, vectors, ids)
        assert status.OK()
        time.sleep(2)
        status, res = connect.get_table_row_count(table)
        assert status.OK()
        assert res == nb 

    @pytest.fixture(
        scope="function",
        params=[
            1,
            100
        ],
    )
    def same_ids(self, request):
        yield request.param

    # both autoflush / flush
    def test_add_flush_same_ids(self, connect, table, same_ids):
        '''
        method: add vectors, with same ids, count(same ids) < 15, > 15
        expected: status ok
        '''
        vectors = gen_vectors(nb, dim)
        ids = [i for i in range(nb)]
        for i, item in enumerate(ids):
            if item <= same_ids:
                ids[i] = 0
        status, ids = connect.add_vectors(table, vectors, ids)
        time.sleep(2)
        status = connect.flush([table])
        assert status.OK()
        status, res = connect.get_table_row_count(table)
        assert status.OK()
        assert res == nb 

    def test_delete_flush_multiable_times(self, connect, table):
        '''
        method: delete vectors, flush serveral times
        expected: status ok
        '''
        vectors = gen_vectors(nb, dim)
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

    # TODO: CI fail, LOCAL pass
    def _test_table_count_during_flush(self, connect, args):
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
            status = milvus.delete_by_id(table_name, [i for i in range(nb)])
            assert status.OK()
            status = milvus.flush([table_name])
            assert status.OK()
        p = Process(target=flush, args=(table, ))
        p.start()
        status, res = milvus.get_table_row_count(table)
        assert status.OK()
        p.join()
        status, res = milvus.get_table_row_count(table)
        assert status.OK()
        logging.getLogger().info(res)
        assert res == 0


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
