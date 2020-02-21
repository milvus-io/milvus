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
COMPACT_TIMEOUT = 30
nprobe = 1
top_k = 1
epsilon = 0.0001
tag = "1970-01-01"
nb = 6000


class TestTableInfo:
    """
    ******************************************************************
      The following cases are used to test `table_info` function
    ******************************************************************
    """
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_get_table_info_name_None(self, connect, table):
        '''
        target: get table info where table name is None
        method: call table_info with the table_name: None
        expected: status not ok
        '''
        table_name = None
        status, info = connect.table_info(table_name)
        assert not status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
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

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_get_table_info_name_invalid(self, connect, get_table_name):
        '''
        target: get table info where table name is invalid
        method: call table_info with invalid table_name
        expected: status not ok
        '''
        table_name = get_table_name
        status, info = connect.table_info(table_name)
        assert not status.OK()


class TestCompactBase:
    """
    ******************************************************************
      The following cases are used to test `compact` function
    ******************************************************************
    """
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_compact_table_name_None(self, connect, table):
        '''
        target: compact table where table name is None
        method: compact with the table_name: None
        expected: exception raised
        '''
        table_name = None
        with pytest.raises(Exception) as e:
            status = connect.compact(table_name)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_compact_table_name_not_existed(self, connect, table):
        '''
        target: compact table not existed
        method: compact with a random table_name, which is not in db
        expected: status not ok
        '''
        table_name = gen_unique_str("not_existed_table")
        status = connect.compact(table_name)
        assert not status.OK()
    
    @pytest.fixture(
        scope="function",
        params=gen_invalid_table_names()
    )
    def get_table_name(self, request):
        yield request.param

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_compact_table_name_invalid(self, connect, get_table_name):
        '''
        target: compact table with invalid name
        method: compact with invalid table_name
        expected: status not ok
        '''
        table_name = get_table_name
        status = connect.compact(table_name)
        assert not status.OK()
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_and_compact(self, connect, table):
        '''
        target: test add vector and compact 
        method: add vector and compact table
        expected: status ok, vector added
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(table)
        assert status.OK()
        size_before = info.native_stat.segment_stats[0].data_size
        status = connect.compact(table)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(table)
        assert status.OK()
        size_after = info.native_stat.segment_stats[0].data_size
        assert(size_before == size_after)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vectors_and_compact(self, connect, table):
        '''
        target: test add vectors and compact 
        method: add vectors and compact table
        expected: status ok, vectors added
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(table)
        assert status.OK()
        size_before = info.native_stat.segment_stats[0].data_size
        status = connect.compact(table)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(table)
        assert status.OK()
        size_after = info.native_stat.segment_stats[0].data_size
        assert(size_before == size_after)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vectors_delete_part_and_compact(self, connect, table):
        '''
        target: test add vectors, delete part of them and compact 
        method: add vectors, delete a few and compact table
        expected: status ok, data size is smaller after compact
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_by_id(table, delete_ids)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(table)
        assert status.OK()
        logging.getLogger().info(info.native_stat)
        size_before = info.native_stat.segment_stats[0].data_size
        logging.getLogger().info(size_before)
        status = connect.compact(table)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(table)
        assert status.OK()
        logging.getLogger().info(info.native_stat)
        size_after = info.native_stat.segment_stats[0].data_size
        logging.getLogger().info(size_after)
        assert(size_before > size_after)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vectors_delete_all_and_compact(self, connect, table):
        '''
        target: test add vectors, delete them and compact 
        method: add vectors, delete all and compact table
        expected: status ok, no data size in table info because table is empty
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.delete_by_id(table, ids)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(table)
        assert status.OK()
        status = connect.compact(table)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(table)
        assert status.OK()
        logging.getLogger().info(info.native_stat)
        assert(len(info.native_stat.segment_stats) == 0)

    @pytest.fixture(
        scope="function",
        params=gen_simple_index_params()
    )
    def get_simple_index_params(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] not in [IndexType.IVF_SQ8, IndexType.IVFLAT, IndexType.FLAT]:
                pytest.skip("Only support index_type: flat/ivf_flat/ivf_sq8")
        else:
            pytest.skip("Only support CPU mode")
        return request.param

    def test_compact_after_index_created(self, connect, table, get_simple_index_params):
        '''
        target: test compact table after index created
        method: add vectors, create index, delete part of vectors and compact
        expected: status ok, index description no change, data size smaller after compact
        '''
        index_params = get_simple_index_params
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.create_index(table, index_params) 
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_by_id(table, delete_ids)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(table)
        assert status.OK()
        size_before = info.native_stat.segment_stats[0].data_size
        logging.getLogger().info(info.native_stat)
        status = connect.compact(table)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(table)
        assert status.OK()
        logging.getLogger().info(info.native_stat)
        size_after = info.native_stat.segment_stats[0].data_size
        assert(size_before > size_after)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_and_compact_twice(self, connect, table):
        '''
        target: test add vector and compact twice
        method: add vector and compact table twice
        expected: status ok, data size no change
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(table)
        assert status.OK()
        size_before = info.native_stat.segment_stats[0].data_size
        status = connect.compact(table)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(table)
        assert status.OK()
        size_after = info.native_stat.segment_stats[0].data_size
        assert(size_before == size_after)
        status = connect.compact(table)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info after compact twice
        status, info = connect.table_info(table)
        assert status.OK()
        size_after_twice = info.native_stat.segment_stats[0].data_size
        assert(size_after == size_after_twice)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vectors_delete_part_and_compact_twice(self, connect, table):
        '''
        target: test add vectors, delete part of them and compact twice
        method: add vectors, delete part and compact table twice
        expected: status ok, data size smaller after first compact, no change after second
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_by_id(table, delete_ids)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(table)
        assert status.OK()
        size_before = info.native_stat.segment_stats[0].data_size
        status = connect.compact(table)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(table)
        assert status.OK()
        size_after = info.native_stat.segment_stats[0].data_size
        assert(size_before > size_after)
        status = connect.compact(table)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info after compact twice
        status, info = connect.table_info(table)
        assert status.OK()
        size_after_twice = info.native_stat.segment_stats[0].data_size
        assert(size_after == size_after_twice)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_compact_multi_tables(self, connect):
        '''
        target: test compact works or not with multiple tables
        method: create 50 tables, add vectors into them and compact in turn
        expected: status ok
        '''
        nq = 100
        num_tables = 50
        vectors = gen_vectors(nq, dim)
        table_list = []
        for i in range(num_tables):
            table_name = gen_unique_str("test_compact_multi_table_%d" % i)
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.L2}
            connect.create_table(param)
        time.sleep(6)
        for i in range(num_tables):
            status, ids = connect.add_vectors(table_name=table_list[i], records=vectors)
            assert status.OK()
            status = connect.compact(table_list[i])
            assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_after_compact(self, connect, table):
        '''
        target: test add vector after compact 
        method: after compact operation, add vector
        expected: status ok, vector added
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(table)
        assert status.OK()
        size_before = info.native_stat.segment_stats[0].data_size
        status = connect.compact(table)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(table)
        assert status.OK()
        size_after = info.native_stat.segment_stats[0].data_size
        assert(size_before == size_after)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(table, vector)
        assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_index_creation_after_compact(self, connect, table, get_simple_index_params):
        '''
        target: test index creation after compact
        method: after compact operation, create index
        expected: status ok, index description no change
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.compact(table)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        index_params = get_simple_index_params
        status = connect.create_index(table, index_params) 
        assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_delete_vectors_after_compact(self, connect, table):
        '''
        target: test delete vectors after compact
        method: after compact operation, delete vectors
        expected: status ok, vectors deleted
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.compact(table)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.delete_by_id(table, ids)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_search_after_compact(self, connect, table):
        '''
        target: test search after compact
        method: after compact operation, search vector
        expected: status ok
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.compact(table)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        query_vecs = [vectors[0]]
        status, res = connect.search_vectors(table, top_k, nprobe, query_vecs) 
        logging.getLogger().info(res)
        assert status.OK()


class TestCompactJAC:
    """
    ******************************************************************
      The following cases are used to test `compact` function
    ******************************************************************
    """
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_and_compact(self, connect, jac_table):
        '''
        target: test add vector and compact 
        method: add vector and compact table
        expected: status ok, vector added
        '''
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.add_vectors(jac_table, vector)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(jac_table)
        assert status.OK()
        size_before = info.native_stat.segment_stats[0].data_size
        status = connect.compact(jac_table)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(jac_table)
        assert status.OK()
        size_after = info.native_stat.segment_stats[0].data_size
        assert(size_before == size_after)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vectors_and_compact(self, connect, jac_table):
        '''
        target: test add vectors and compact 
        method: add vectors and compact table
        expected: status ok, vectors added
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.add_vectors(jac_table, vectors)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(jac_table)
        assert status.OK()
        size_before = info.native_stat.segment_stats[0].data_size
        status = connect.compact(jac_table)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(jac_table)
        assert status.OK()
        size_after = info.native_stat.segment_stats[0].data_size
        assert(size_before == size_after)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vectors_delete_part_and_compact(self, connect, jac_table):
        '''
        target: test add vectors, delete part of them and compact 
        method: add vectors, delete a few and compact table
        expected: status ok, data size is smaller after compact
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.add_vectors(jac_table, vectors)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_by_id(jac_table, delete_ids)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(jac_table)
        assert status.OK()
        logging.getLogger().info(info.native_stat)
        size_before = info.native_stat.segment_stats[0].data_size
        logging.getLogger().info(size_before)
        status = connect.compact(jac_table)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(jac_table)
        assert status.OK()
        logging.getLogger().info(info.native_stat)
        size_after = info.native_stat.segment_stats[0].data_size
        logging.getLogger().info(size_after)
        assert(size_before > size_after)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vectors_delete_all_and_compact(self, connect, jac_table):
        '''
        target: test add vectors, delete them and compact 
        method: add vectors, delete all and compact table
        expected: status ok, no data size in table info because table is empty
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.add_vectors(jac_table, vectors)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        status = connect.delete_by_id(jac_table, ids)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(jac_table)
        assert status.OK()
        status = connect.compact(jac_table)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(jac_table)
        assert status.OK()
        logging.getLogger().info(info.native_stat)
        assert(len(info.native_stat.segment_stats) == 0)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_and_compact_twice(self, connect, jac_table):
        '''
        target: test add vector and compact twice
        method: add vector and compact table twice
        expected: status ok
        '''
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.add_vectors(jac_table, vector)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(jac_table)
        assert status.OK()
        size_before = info.native_stat.segment_stats[0].data_size
        status = connect.compact(jac_table)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(jac_table)
        assert status.OK()
        size_after = info.native_stat.segment_stats[0].data_size
        assert(size_before == size_after)
        status = connect.compact(jac_table)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        # get table info after compact twice
        status, info = connect.table_info(jac_table)
        assert status.OK()
        size_after_twice = info.native_stat.segment_stats[0].data_size
        assert(size_after == size_after_twice)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vectors_delete_part_and_compact_twice(self, connect, jac_table):
        '''
        target: test add vectors, delete part of them and compact twice
        method: add vectors, delete part and compact table twice
        expected: status ok, data size smaller after first compact, no change after second
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.add_vectors(jac_table, vectors)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_by_id(jac_table, delete_ids)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(jac_table)
        assert status.OK()
        size_before = info.native_stat.segment_stats[0].data_size
        status = connect.compact(jac_table)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(jac_table)
        assert status.OK()
        size_after = info.native_stat.segment_stats[0].data_size
        assert(size_before > size_after)
        status = connect.compact(jac_table)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        # get table info after compact twice
        status, info = connect.table_info(jac_table)
        assert status.OK()
        size_after_twice = info.native_stat.segment_stats[0].data_size
        assert(size_after == size_after_twice)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_compact_multi_tables(self, connect):
        '''
        target: test compact works or not with multiple tables
        method: create 50 tables, add vectors into them and compact in turn
        expected: status ok
        '''
        nq = 100
        num_tables = 50
        tmp, vectors = gen_binary_vectors(nq, dim)
        table_list = []
        for i in range(num_tables):
            table_name = gen_unique_str("test_compact_multi_table_%d" % i)
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.JACCARD}
            connect.create_table(param)
        time.sleep(6)
        for i in range(num_tables):
            status, ids = connect.add_vectors(table_name=table_list[i], records=vectors)
            assert status.OK()
            status = connect.compact(table_list[i])
            assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_after_compact(self, connect, jac_table):
        '''
        target: test add vector after compact 
        method: after compact operation, add vector
        expected: status ok, vector added
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.add_vectors(jac_table, vectors)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(jac_table)
        assert status.OK()
        size_before = info.native_stat.segment_stats[0].data_size
        status = connect.compact(jac_table)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(jac_table)
        assert status.OK()
        size_after = info.native_stat.segment_stats[0].data_size
        assert(size_before == size_after)
        tmp, vector = gen_binary_vectors(1, dim)
        status, ids = connect.add_vectors(jac_table, vector)
        assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_delete_vectors_after_compact(self, connect, jac_table):
        '''
        target: test delete vectors after compact
        method: after compact operation, delete vectors
        expected: status ok, vectors deleted
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.add_vectors(jac_table, vectors)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        status = connect.compact(jac_table)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        status = connect.delete_by_id(jac_table, ids)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_search_after_compact(self, connect, jac_table):
        '''
        target: test search after compact
        method: after compact operation, search vector
        expected: status ok
        '''
        tmp, vectors = gen_binary_vectors(nb, dim)
        status, ids = connect.add_vectors(jac_table, vectors)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        status = connect.compact(jac_table)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        query_vecs = [vectors[0]]
        status, res = connect.search_vectors(jac_table, top_k, nprobe, query_vecs) 
        logging.getLogger().info(res)
        assert status.OK()


class TestCompactIP:
    """
    ******************************************************************
      The following cases are used to test `compact` function
    ******************************************************************
    """
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_and_compact(self, connect, ip_table):
        '''
        target: test add vector and compact 
        method: add vector and compact table
        expected: status ok, vector added
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(ip_table)
        assert status.OK()
        size_before = info.native_stat.segment_stats[0].data_size
        status = connect.compact(ip_table)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(ip_table)
        assert status.OK()
        size_after = info.native_stat.segment_stats[0].data_size
        assert(size_before == size_after)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vectors_and_compact(self, connect, ip_table):
        '''
        target: test add vectors and compact 
        method: add vectors and compact table
        expected: status ok, vectors added
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(ip_table, vectors)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(ip_table)
        assert status.OK()
        size_before = info.native_stat.segment_stats[0].data_size
        status = connect.compact(ip_table)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(ip_table)
        assert status.OK()
        size_after = info.native_stat.segment_stats[0].data_size
        assert(size_before == size_after)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vectors_delete_part_and_compact(self, connect, ip_table):
        '''
        target: test add vectors, delete part of them and compact 
        method: add vectors, delete a few and compact table
        expected: status ok, data size is smaller after compact
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(ip_table, vectors)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_by_id(ip_table, delete_ids)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(ip_table)
        assert status.OK()
        logging.getLogger().info(info.native_stat)
        size_before = info.native_stat.segment_stats[0].data_size
        logging.getLogger().info(size_before)
        status = connect.compact(ip_table)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(ip_table)
        assert status.OK()
        logging.getLogger().info(info.native_stat)
        size_after = info.native_stat.segment_stats[0].data_size
        logging.getLogger().info(size_after)
        assert(size_before > size_after)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vectors_delete_all_and_compact(self, connect, ip_table):
        '''
        target: test add vectors, delete them and compact 
        method: add vectors, delete all and compact table
        expected: status ok, no data size in table info because table is empty
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(ip_table, vectors)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        status = connect.delete_by_id(ip_table, ids)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(ip_table)
        assert status.OK()
        status = connect.compact(ip_table)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(ip_table)
        assert status.OK()
        logging.getLogger().info(info.native_stat)
        assert(len(info.native_stat.segment_stats) == 0)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_and_compact_twice(self, connect, ip_table):
        '''
        target: test add vector and compact twice
        method: add vector and compact table twice
        expected: status ok
        '''
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(ip_table)
        assert status.OK()
        size_before = info.native_stat.segment_stats[0].data_size
        status = connect.compact(ip_table)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(ip_table)
        assert status.OK()
        size_after = info.native_stat.segment_stats[0].data_size
        assert(size_before == size_after)
        status = connect.compact(ip_table)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        # get table info after compact twice
        status, info = connect.table_info(ip_table)
        assert status.OK()
        size_after_twice = info.native_stat.segment_stats[0].data_size
        assert(size_after == size_after_twice)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vectors_delete_part_and_compact_twice(self, connect, ip_table):
        '''
        target: test add vectors, delete part of them and compact twice
        method: add vectors, delete part and compact table twice
        expected: status ok, data size smaller after first compact, no change after second
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(ip_table, vectors)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_by_id(ip_table, delete_ids)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(ip_table)
        assert status.OK()
        size_before = info.native_stat.segment_stats[0].data_size
        status = connect.compact(ip_table)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(ip_table)
        assert status.OK()
        size_after = info.native_stat.segment_stats[0].data_size
        assert(size_before > size_after)
        status = connect.compact(ip_table)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        # get table info after compact twice
        status, info = connect.table_info(ip_table)
        assert status.OK()
        size_after_twice = info.native_stat.segment_stats[0].data_size
        assert(size_after == size_after_twice)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_compact_multi_tables(self, connect):
        '''
        target: test compact works or not with multiple tables
        method: create 50 tables, add vectors into them and compact in turn
        expected: status ok
        '''
        nq = 100
        num_tables = 50
        vectors = gen_vectors(nq, dim)
        table_list = []
        for i in range(num_tables):
            table_name = gen_unique_str("test_compact_multi_table_%d" % i)
            table_list.append(table_name)
            param = {'table_name': table_name,
                     'dimension': dim,
                     'index_file_size': index_file_size,
                     'metric_type': MetricType.IP}
            connect.create_table(param)
        time.sleep(6)
        for i in range(num_tables):
            status, ids = connect.add_vectors(table_name=table_list[i], records=vectors)
            assert status.OK()
            status = connect.compact(table_list[i])
            assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_vector_after_compact(self, connect, ip_table):
        '''
        target: test add vector after compact 
        method: after compact operation, add vector
        expected: status ok, vector added
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(ip_table, vectors)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        # get table info before compact
        status, info = connect.table_info(ip_table)
        assert status.OK()
        size_before = info.native_stat.segment_stats[0].data_size
        status = connect.compact(ip_table)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        # get table info after compact
        status, info = connect.table_info(ip_table)
        assert status.OK()
        size_after = info.native_stat.segment_stats[0].data_size
        assert(size_before == size_after)
        vector = gen_single_vector(dim)
        status, ids = connect.add_vectors(ip_table, vector)
        assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_delete_vectors_after_compact(self, connect, ip_table):
        '''
        target: test delete vectors after compact
        method: after compact operation, delete vectors
        expected: status ok, vectors deleted
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(ip_table, vectors)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        status = connect.compact(ip_table)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        status = connect.delete_by_id(ip_table, ids)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_search_after_compact(self, connect, ip_table):
        '''
        target: test search after compact
        method: after compact operation, search vector
        expected: status ok
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(ip_table, vectors)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        status = connect.compact(ip_table)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        query_vecs = [vectors[0]]
        status, res = connect.search_vectors(ip_table, top_k, nprobe, query_vecs) 
        logging.getLogger().info(res)
        assert status.OK()
