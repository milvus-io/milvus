import time
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
nlist = 1024


class TestTableInfoBase:
    def index_string_convert(self, index_string, index_type):
        if index_string == "IDMAP" and index_type == IndexType.FLAT:
            return True
        if index_string == "IVFSQ8" and index_type == IndexType.IVF_SQ8:
            return True
        if index_string == "IVFFLAT" and index_type == IndexType.IVFLAT:
            return True
        return False

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

    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_table_info_table_row_count(self, connect, table):
        '''
        target: get row count with table_info
        method: add and delete vectors, check count in table info
        expected: status ok, count as expected
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, info = connect.table_info(table)
        assert status.OK()
        assert info.count == nb
        # delete a few vectors
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_by_id(table, delete_ids)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, info = connect.table_info(table)
        assert status.OK()
        assert info.count == nb - 2

    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_table_info_partition_stats_A(self, connect, table):
        '''
        target: get partition info in a table
        method: no partition, call table_info and check partition_stats
        expected: status ok, "_default" partition is listed
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, info = connect.table_info(table)
        assert status.OK()
        logging.getLogger().info(info)
        assert len(info.partitions_stat) == 1
        assert info.partitions_stat[0].tag == "_default"
        assert info.partitions_stat[0].count == nb


    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_table_info_partition_stats_B(self, connect, table):
        '''
        target: get partition info in a table
        method: call table_info after partition created and check partition_stats
        expected: status ok, vectors added to partition
        '''
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(table, tag)
        status, ids = connect.add_vectors(table, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, info = connect.table_info(table)
        assert status.OK()
        logging.getLogger().info(info)
        assert len(info.partitions_stat) == 2
        assert info.partitions_stat[1].tag == tag
        assert info.partitions_stat[1].count == nb

    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_table_info_partition_stats_C(self, connect, table):
        '''
        target: get partition info in a table
        method: create two partitions, add vectors in one of the partitions, call table_info and check 
        expected: status ok, vectors added to one partition but not the other
        '''
        new_tag = "new_tag"
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(table, tag)
        assert status.OK()
        status = connect.create_partition(table, new_tag)
        assert status.OK()
        status, ids = connect.add_vectors(table, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, info = connect.table_info(table)
        assert status.OK()
        logging.getLogger().info(info)
        for partition in info.partitions_stat:
            if partition.tag == tag:
                assert partition.count == nb
            else:
                assert partition.count == 0

    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_table_info_partition_stats_D(self, connect, table):
        '''
        target: get partition info in a table
        method: create two partitions, add vectors in both partitions, call table_info and check 
        expected: status ok, vectors added to both partitions
        '''
        new_tag = "new_tag"
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(table, tag)
        assert status.OK()
        status = connect.create_partition(table, new_tag)
        assert status.OK()
        status, ids = connect.add_vectors(table, vectors, partition_tag=tag)
        assert status.OK()
        status, ids = connect.add_vectors(table, vectors, partition_tag=new_tag)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, info = connect.table_info(table)
        assert status.OK()
        assert info.count == nb * 2
        for partition in info.partitions_stat:
            if partition.tag == tag:
                assert partition.count == nb
            elif partition.tag == new_tag:
                assert partition.count == nb

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] not in [IndexType.IVF_SQ8, IndexType.IVFLAT, IndexType.FLAT]:
                pytest.skip("Only support index_type: flat/ivf_flat/ivf_sq8")
        else:
            pytest.skip("Only support CPU mode")
        return request.param
    
    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_table_info_after_index_created(self, connect, table, get_simple_index):
        '''
        target: test table info after index created
        method: create table, add vectors, create index and call table_info 
        expected: status ok, index created and shown in segments_stat
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.create_index(table, index_type, index_param) 
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, info = connect.table_info(table)
        assert status.OK()
        logging.getLogger().info(info)
        index_string = info.partitions_stat[0].segments_stat[0].index_name
        match = self.index_string_convert(index_string, index_type)
        assert match
        assert nb == info.partitions_stat[0].segments_stat[0].count

    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_table_info_after_create_same_index_repeatedly(self, connect, table, get_simple_index):
        '''
        target: test table info after index created repeatedly
        method: create table, add vectors, create index and call table_info multiple times 
        expected: status ok, index info shown in segments_stat
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status = connect.create_index(table, index_type, index_param)
        status = connect.create_index(table, index_type, index_param)
        status = connect.create_index(table, index_type, index_param)
        assert status.OK()
        status, info = connect.table_info(table)
        assert status.OK()
        logging.getLogger().info(info)
        index_string = info.partitions_stat[0].segments_stat[0].index_name
        match = self.index_string_convert(index_string, index_type)
        assert match
        assert nb == info.partitions_stat[0].segments_stat[0].count

    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_table_info_after_create_different_index_repeatedly(self, connect, table, get_simple_index):
        '''
        target: test table info after index created repeatedly
        method: create table, add vectors, create index and call table_info multiple times 
        expected: status ok, index info shown in segments_stat
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        index_param = {"nlist": nlist} 
        for index_type in [IndexType.FLAT, IndexType.IVFLAT, IndexType.IVF_SQ8]:
            status = connect.create_index(table, index_type, index_param)
            assert status.OK()
            status, info = connect.table_info(table)
            assert status.OK()
            logging.getLogger().info(info)
            index_string = info.partitions_stat[0].segments_stat[0].index_name
            match = self.index_string_convert(index_string, index_type)
            assert match
            assert nb == info.partitions_stat[0].segments_stat[0].count
