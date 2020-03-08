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
GET_TIMEOUT = 30
nprobe = 1
top_k = 1
epsilon = 0.001
tag = "1970-01-01"
nb = 6000


class TestGetVectorIdsBase:
    def get_valid_segment_name(self, connect, table):
        vectors = gen_vector(nb, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, info = connect.table_info(table)
        assert status.OK()
        logging.getLogger().info(info.partitions_stat[0].segments_stat[0].segment_name)
        return info.partitions_stat[0].segments_stat[0].segment_name
        
    """
    ******************************************************************
      The following cases are used to test `get_vector_ids` function
    ******************************************************************
    """
    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_table_name_None(self, connect, table):
        '''
        target: get vector ids where table name is None
        method: call get_vector_ids with the table_name: None
        expected: exception raised
        '''
        table_name = None
        segment_name = self.get_valid_segment_name(connect, table)
        with pytest.raises(Exception) as e:
            status, vector_ids = connect.get_vector_ids(table_name, segment_name)

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_table_name_not_existed(self, connect, table):
        '''
        target: get vector ids where table name does not exist
        method: call get_vector_ids with a random table_name, which is not in db
        expected: status not ok
        '''
        table_name = gen_unique_str("not_existed_table")
        segment_name = self.get_valid_segment_name(connect, table)
        status, vector_ids = connect.get_vector_ids(table_name, segment_name)
        assert not status.OK()
    
    @pytest.fixture(
        scope="function",
        params=gen_invalid_table_names()
    )
    def get_table_name(self, request):
        yield request.param

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_table_name_invalid(self, connect, table, get_table_name):
        '''
        target: get vector ids where table name is invalid
        method: call get_vector_ids with invalid table_name
        expected: status not ok
        '''
        table_name = get_table_name
        segment_name = self.get_valid_segment_name(connect, table)
        status, vector_ids = connect.get_vector_ids(table_name, segment_name)
        assert not status.OK()

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_segment_name_None(self, connect, table):
        '''
        target: get vector ids where segment name is None
        method: call get_vector_ids with the segment_name: None
        expected: exception raised
        '''
        valid_segment_name = self.get_valid_segment_name(connect, table)
        segment = None
        with pytest.raises(Exception) as e:
            status, vector_ids = connect.get_vector_ids(table, segment)

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_segment_name_not_existed(self, connect, table):
        '''
        target: get vector ids where segment name does not exist
        method: call get_vector_ids with a random segment name
        expected: status not ok
        '''
        valid_segment_name = self.get_valid_segment_name(connect, table)
        segment = gen_unique_str("not_existed_segment")
        status, vector_ids = connect.get_vector_ids(table, segment)
        logging.getLogger().info(vector_ids)
        assert not status.OK()

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_without_index_A(self, connect, table):
        '''
        target: get vector ids when there is no index
        method: call get_vector_ids and check if the segment contains vectors
        expected: status ok
        '''
        vectors = gen_vector(10, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, info = connect.table_info(table)
        assert status.OK()
        status, vector_ids = connect.get_vector_ids(table, info.partitions_stat[0].segments_stat[0].segment_name)
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]


    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_without_index_B(self, connect, table):
        '''
        target: get vector ids when there is no index but with partition
        method: create partition, add vectors to it and call get_vector_ids, check if the segment contains vectors
        expected: status ok
        '''
        status = connect.create_partition(table, tag)
        assert status.OK()
        vectors = gen_vector(10, dim)
        status, ids = connect.add_vectors(table, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, info = connect.table_info(table)
        assert status.OK()
        assert info.partitions_stat[1].tag == tag
        status, vector_ids = connect.get_vector_ids(table, info.partitions_stat[1].segments_stat[0].segment_name)
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]

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

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_with_index_A(self, connect, table, get_simple_index):
        '''
        target: get vector ids when there is index
        method: call get_vector_ids and check if the segment contains vectors
        expected: status ok
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status = connect.create_index(table, index_type, index_param)
        assert status.OK()
        vectors = gen_vector(10, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, info = connect.table_info(table)
        assert status.OK()
        status, vector_ids = connect.get_vector_ids(table, info.partitions_stat[0].segments_stat[0].segment_name)
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_with_index_B(self, connect, table, get_simple_index):
        '''
        target: get vector ids when there is index and with partition
        method: create partition, add vectors to it and call get_vector_ids, check if the segment contains vectors
        expected: status ok
        '''
        status = connect.create_partition(table, tag)
        assert status.OK()
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status = connect.create_index(table, index_type, index_param)
        assert status.OK()
        vectors = gen_vector(10, dim)
        status, ids = connect.add_vectors(table, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([table])
        assert status.OK()
        status, info = connect.table_info(table)
        assert status.OK()
        assert info.partitions_stat[1].tag == tag
        status, vector_ids = connect.get_vector_ids(table, info.partitions_stat[1].segments_stat[0].segment_name)
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_after_delete_vectors(self, connect, table):
        '''
        target: get vector ids after vectors are deleted
        method: add vectors and delete a few, call get_vector_ids
        expected: status ok, vector_ids decreased after vectors deleted
        '''
        vectors = gen_vector(2, dim)
        status, ids = connect.add_vectors(table, vectors)
        assert status.OK()
        delete_ids = [ids[0]]
        status = connect.delete_by_id(table, delete_ids)
        status = connect.flush([table])
        assert status.OK()
        status, info = connect.table_info(table)
        assert status.OK()
        status, vector_ids = connect.get_vector_ids(table, info.partitions_stat[0].segments_stat[0].segment_name)
        assert len(vector_ids) == 1
        assert vector_ids[0] == ids[1]


class TestGetVectorIdsIP:
    """
    ******************************************************************
      The following cases are used to test `get_vector_ids` function
    ******************************************************************
    """
    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_without_index_A(self, connect, ip_table):
        '''
        target: get vector ids when there is no index
        method: call get_vector_ids and check if the segment contains vectors
        expected: status ok
        '''
        vectors = gen_vector(10, dim)
        status, ids = connect.add_vectors(ip_table, vectors)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        status, info = connect.table_info(ip_table)
        assert status.OK()
        status, vector_ids = connect.get_vector_ids(ip_table, info.partitions_stat[0].segments_stat[0].segment_name)
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]


    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_without_index_B(self, connect, ip_table):
        '''
        target: get vector ids when there is no index but with partition
        method: create partition, add vectors to it and call get_vector_ids, check if the segment contains vectors
        expected: status ok
        '''
        status = connect.create_partition(ip_table, tag)
        assert status.OK()
        vectors = gen_vector(10, dim)
        status, ids = connect.add_vectors(ip_table, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        status, info = connect.table_info(ip_table)
        assert status.OK()
        assert info.partitions_stat[1].tag == tag
        status, vector_ids = connect.get_vector_ids(ip_table, info.partitions_stat[1].segments_stat[0].segment_name)
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]

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

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_with_index_A(self, connect, ip_table, get_simple_index):
        '''
        target: get vector ids when there is index
        method: call get_vector_ids and check if the segment contains vectors
        expected: status ok
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status = connect.create_index(ip_table, index_type, index_param)
        assert status.OK()
        vectors = gen_vector(10, dim)
        status, ids = connect.add_vectors(ip_table, vectors)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        status, info = connect.table_info(ip_table)
        assert status.OK()
        status, vector_ids = connect.get_vector_ids(ip_table, info.partitions_stat[0].segments_stat[0].segment_name)
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_with_index_B(self, connect, ip_table, get_simple_index):
        '''
        target: get vector ids when there is index and with partition
        method: create partition, add vectors to it and call get_vector_ids, check if the segment contains vectors
        expected: status ok
        '''
        status = connect.create_partition(ip_table, tag)
        assert status.OK()
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status = connect.create_index(ip_table, index_type, index_param)
        assert status.OK()
        vectors = gen_vector(10, dim)
        status, ids = connect.add_vectors(ip_table, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([ip_table])
        assert status.OK()
        status, info = connect.table_info(ip_table)
        assert status.OK()
        assert info.partitions_stat[1].tag == tag
        status, vector_ids = connect.get_vector_ids(ip_table, info.partitions_stat[1].segments_stat[0].segment_name)
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_after_delete_vectors(self, connect, ip_table):
        '''
        target: get vector ids after vectors are deleted
        method: add vectors and delete a few, call get_vector_ids
        expected: status ok, vector_ids decreased after vectors deleted
        '''
        vectors = gen_vector(2, dim)
        status, ids = connect.add_vectors(ip_table, vectors)
        assert status.OK()
        delete_ids = [ids[0]]
        status = connect.delete_by_id(ip_table, delete_ids)
        status = connect.flush([ip_table])
        assert status.OK()
        status, info = connect.table_info(ip_table)
        assert status.OK()
        status, vector_ids = connect.get_vector_ids(ip_table, info.partitions_stat[0].segments_stat[0].segment_name)
        assert len(vector_ids) == 1
        assert vector_ids[0] == ids[1]


class TestGetVectorIdsJAC:
    """
    ******************************************************************
      The following cases are used to test `get_vector_ids` function
    ******************************************************************
    """
    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_without_index_A(self, connect, jac_table):
        '''
        target: get vector ids when there is no index
        method: call get_vector_ids and check if the segment contains vectors
        expected: status ok
        '''
        tmp, vectors = gen_binary_vectors(10, dim)
        status, ids = connect.add_vectors(jac_table, vectors)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        status, info = connect.table_info(jac_table)
        assert status.OK()
        status, vector_ids = connect.get_vector_ids(jac_table, info.partitions_stat[0].segments_stat[0].segment_name)
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]


    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_without_index_B(self, connect, jac_table):
        '''
        target: get vector ids when there is no index but with partition
        method: create partition, add vectors to it and call get_vector_ids, check if the segment contains vectors
        expected: status ok
        '''
        status = connect.create_partition(jac_table, tag)
        assert status.OK()
        tmp, vectors = gen_binary_vectors(10, dim)
        status, ids = connect.add_vectors(jac_table, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        status, info = connect.table_info(jac_table)
        assert status.OK()
        assert info.partitions_stat[1].tag == tag
        status, vector_ids = connect.get_vector_ids(jac_table, info.partitions_stat[1].segments_stat[0].segment_name)
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]

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

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_with_index_A(self, connect, jac_table, get_jaccard_index):
        '''
        target: get vector ids when there is index
        method: call get_vector_ids and check if the segment contains vectors
        expected: status ok
        '''
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        status = connect.create_index(jac_table, index_type, index_param)
        assert status.OK()
        tmp, vectors = gen_binary_vectors(10, dim)
        status, ids = connect.add_vectors(jac_table, vectors)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        status, info = connect.table_info(jac_table)
        assert status.OK()
        status, vector_ids = connect.get_vector_ids(jac_table, info.partitions_stat[0].segments_stat[0].segment_name)
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_with_index_B(self, connect, jac_table, get_jaccard_index):
        '''
        target: get vector ids when there is index and with partition
        method: create partition, add vectors to it and call get_vector_ids, check if the segment contains vectors
        expected: status ok
        '''
        status = connect.create_partition(jac_table, tag)
        assert status.OK()
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        status = connect.create_index(jac_table, index_type, index_param)
        assert status.OK()
        tmp, vectors = gen_binary_vectors(10, dim)
        status, ids = connect.add_vectors(jac_table, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([jac_table])
        assert status.OK()
        status, info = connect.table_info(jac_table)
        assert status.OK()
        assert info.partitions_stat[1].tag == tag
        status, vector_ids = connect.get_vector_ids(jac_table, info.partitions_stat[1].segments_stat[0].segment_name)
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_get_vector_ids_after_delete_vectors(self, connect, jac_table):
        '''
        target: get vector ids after vectors are deleted
        method: add vectors and delete a few, call get_vector_ids
        expected: status ok, vector_ids decreased after vectors deleted
        '''
        tmp, vectors = gen_binary_vectors(2, dim)
        status, ids = connect.add_vectors(jac_table, vectors)
        assert status.OK()
        delete_ids = [ids[0]]
        status = connect.delete_by_id(jac_table, delete_ids)
        status = connect.flush([jac_table])
        assert status.OK()
        status, info = connect.table_info(jac_table)
        assert status.OK()
        status, vector_ids = connect.get_vector_ids(jac_table, info.partitions_stat[0].segments_stat[0].segment_name)
        assert len(vector_ids) == 1
        assert vector_ids[0] == ids[1]