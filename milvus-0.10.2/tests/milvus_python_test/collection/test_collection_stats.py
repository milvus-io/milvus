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


class TestCollectionInfoBase:
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
      The following cases are used to test `collection_info` function
    ******************************************************************
    """
    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_collection_info_name_None(self, connect, collection):
        '''
        target: get collection info where collection name is None
        method: call collection_info with the collection_name: None
        expected: status not ok
        '''
        collection_name = None
        with pytest.raises(Exception) as e:
            status, info = connect.get_collection_stats(collection_name)

    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_collection_info_name_not_existed(self, connect, collection):
        '''
        target: get collection info where collection name does not exist
        method: call collection_info with a random collection_name, which is not in db
        expected: status not ok
        '''
        collection_name = gen_unique_str("not_existed_collection")
        status, info = connect.get_collection_stats(collection_name)
        assert not status.OK()
    
    @pytest.fixture(
        scope="function",
        params=gen_invalid_collection_names()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_collection_info_name_invalid(self, connect, get_collection_name):
        '''
        target: get collection info where collection name is invalid
        method: call collection_info with invalid collection_name
        expected: status not ok
        '''
        collection_name = get_collection_name
        status, info = connect.get_collection_stats(collection_name)
        assert not status.OK()

    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_collection_info_collection_row_count(self, connect, collection):
        '''
        target: get row count with collection_info
        method: add and delete vectors, check count in collection info
        expected: status ok, count as expected
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        assert info["row_count"] == nb
        # delete a few vectors
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_entity_by_id(collection, delete_ids)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        assert info["row_count"] == nb - 2

    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_collection_info_partition_stats_A(self, connect, collection):
        '''
        target: get partition info in a collection
        method: no partition, call collection_info and check partition_stats
        expected: status ok, "_default" partition is listed
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        logging.getLogger().info(info)
        assert len(info["partitions"]) == 1
        assert info["partitions"][0]["tag"] == "_default"
        assert info["partitions"][0]["row_count"] == nb


    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_collection_info_partition_stats_B(self, connect, collection):
        '''
        target: get partition info in a collection
        method: call collection_info after partition created and check partition_stats
        expected: status ok, vectors added to partition
        '''
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(collection, tag)
        status, ids = connect.insert(collection, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        logging.getLogger().info(info)
        assert len(info["partitions"]) == 2
        assert info["partitions"][1]["tag"] == tag
        assert info["partitions"][1]["row_count"] == nb

    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_collection_info_partition_stats_C(self, connect, collection):
        '''
        target: get partition info in a collection
        method: create two partitions, add vectors in one of the partitions, call collection_info and check 
        expected: status ok, vectors added to one partition but not the other
        '''
        new_tag = "new_tag"
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(collection, tag)
        assert status.OK()
        status = connect.create_partition(collection, new_tag)
        assert status.OK()
        status, ids = connect.insert(collection, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        logging.getLogger().info(info)
        for partition in info["partitions"]:
            if partition["tag"] == tag:
                assert partition["row_count"] == nb
            else:
                assert partition["row_count"] == 0

    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_collection_info_partition_stats_D(self, connect, collection):
        '''
        target: get partition info in a collection
        method: create two partitions, add vectors in both partitions, call collection_info and check 
        expected: status ok, vectors added to both partitions
        '''
        new_tag = "new_tag"
        vectors = gen_vectors(nb, dim)
        status = connect.create_partition(collection, tag)
        assert status.OK()
        status = connect.create_partition(collection, new_tag)
        assert status.OK()
        status, ids = connect.insert(collection, vectors, partition_tag=tag)
        assert status.OK()
        status, ids = connect.insert(collection, vectors, partition_tag=new_tag)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        assert info["row_count"] == nb * 2
        for partition in info["partitions"]:
            if partition["tag"] == tag:
                assert partition["row_count"] == nb
            elif partition["tag"] == new_tag:
                assert partition["row_count"] == nb

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
    def test_get_collection_info_after_index_created(self, connect, collection, get_simple_index):
        '''
        target: test collection info after index created
        method: create collection, add vectors, create index and call collection_info 
        expected: status ok, index created and shown in segments
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.create_index(collection, index_type, index_param) 
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        logging.getLogger().info(info)
        index_string = info["partitions"][0]["segments"][0]["index_name"]
        match = self.index_string_convert(index_string, index_type)
        assert match
        assert nb == info["partitions"][0]["segments"][0]["row_count"]

    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_collection_info_after_create_same_index_repeatedly(self, connect, collection, get_simple_index):
        '''
        target: test collection info after index created repeatedly
        method: create collection, add vectors, create index and call collection_info multiple times 
        expected: status ok, index info shown in segments
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status = connect.create_index(collection, index_type, index_param)
        status = connect.create_index(collection, index_type, index_param)
        status = connect.create_index(collection, index_type, index_param)
        assert status.OK()
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        logging.getLogger().info(info)
        index_string = info["partitions"][0]["segments"][0]["index_name"]
        match = self.index_string_convert(index_string, index_type)
        assert match
        assert nb == info["partitions"][0]["segments"][0]["row_count"]

    @pytest.mark.timeout(INFO_TIMEOUT)
    def test_get_collection_info_after_create_different_index_repeatedly(self, connect, collection, get_simple_index):
        '''
        target: test collection info after index created repeatedly
        method: create collection, add vectors, create index and call collection_info multiple times 
        expected: status ok, index info shown in segments
        '''
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        index_param = {"nlist": nlist} 
        for index_type in [IndexType.FLAT, IndexType.IVFLAT, IndexType.IVF_SQ8]:
            status = connect.create_index(collection, index_type, index_param)
            assert status.OK()
            status, info = connect.get_collection_stats(collection)
            assert status.OK()
            logging.getLogger().info(info)
            index_string = info["partitions"][0]["segments"][0]["index_name"]
            match = self.index_string_convert(index_string, index_type)
            assert match
            assert nb == info["partitions"][0]["segments"][0]["row_count"]
