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
    def get_valid_name(self, connect, collection):
        vectors = gen_vector(nb, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        return info["partitions"][0]["segments"][0]["name"]
        
    """
    ******************************************************************
      The following cases are used to test `list_id_in_segment` function
    ******************************************************************
    """
    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_collection_name_None(self, connect, collection):
        '''
        target: get vector ids where collection name is None
        method: call list_id_in_segment with the collection_name: None
        expected: exception raised
        '''
        collection_name = None
        name = self.get_valid_name(connect, collection)
        with pytest.raises(Exception) as e:
            status, vector_ids = connect.list_id_in_segment(collection_name, name)

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_collection_name_not_existed(self, connect, collection):
        '''
        target: get vector ids where collection name does not exist
        method: call list_id_in_segment with a random collection_name, which is not in db
        expected: status not ok
        '''
        collection_name = gen_unique_str("not_existed_collection")
        name = self.get_valid_name(connect, collection)
        status, vector_ids = connect.list_id_in_segment(collection_name, name)
        assert not status.OK()
    
    @pytest.fixture(
        scope="function",
        params=gen_invalid_collection_names()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_collection_name_invalid(self, connect, collection, get_collection_name):
        '''
        target: get vector ids where collection name is invalid
        method: call list_id_in_segment with invalid collection_name
        expected: status not ok
        '''
        collection_name = get_collection_name
        name = self.get_valid_name(connect, collection)
        status, vector_ids = connect.list_id_in_segment(collection_name, name)
        assert not status.OK()

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_name_None(self, connect, collection):
        '''
        target: get vector ids where segment name is None
        method: call list_id_in_segment with the name: None
        expected: exception raised
        '''
        valid_name = self.get_valid_name(connect, collection)
        segment = None
        with pytest.raises(Exception) as e:
            status, vector_ids = connect.list_id_in_segment(collection, segment)

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_name_not_existed(self, connect, collection):
        '''
        target: get vector ids where segment name does not exist
        method: call list_id_in_segment with a random segment name
        expected: status not ok
        '''
        valid_name = self.get_valid_name(connect, collection)
        segment = gen_unique_str("not_existed_segment")
        status, vector_ids = connect.list_id_in_segment(collection, segment)
        logging.getLogger().info(vector_ids)
        assert not status.OK()

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_without_index_A(self, connect, collection):
        '''
        target: get vector ids when there is no index
        method: call list_id_in_segment and check if the segment contains vectors
        expected: status ok
        '''
        vectors = gen_vector(10, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        status, vector_ids = connect.list_id_in_segment(collection, info["partitions"][0]["segments"][0]["name"])
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]


    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_without_index_B(self, connect, collection):
        '''
        target: get vector ids when there is no index but with partition
        method: create partition, add vectors to it and call list_id_in_segment, check if the segment contains vectors
        expected: status ok
        '''
        status = connect.create_partition(collection, tag)
        assert status.OK()
        vectors = gen_vector(10, dim)
        status, ids = connect.insert(collection, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        assert info["partitions"][1]["tag"] == tag
        status, vector_ids = connect.list_id_in_segment(collection, info["partitions"][1]["segments"][0]["name"])
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
    def test_list_id_in_segment_with_index_A(self, connect, collection, get_simple_index):
        '''
        target: get vector ids when there is index
        method: call list_id_in_segment and check if the segment contains vectors
        expected: status ok
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status = connect.create_index(collection, index_type, index_param)
        assert status.OK()
        vectors = gen_vector(10, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        status, vector_ids = connect.list_id_in_segment(collection, info["partitions"][0]["segments"][0]["name"])
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_with_index_B(self, connect, collection, get_simple_index):
        '''
        target: get vector ids when there is index and with partition
        method: create partition, add vectors to it and call list_id_in_segment, check if the segment contains vectors
        expected: status ok
        '''
        status = connect.create_partition(collection, tag)
        assert status.OK()
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status = connect.create_index(collection, index_type, index_param)
        assert status.OK()
        vectors = gen_vector(10, dim)
        status, ids = connect.insert(collection, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        assert info["partitions"][1]["tag"] == tag
        status, vector_ids = connect.list_id_in_segment(collection, info["partitions"][1]["segments"][0]["name"])
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_after_delete_vectors(self, connect, collection):
        '''
        target: get vector ids after vectors are deleted
        method: add vectors and delete a few, call list_id_in_segment
        expected: status ok, vector_ids decreased after vectors deleted
        '''
        vectors = gen_vector(2, dim)
        status, ids = connect.insert(collection, vectors)
        assert status.OK()
        delete_ids = [ids[0]]
        status = connect.delete_entity_by_id(collection, delete_ids)
        status = connect.flush([collection])
        assert status.OK()
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        status, vector_ids = connect.list_id_in_segment(collection, info["partitions"][0]["segments"][0]["name"])
        assert len(vector_ids) == 1
        assert vector_ids[0] == ids[1]


class TestGetVectorIdsIP:
    """
    ******************************************************************
      The following cases are used to test `list_id_in_segment` function
    ******************************************************************
    """
    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_without_index_A(self, connect, ip_collection):
        '''
        target: get vector ids when there is no index
        method: call list_id_in_segment and check if the segment contains vectors
        expected: status ok
        '''
        vectors = gen_vector(10, dim)
        status, ids = connect.insert(ip_collection, vectors)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        status, vector_ids = connect.list_id_in_segment(ip_collection, info["partitions"][0]["segments"][0]["name"])
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]


    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_without_index_B(self, connect, ip_collection):
        '''
        target: get vector ids when there is no index but with partition
        method: create partition, add vectors to it and call list_id_in_segment, check if the segment contains vectors
        expected: status ok
        '''
        status = connect.create_partition(ip_collection, tag)
        assert status.OK()
        vectors = gen_vector(10, dim)
        status, ids = connect.insert(ip_collection, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        assert info["partitions"][1]["tag"] == tag
        status, vector_ids = connect.list_id_in_segment(ip_collection, info["partitions"][1]["segments"][0]["name"])
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
    def test_list_id_in_segment_with_index_A(self, connect, ip_collection, get_simple_index):
        '''
        target: get vector ids when there is index
        method: call list_id_in_segment and check if the segment contains vectors
        expected: status ok
        '''
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status = connect.create_index(ip_collection, index_type, index_param)
        assert status.OK()
        vectors = gen_vector(10, dim)
        status, ids = connect.insert(ip_collection, vectors)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        status, vector_ids = connect.list_id_in_segment(ip_collection, info["partitions"][0]["segments"][0]["name"])
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_with_index_B(self, connect, ip_collection, get_simple_index):
        '''
        target: get vector ids when there is index and with partition
        method: create partition, add vectors to it and call list_id_in_segment, check if the segment contains vectors
        expected: status ok
        '''
        status = connect.create_partition(ip_collection, tag)
        assert status.OK()
        index_param = get_simple_index["index_param"]
        index_type = get_simple_index["index_type"]
        status = connect.create_index(ip_collection, index_type, index_param)
        assert status.OK()
        vectors = gen_vector(10, dim)
        status, ids = connect.insert(ip_collection, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([ip_collection])
        assert status.OK()
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        assert info["partitions"][1]["tag"] == tag
        status, vector_ids = connect.list_id_in_segment(ip_collection, info["partitions"][1]["segments"][0]["name"])
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_after_delete_vectors(self, connect, ip_collection):
        '''
        target: get vector ids after vectors are deleted
        method: add vectors and delete a few, call list_id_in_segment
        expected: status ok, vector_ids decreased after vectors deleted
        '''
        vectors = gen_vector(2, dim)
        status, ids = connect.insert(ip_collection, vectors)
        assert status.OK()
        delete_ids = [ids[0]]
        status = connect.delete_entity_by_id(ip_collection, delete_ids)
        status = connect.flush([ip_collection])
        assert status.OK()
        status, info = connect.get_collection_stats(ip_collection)
        assert status.OK()
        status, vector_ids = connect.list_id_in_segment(ip_collection, info["partitions"][0]["segments"][0]["name"])
        assert len(vector_ids) == 1
        assert vector_ids[0] == ids[1]


class TestGetVectorIdsJAC:
    """
    ******************************************************************
      The following cases are used to test `list_id_in_segment` function
    ******************************************************************
    """
    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_without_index_A(self, connect, jac_collection):
        '''
        target: get vector ids when there is no index
        method: call list_id_in_segment and check if the segment contains vectors
        expected: status ok
        '''
        tmp, vectors = gen_binary_vectors(10, dim)
        status, ids = connect.insert(jac_collection, vectors)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        status, vector_ids = connect.list_id_in_segment(jac_collection, info["partitions"][0]["segments"][0]["name"])
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_without_index_B(self, connect, jac_collection):
        '''
        target: get vector ids when there is no index but with partition
        method: create partition, add vectors to it and call list_id_in_segment, check if the segment contains vectors
        expected: status ok
        '''
        status = connect.create_partition(jac_collection, tag)
        assert status.OK()
        tmp, vectors = gen_binary_vectors(10, dim)
        status, ids = connect.insert(jac_collection, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        assert info["partitions"][1]["tag"] == tag
        status, vector_ids = connect.list_id_in_segment(jac_collection, info["partitions"][1]["segments"][0]["name"])
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
    def test_list_id_in_segment_with_index_A(self, connect, jac_collection, get_jaccard_index):
        '''
        target: get vector ids when there is index
        method: call list_id_in_segment and check if the segment contains vectors
        expected: status ok
        '''
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        status = connect.create_index(jac_collection, index_type, index_param)
        assert status.OK()
        tmp, vectors = gen_binary_vectors(10, dim)
        status, ids = connect.insert(jac_collection, vectors)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        status, vector_ids = connect.list_id_in_segment(jac_collection, info["partitions"][0]["segments"][0]["name"])
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_with_index_B(self, connect, jac_collection, get_jaccard_index):
        '''
        target: get vector ids when there is index and with partition
        method: create partition, add vectors to it and call list_id_in_segment, check if the segment contains vectors
        expected: status ok
        '''
        status = connect.create_partition(jac_collection, tag)
        assert status.OK()
        index_param = get_jaccard_index["index_param"]
        index_type = get_jaccard_index["index_type"]
        status = connect.create_index(jac_collection, index_type, index_param)
        assert status.OK()
        tmp, vectors = gen_binary_vectors(10, dim)
        status, ids = connect.insert(jac_collection, vectors, partition_tag=tag)
        assert status.OK()
        status = connect.flush([jac_collection])
        assert status.OK()
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        assert info["partitions"][1]["tag"] == tag
        status, vector_ids = connect.list_id_in_segment(jac_collection, info["partitions"][1]["segments"][0]["name"])
        # vector_ids should match ids
        assert len(vector_ids) == 10
        for i in range(10):
            assert vector_ids[i] == ids[i]

    @pytest.mark.timeout(GET_TIMEOUT)
    def test_list_id_in_segment_after_delete_vectors(self, connect, jac_collection):
        '''
        target: get vector ids after vectors are deleted
        method: add vectors and delete a few, call list_id_in_segment
        expected: status ok, vector_ids decreased after vectors deleted
        '''
        tmp, vectors = gen_binary_vectors(2, dim)
        status, ids = connect.insert(jac_collection, vectors)
        assert status.OK()
        delete_ids = [ids[0]]
        status = connect.delete_entity_by_id(jac_collection, delete_ids)
        status = connect.flush([jac_collection])
        assert status.OK()
        status, info = connect.get_collection_stats(jac_collection)
        assert status.OK()
        status, vector_ids = connect.list_id_in_segment(jac_collection, info["partitions"][0]["segments"][0]["name"])
        assert len(vector_ids) == 1
        assert vector_ids[0] == ids[1]
