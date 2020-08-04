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
segment_size = 100
nb = 6000
tag = "1970-01-01"
field_name = "float_vector"
default_index_name = "list_index"
collection_id = "list_id_in_segment"
entity = gen_entities(1)
raw_vector, binary_entity = gen_binary_entities(1)
entities = gen_entities(nb)
raw_vectors, binary_entities = gen_binary_entities(nb)
default_fields = gen_default_fields() 


def get_segment_name(connect, collection, nb=1, vec_type='float', index_params=None):
    if vec_type != "float":
        vectors, entities = gen_binary_entities(nb)
    else:
        entities = gen_entities(nb)
    ids = connect.insert(collection, entities)
    connect.flush([collection])
    if index_params:
        connect.create_index(collection, field_name, default_index_name, index_params)
    stats = connect.get_collection_stats(collection)
    return ids, stats["partitions"][0]["segments"][0]["name"]


class TestGetVectorIdsBase:
        
    """
    ******************************************************************
      The following cases are used to test `list_id_in_segment` function
    ******************************************************************
    """
    def test_list_id_in_segment_collection_name_None(self, connect, collection):
        '''
        target: get vector ids where collection name is None
        method: call list_id_in_segment with the collection_name: None
        expected: exception raised
        '''
        collection_name = None
        ids, name = get_segment_name(connect, collection)
        with pytest.raises(Exception) as e:
            vector_ids = connect.list_id_in_segment(collection_name, name)

    def test_list_id_in_segment_collection_name_not_existed(self, connect, collection):
        '''
        target: get vector ids where collection name does not exist
        method: call list_id_in_segment with a random collection_name, which is not in db
        expected: status not ok
        '''
        collection_name = gen_unique_str(collection_id)
        ids, name = get_segment_name(connect, collection)
        with pytest.raises(Exception) as e:
            vector_ids = connect.list_id_in_segment(collection_name, name)
    
    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    def test_list_id_in_segment_collection_name_invalid(self, connect, collection, get_collection_name):
        '''
        target: get vector ids where collection name is invalid
        method: call list_id_in_segment with invalid collection_name
        expected: status not ok
        '''
        collection_name = get_collection_name
        ids, name = get_segment_name(connect, collection)
        with pytest.raises(Exception) as e:
            vector_ids = connect.list_id_in_segment(collection_name, name)

    def test_list_id_in_segment_name_None(self, connect, collection):
        '''
        target: get vector ids where segment name is None
        method: call list_id_in_segment with the name: None
        expected: exception raised
        '''
        ids, valid_name = get_segment_name(connect, collection)
        segment = None
        with pytest.raises(Exception) as e:
            vector_ids = connect.list_id_in_segment(collection, segment)

    def test_list_id_in_segment_name_not_existed(self, connect, collection):
        '''
        target: get vector ids where segment name does not exist
        method: call list_id_in_segment with a random segment name
        expected: status not ok
        '''
        ids, valid_name = get_segment_name(connect, collection)
        segment = gen_unique_str(collection_id)
        with pytest.raises(Exception) as e:
            vector_ids = connect.list_id_in_segment(collection, segment)

    def test_list_id_in_segment_without_index_A(self, connect, collection):
        '''
        target: get vector ids when there is no index
        method: call list_id_in_segment and check if the segment contains vectors
        expected: status ok
        '''
        nb = 1
        ids, name = get_segment_name(connect, collection, nb=nb)
        vector_ids = connect.list_id_in_segment(collection, name)
        # vector_ids should match ids
        assert len(vector_ids) == nb
        assert vector_ids[0] == ids[0]

    def test_list_id_in_segment_without_index_B(self, connect, collection):
        '''
        target: get vector ids when there is no index but with partition
        method: create partition, add vectors to it and call list_id_in_segment, check if the segment contains vectors
        expected: status ok
        '''
        nb = 10
        entities = gen_entities(nb)
        connect.create_partition(collection, tag)
        ids = connect.insert(collection, entities, partition_tag=tag)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats["partitions"][1]["tag"] == tag
        vector_ids = connect.list_id_in_segment(collection, stats["partitions"][1]["segments"][0]["name"])
        # vector_ids should match ids
        assert len(vector_ids) == nb
        for i in range(nb):
            assert vector_ids[i] == ids[i]

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")) == "CPU":
            if request.param["index_type"] in index_cpu_not_support():
                pytest.skip("CPU not support index_type: ivf_sq8h")
        return request.param

    def test_list_id_in_segment_with_index_A(self, connect, collection, get_simple_index):
        '''
        target: get vector ids when there is index
        method: call list_id_in_segment and check if the segment contains vectors
        expected: status ok
        '''
        ids, name = get_segment_name(connect, collection, nb=nb, index_params=get_simple_index)
        vector_ids = connect.list_id_in_segment(collection, name)
        # TODO: 

    def test_list_id_in_segment_with_index_B(self, connect, collection, get_simple_index):
        '''
        target: get vector ids when there is index and with partition
        method: create partition, add vectors to it and call list_id_in_segment, check if the segment contains vectors
        expected: status ok
        '''
        connect.create_partition(collection, tag)
        ids = connect.insert(collection, entities, partition_tag=tag)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats["partitions"][1]["tag"] == tag
        vector_ids = connect.list_id_in_segment(collection, stats["partitions"][1]["segments"][0]["name"])
        # vector_ids should match ids
        # TODO

    def test_list_id_in_segment_after_delete_vectors(self, connect, collection):
        '''
        target: get vector ids after vectors are deleted
        method: add vectors and delete a few, call list_id_in_segment
        expected: status ok, vector_ids decreased after vectors deleted
        '''
        nb = 2
        ids, name = get_segment_name(connect, collection, nb=nb)
        delete_ids = [ids[0]]
        status = connect.delete_entity_by_id(collection, delete_ids)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        vector_ids = connect.list_id_in_segment(collection, stats["partitions"][0]["segments"][0]["name"])
        assert len(vector_ids) == 1
        assert vector_ids[0] == ids[1]


class TestGetVectorIdsIP:
    """
    ******************************************************************
      The following cases are used to test `list_id_in_segment` function
    ******************************************************************
    """
    def test_list_id_in_segment_without_index_A(self, connect, ip_collection):
        '''
        target: get vector ids when there is no index
        method: call list_id_in_segment and check if the segment contains vectors
        expected: status ok
        '''
        nb = 10
        entities = gen_entities(nb)
        ids = connect.insert(ip_collection, entities)
        connect.flush([ip_collection])
        stats = connect.get_collection_stats(ip_collection)
        vector_ids = connect.list_id_in_segment(ip_collection, stats["partitions"][0]["segments"][0]["name"])
        # vector_ids should match ids
        assert len(vector_ids) == nb
        for i in range(nb):
            assert vector_ids[i] == ids[i]

    def test_list_id_in_segment_without_index_B(self, connect, ip_collection):
        '''
        target: get vector ids when there is no index but with partition
        method: create partition, add vectors to it and call list_id_in_segment, check if the segment contains vectors
        expected: status ok
        '''
        connect.create_partition(ip_collection, tag)
        nb = 10
        entities = gen_entities(nb)
        ids = connect.insert(ip_collection, entities, partition_tag=tag)
        connect.flush([ip_collection])
        stats = connect.get_collection_stats(ip_collection)
        assert stats["partitions"][1]["tag"] == tag
        vector_ids = connect.list_id_in_segment(ip_collection, stats["partitions"][1]["segments"][0]["name"])
        # vector_ids should match ids
        assert len(vector_ids) == nb
        for i in range(nb):
            assert vector_ids[i] == ids[i]

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")) == "CPU":
            if request.param["index_type"] in index_cpu_not_support():
                pytest.skip("CPU not support index_type: ivf_sq8h")
        return request.param

    def test_list_id_in_segment_with_index_A(self, connect, ip_collection, get_simple_index):
        '''
        target: get vector ids when there is index
        method: call list_id_in_segment and check if the segment contains vectors
        expected: status ok
        '''
        ids, name = get_segment_name(connect, ip_collection, nb=nb, index_params=get_simple_index)
        vector_ids = connect.list_id_in_segment(ip_collection, name)
        # TODO: 

    def test_list_id_in_segment_with_index_B(self, connect, ip_collection, get_simple_index):
        '''
        target: get vector ids when there is index and with partition
        method: create partition, add vectors to it and call list_id_in_segment, check if the segment contains vectors
        expected: status ok
        '''
        connect.create_partition(ip_collection, tag)
        ids = connect.insert(ip_collection, entities, partition_tag=tag)
        connect.flush([ip_collection])
        stats = connect.get_collection_stats(ip_collection)
        assert stats["partitions"][1]["tag"] == tag
        vector_ids = connect.list_id_in_segment(ip_collection, stats["partitions"][1]["segments"][0]["name"])
        # vector_ids should match ids
        # TODO

    def test_list_id_in_segment_after_delete_vectors(self, connect, ip_collection):
        '''
        target: get vector ids after vectors are deleted
        method: add vectors and delete a few, call list_id_in_segment
        expected: status ok, vector_ids decreased after vectors deleted
        '''
        nb = 2
        ids, name = get_segment_name(connect, ip_collection, nb=nb)
        delete_ids = [ids[0]]
        status = connect.delete_entity_by_id(ip_collection, delete_ids)
        connect.flush([ip_collection])
        stats = connect.get_collection_stats(ip_collection)
        vector_ids = connect.list_id_in_segment(ip_collection, stats["partitions"][0]["segments"][0]["name"])
        assert len(vector_ids) == 1
        assert vector_ids[0] == ids[1]


class TestGetVectorIdsJAC:
    """
    ******************************************************************
      The following cases are used to test `list_id_in_segment` function
    ******************************************************************
    """
    def test_list_id_in_segment_without_index_A(self, connect, jac_collection):
        '''
        target: get vector ids when there is no index
        method: call list_id_in_segment and check if the segment contains vectors
        expected: status ok
        '''
        nb = 10
        vectors, entities = gen_binary_entities(nb)
        ids = connect.insert(jac_collection, entities)
        connect.flush([jac_collection])
        stats = connect.get_collection_stats(jac_collection)
        vector_ids = connect.list_id_in_segment(jac_collection, stats["partitions"][0]["segments"][0]["name"])
        # vector_ids should match ids
        assert len(vector_ids) == nb
        for i in range(nb):
            assert vector_ids[i] == ids[i]

    def test_list_id_in_segment_without_index_B(self, connect, jac_collection):
        '''
        target: get vector ids when there is no index but with partition
        method: create partition, add vectors to it and call list_id_in_segment, check if the segment contains vectors
        expected: status ok
        '''
        connect.create_partition(jac_collection, tag)
        nb = 10
        vectors, entities = gen_binary_entities(nb)
        ids = connect.insert(jac_collection, entities, partition_tag=tag)
        connect.flush([jac_collection])
        stats = connect.get_collection_stats(jac_collection)
        vector_ids = connect.list_id_in_segment(jac_collection, stats["partitions"][1]["segments"][0]["name"])
        # vector_ids should match ids
        assert len(vector_ids) == nb
        for i in range(nb):
            assert vector_ids[i] == ids[i]

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_jaccard_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] in binary_support():
            return request.param
        else:
            pytest.skip("not support")

    def test_list_id_in_segment_with_index_A(self, connect, jac_collection, get_jaccard_index):
        '''
        target: get vector ids when there is index
        method: call list_id_in_segment and check if the segment contains vectors
        expected: status ok
        '''
        ids, name = get_segment_name(connect, jac_collection, nb=nb, index_params=get_jaccard_index, vec_type='binary')
        vector_ids = connect.list_id_in_segment(jac_collection, name)
        # TODO: 

    def test_list_id_in_segment_with_index_B(self, connect, jac_collection, get_jaccard_index):
        '''
        target: get vector ids when there is index and with partition
        method: create partition, add vectors to it and call list_id_in_segment, check if the segment contains vectors
        expected: status ok
        '''
        connect.create_partition(jac_collection, tag)
        ids = connect.insert(jac_collection, entities, partition_tag=tag)
        connect.flush([jac_collection])
        stats = connect.get_collection_stats(jac_collection)
        assert stats["partitions"][1]["tag"] == tag
        vector_ids = connect.list_id_in_segment(jac_collection, stats["partitions"][1]["segments"][0]["name"])
        # vector_ids should match ids
        # TODO

    def test_list_id_in_segment_after_delete_vectors(self, connect, jac_collection, get_jaccard_index):
        '''
        target: get vector ids after vectors are deleted
        method: add vectors and delete a few, call list_id_in_segment
        expected: status ok, vector_ids decreased after vectors deleted
        '''
        nb = 2
        ids, name = get_segment_name(connect, jac_collection, nb=nb, vec_type='binary', index_params=get_jaccard_index)
        delete_ids = [ids[0]]
        status = connect.delete_entity_by_id(jac_collection, delete_ids)
        connect.flush([jac_collection])
        stats = connect.get_collection_stats(jac_collection)
        vector_ids = connect.list_id_in_segment(jac_collection, stats["partitions"][0]["segments"][0]["name"])
        assert len(vector_ids) == 1
        assert vector_ids[0] == ids[1]
