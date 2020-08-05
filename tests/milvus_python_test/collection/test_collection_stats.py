import time
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from utils import *

dim = 128
segment_row_count = 5000
nprobe = 1
top_k = 1
epsilon = 0.0001
tag = "1970-01-01"
nb = 6000
nlist = 1024
collection_id = "collection_stats"
field_name = "float_vector"
entity = gen_entities(1)
raw_vector, binary_entity = gen_binary_entities(1)
entities = gen_entities(nb)
raw_vectors, binary_entities = gen_binary_entities(nb)
default_fields = gen_default_fields()


class TestStatsBase:
    """
    ******************************************************************
      The following cases are used to test `collection_stats` function
    ******************************************************************
    """
    
    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")) == "CPU":
            if request.param["index_type"] in index_cpu_not_support():
                pytest.skip("CPU not support index_type: ivf_sq8h")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_jaccard_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] in binary_support():
            request.param["metric_type"] = "JACCARD"
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    def test_get_collection_stats_name_not_existed(self, connect, collection):
        '''
        target: get collection stats where collection name does not exist
        method: call collection_stats with a random collection_name, which is not in db
        expected: status not ok
        '''
        collection_name = gen_unique_str(collection_id)
        with pytest.raises(Exception) as e:
            stats = connect.get_collection_stats(collection_name)

    @pytest.mark.level(2)
    def test_get_collection_stats_name_invalid(self, connect, get_collection_name):
        '''
        target: get collection stats where collection name is invalid
        method: call collection_stats with invalid collection_name
        expected: status not ok
        '''
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            stats = connect.get_collection_stats(collection_name)

    def test_get_collection_stats_empty(self, connect, collection):
        '''
        target: get collection stats where no entity in collection
        method: call collection_stats in empty collection
        expected: segment = []
        '''
        stats = connect.get_collection_stats(collection)
        assert stats["row_count"] == 0
        assert len(stats["partitions"]) == 1
        assert stats["partitions"][0]["tag"] == "_default"
        assert stats["partitions"][0]["row_count"] == 0

    def test_get_collection_stats_batch(self, connect, collection):
        '''
        target: get row count with collection_stats
        method: add entities, check count in collection info
        expected: count as expected
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats["row_count"] == nb
        assert len(stats["partitions"]) == 1
        assert stats["partitions"][0]["tag"] == "_default"
        assert stats["partitions"][0]["row_count"] == nb

    def test_get_collection_stats_single(self, connect, collection):
        '''
        target: get row count with collection_stats
        method: add entity one by one, check count in collection info
        expected: count as expected
        '''
        nb = 10
        for i in range(nb):
            ids = connect.insert(collection, entity)
            connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats["row_count"] == nb
        assert len(stats["partitions"]) == 1
        assert stats["partitions"][0]["tag"] == "_default"
        assert stats["partitions"][0]["row_count"] == nb

    # TODO
    @pytest.mark.level(2)
    def test_get_collection_stats_after_delete(self, connect, collection):
        '''
        target: get row count with collection_stats
        method: add and delete entities, check count in collection info
        expected: status ok, count as expected
        '''
        ids = connect.insert(collection, entities)
        status = connect.flush([collection])
        delete_ids = [ids[0], ids[-1]]
        connect.delete_entity_by_id(collection, delete_ids)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats["row_count"] == nb - 2
        assert stats["partitions"][0]["segments"][0]["data_size"] > 0
        assert stats["partitions"][0]["segments"][0]["index_name"] == "FLAT"

    # TODO
    @pytest.mark.level(2)
    def test_get_collection_stats_after_compact_parts(self, connect, collection):
        '''
        target: get row count with collection_stats
        method: add and delete entities, and compact collection, check count in collection info
        expected: status ok, count as expected
        '''
        ids = connect.insert(collection, entities)
        status = connect.flush([collection])
        delete_ids = ids[:3000]
        connect.delete_entity_by_id(collection, delete_ids)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        logging.getLogger().info(stats)
        assert stats["row_count"] == nb - 3000
        compact_before = stats["partitions"][0]["segments"][0]["data_size"]
        connect.compact(collection)
        stats = connect.get_collection_stats(collection)
        logging.getLogger().info(stats)
        compact_after = stats["partitions"][0]["segments"][0]["data_size"]
        # pdb.set_trace()
        assert compact_before > compact_after

    # TODO
    @pytest.mark.level(2)
    def test_get_collection_stats_after_compact_delete_one(self, connect, collection):
        '''
        target: get row count with collection_stats
        method: add and delete one entity, and compact collection, check count in collection info
        expected: status ok, count as expected
        '''
        ids = connect.insert(collection, entities)
        status = connect.flush([collection])
        delete_ids = ids[:1]
        connect.delete_entity_by_id(collection, delete_ids)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        logging.getLogger().info(stats)
        compact_before = stats["partitions"][0]["segments"][0]["data_size"]
        connect.compact(collection)
        stats = connect.get_collection_stats(collection)
        logging.getLogger().info(stats)
        compact_after = stats["partitions"][0]["segments"][0]["data_size"]
        # pdb.set_trace()
        assert compact_before == compact_after

    def test_get_collection_stats_partition(self, connect, collection):
        '''
        target: get partition info in a collection
        method: call collection_stats after partition created and check partition_stats
        expected: status ok, vectors added to partition
        '''
        connect.create_partition(collection, tag)
        ids = connect.insert(collection, entities, partition_tag=tag)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert len(stats["partitions"]) == 2
        assert stats["partitions"][1]["tag"] == tag
        assert stats["partitions"][1]["row_count"] == nb

    def test_get_collection_stats_partitions(self, connect, collection):
        '''
        target: get partition info in a collection
        method: create two partitions, add vectors in one of the partitions, call collection_stats and check 
        expected: status ok, vectors added to one partition but not the other
        '''
        new_tag = "new_tag"
        connect.create_partition(collection, tag)
        connect.create_partition(collection, new_tag)
        ids = connect.insert(collection, entities, partition_tag=tag)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        for partition in stats["partitions"]:
            if partition["tag"] == tag:
                assert partition["row_count"] == nb
            else:
                assert partition["row_count"] == 0
        ids = connect.insert(collection, entities, partition_tag=new_tag)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        for partition in stats["partitions"]:
            if partition["tag"] in [tag, new_tag]:
                assert partition["row_count"] == nb
    
    # TODO
    @pytest.mark.level(2)
    def test_get_collection_stats_after_index_created(self, connect, collection, get_simple_index):
        '''
        target: test collection info after index created
        method: create collection, add vectors, create index and call collection_stats 
        expected: status ok, index created and shown in segments
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        connect.create_index(collection, field_name, get_simple_index)
        stats = connect.get_collection_stats(collection)
        logging.getLogger().info(stats)
        assert stats["partitions"][0]["segments"][0]["row_count"] == nb
        assert stats["partitions"][0]["segments"][0]["index_name"] == get_simple_index["index_type"]

    # TODO
    @pytest.mark.level(2)
    def test_get_collection_stats_after_index_created_ip(self, connect, collection, get_simple_index):
        '''
        target: test collection info after index created
        method: create collection, add vectors, create index and call collection_stats 
        expected: status ok, index created and shown in segments
        '''
        get_simple_index["metric_type"] = "IP"
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        get_simple_index.update({"metric_type": "IP"})
        connect.create_index(collection, field_name, get_simple_index)
        stats = connect.get_collection_stats(collection)
        logging.getLogger().info(stats)
        assert stats["partitions"][0]["segments"][0]["row_count"] == nb
        assert stats["partitions"][0]["segments"][0]["index_name"] == get_simple_index["index_type"]

    def test_get_collection_stats_after_index_created_jac(self, connect, binary_collection, get_jaccard_index):
        '''
        target: test collection info after index created
        method: create collection, add binary entities, create index and call collection_stats 
        expected: status ok, index created and shown in segments
        '''
        ids = connect.insert(binary_collection, binary_entities)
        connect.flush([binary_collection])
        connect.create_index(binary_collection, "binary_vector", get_jaccard_index)
        stats = connect.get_collection_stats(binary_collection)
        logging.getLogger().info(stats)
        assert stats["partitions"][0]["segments"][0]["row_count"] == nb
        assert stats["partitions"][0]["segments"][0]["index_name"] == get_jaccard_index["index_type"]

    # TODO
    @pytest.mark.level(2)
    def test_get_collection_stats_after_create_different_index(self, connect, collection):
        '''
        target: test collection info after index created repeatedly
        method: create collection, add vectors, create index and call collection_stats multiple times 
        expected: status ok, index info shown in segments
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        for index_type in ["IVF_FLAT", "IVF_SQ8"]:
            connect.create_index(collection, field_name, {"index_type": index_type, "params":{"nlist": 1024}, "metric_type": "L2"})
            stats = connect.get_collection_stats(collection)
            logging.getLogger().info(stats)
            assert stats["partitions"][0]["segments"][0]["index_name"] == index_type
            assert stats["partitions"][0]["segments"][0]["row_count"] == nb

    def test_collection_count_multi_collections(self, connect):
        '''
        target: test collection rows_count is correct or not with multiple collections of L2
        method: create collection and add entities in it,
            assert the value returned by count_entities method is equal to length of entities
        expected: row count in segments
        '''
        collection_list = []
        collection_num = 10
        for i in range(collection_num):
            collection_name = gen_unique_str(collection_id)
            collection_list.append(collection_name)
            connect.create_collection(collection_name, default_fields)
            res = connect.insert(collection_name, entities)
        connect.flush(collection_list)
        for i in range(collection_num):
            stats = connect.get_collection_stats(collection_list[i])
            assert stats["partitions"][0]["segments"][0]["row_count"] == nb
            connect.drop_collection(collection_list[i])

    # TODO
    @pytest.mark.level(2)
    def test_collection_count_multi_collections_indexed(self, connect):
        '''
        target: test collection rows_count is correct or not with multiple collections of L2
        method: create collection and add entities in it,
            assert the value returned by count_entities method is equal to length of entities
        expected: row count in segments
        '''
        collection_list = []
        collection_num = 10
        for i in range(collection_num):
            collection_name = gen_unique_str(collection_id)
            collection_list.append(collection_name)
            connect.create_collection(collection_name, default_fields)
            res = connect.insert(collection_name, entities)
            connect.flush(collection_list)
            if i % 2:
                connect.create_index(collection_name, field_name, {"index_type": "IVF_SQ8", "params":{"nlist": 1024}, "metric_type": "L2"})
            else:
                connect.create_index(collection_name, field_name, {"index_type": "IVF_FLAT","params":{ "nlist": 1024}, "metric_type": "L2"})
        for i in range(collection_num):
            stats = connect.get_collection_stats(collection_list[i])
            assert stats["partitions"][0]["segments"][0]["row_count"] == nb
            if i % 2:
                assert stats["partitions"][0]["segments"][0]["index_name"] == "IVF_SQ8"
            else:
                assert stats["partitions"][0]["segments"][0]["index_name"] == "IVF_FLAT"
            connect.drop_collection(collection_list[i])
