import time
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from utils import *

dim = 128
index_file_size = 10
COMPACT_TIMEOUT = 180
nprobe = 1
top_k = 1
tag = "1970-01-01"
nb = 6000
segment_row_count = 5000
entity = gen_entities(1)
entities = gen_entities(nb)
raw_vector, binary_entity = gen_binary_entities(1)
raw_vectors, binary_entities = gen_binary_entities(nb)
default_fields = gen_default_fields()
default_binary_fields = gen_binary_default_fields()
field_name = default_float_vec_field_name
default_single_query = {
    "bool": {
        "must": [
            {"vector": {field_name: {"topk": 10, "query": gen_vectors(1, dim),
                                     "params": {"nprobe": 10}}}}
        ]
    }
}

def ip_query():
    query = copy.deepcopy(default_single_query)
    query["bool"]["must"][0]["vector"][field_name].update({"metric_type": "IP"})
    return query


class TestCompactBase:
    """
    ******************************************************************
      The following cases are used to test `compact` function
    ******************************************************************
    """
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_compact_collection_name_None(self, connect, collection):
        '''
        target: compact collection where collection name is None
        method: compact with the collection_name: None
        expected: exception raised
        '''
        collection_name = None
        with pytest.raises(Exception) as e:
            status = connect.compact(collection_name)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_compact_collection_name_not_existed(self, connect, collection):
        '''
        target: compact collection not existed
        method: compact with a random collection_name, which is not in db
        expected: exception raised
        '''
        collection_name = gen_unique_str("not_existed")
        with pytest.raises(Exception) as e:
            status = connect.compact(collection_name)
    
    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_collection_name(self, request):
        yield request.param

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_compact_collection_name_invalid(self, connect, get_collection_name):
        '''
        target: compact collection with invalid name
        method: compact with invalid collection_name
        expected: exception raised
        '''
        collection_name = get_collection_name
        with pytest.raises(Exception) as e:
            status = connect.compact(collection_name)
            # assert not status.OK()
    
    @pytest.mark.level(2)
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_entity_and_compact(self, connect, collection):
        '''
        target: test add entity and compact
        method: add entity and compact collection
        expected: data_size before and after Compact
        '''
        # vector = gen_single_vector(dim)
        ids = connect.insert(collection, entity)
        assert len(ids) == 1
        connect.flush([collection])
        # get collection info before compact
        info = connect.get_collection_stats(collection)
        logging.getLogger().info(info)
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact
        info = connect.get_collection_stats(collection)
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)

    # TODO
    @pytest.mark.level(2)
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_insert_and_compact(self, connect, collection):
        '''
        target: test add entities and compact 
        method: add entities and compact collection
        expected: data_size before and after Compact
        '''
        # entities = gen_vector(nb, dim)
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        # get collection info before compact
        info = connect.get_collection_stats(collection)
        # assert status.OK()
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact
        info = connect.get_collection_stats(collection)
        # assert status.OK()
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    @pytest.mark.skip(reason="delete not support yet")
    def test_insert_delete_part_and_compact(self, connect, collection):
        '''
        target: test add entities, delete part of them and compact
        method: add entities, delete a few and compact collection
        expected: status ok, data size maybe is smaller after compact
        '''
        ids = connect.insert(collection, entities)
        assert len(ids) == nb
        connect.flush([collection])
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_entity_by_id(collection, delete_ids)
        assert status.OK()
        connect.flush([collection])
        # get collection info before compact
        info = connect.get_collection_stats(collection)
        logging.getLogger().info(info["partitions"])
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        logging.getLogger().info(size_before)
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact
        info = connect.get_collection_stats(collection)
        logging.getLogger().info(info["partitions"])
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        logging.getLogger().info(size_after)
        assert(size_before >= size_after)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    @pytest.mark.skip(reason="delete not support yet")
    def test_insert_delete_all_and_compact(self, connect, collection):
        '''
        target: test add entities, delete them and compact 
        method: add entities, delete all and compact collection
        expected: status ok, no data size in collection info because collection is empty
        '''
        ids = connect.insert(collection, entities)
        assert len(ids) == nb
        connect.flush([collection])
        status = connect.delete_entity_by_id(collection, ids)
        assert status.OK()
        connect.flush([collection])
        # get collection info before compact
        info = connect.get_collection_stats(collection)
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact
        info = connect.get_collection_stats(collection)
        logging.getLogger().info(info["partitions"])
        assert not info["partitions"][0]["segments"]

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    @pytest.mark.skip(reason="delete not support yet")
    def test_insert_partition_delete_half_and_compact(self, connect, collection):
        '''
        target: test add entities into partition, delete them and compact 
        method: add entities, delete half of entities in partition and compact collection
        expected: status ok, data_size less than the older version
        '''
        connect.create_partition(collection, tag)
        assert connect.has_partition(collection, tag)
        ids = connect.insert(collection, entities, partition_tag=tag)
        connect.flush([collection])
        info = connect.get_collection_stats(collection)
        logging.getLogger().info(info["partitions"])

        delete_ids = ids[:3000]
        status = connect.delete_entity_by_id(collection, delete_ids)
        assert status.OK()
        connect.flush([collection])
        # get collection info before compact
        info = connect.get_collection_stats(collection)
        logging.getLogger().info(info["partitions"])
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact
        info_after = connect.get_collection_stats(collection)
        logging.getLogger().info(info_after["partitions"])
        assert info["partitions"][1]["segments"][0]["data_size"] > info_after["partitions"][1]["segments"][0]["data_size"]

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        if str(connect._cmd("mode")) == "GPU":
            if not request.param["index_type"] not in ivf():
                pytest.skip("Only support index_type: idmap/ivf")
        if str(connect._cmd("mode")) == "CPU":
            if request.param["index_type"] in index_cpu_not_support():
                pytest.skip("CPU not support index_type: ivf_sq8h")
        return request.param

    @pytest.mark.skip(reason="create_index not support yet")
    def test_compact_after_index_created(self, connect, collection, get_simple_index):
        '''
        target: test compact collection after index created
        method: add entities, create index, delete part of entities and compact
        expected: status ok, index description no change, data size smaller after compact
        '''
        count = 10
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        connect.create_index(collection, field_name, get_simple_index)
        connect.flush([collection])
        # get collection info before compact
        info = connect.get_collection_stats(collection)
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        logging.getLogger().info(info["partitions"])
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_entity_by_id(collection, delete_ids)
        assert status.OK()
        connect.flush([collection])
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact
        info = connect.get_collection_stats(collection)
        logging.getLogger().info(info["partitions"])
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before >= size_after)

    # TODO
    @pytest.mark.level(2)
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_entity_and_compact_twice(self, connect, collection):
        '''
        target: test add entity and compact twice
        method: add entity and compact collection twice
        expected: status ok, data size no change
        '''
        ids = connect.insert(collection, entity)
        connect.flush([collection])
        # get collection info before compact
        info = connect.get_collection_stats(collection)
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(collection)
        assert status.OK()
        connect.flush([collection])
        # get collection info after compact
        info = connect.get_collection_stats(collection)
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact twice
        info = connect.get_collection_stats(collection)
        size_after_twice = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_after == size_after_twice)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    @pytest.mark.skip(reason="delete not support yet")
    def test_insert_delete_part_and_compact_twice(self, connect, collection):
        '''
        target: test add entities, delete part of them and compact twice
        method: add entities, delete part and compact collection twice
        expected: status ok, data size smaller after first compact, no change after second
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_entity_by_id(collection, delete_ids)
        assert status.OK()
        connect.flush([collection])
        # get collection info before compact
        info = connect.get_collection_stats(collection)
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact
        info = connect.get_collection_stats(collection)
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before >= size_after)
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact twice
        info = connect.get_collection_stats(collection)
        size_after_twice = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_after == size_after_twice)

    # TODO
    @pytest.mark.level(2)
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_compact_multi_collections(self, connect):
        '''
        target: test compact works or not with multiple collections
        method: create 50 collections, add entities into them and compact in turn
        expected: status ok
        '''
        nq = 100
        num_collections = 50
        entities = gen_entities(nq)
        collection_list = []
        for i in range(num_collections):
            collection_name = gen_unique_str("test_compact_multi_collection_%d" % i)
            collection_list.append(collection_name)
            connect.create_collection(collection_name, default_fields)
        time.sleep(6)
        for i in range(num_collections):
            ids = connect.insert(collection_list[i], entities)
            status = connect.compact(collection_list[i])
            assert status.OK()

    @pytest.mark.level(2)
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_entity_after_compact(self, connect, collection):
        '''
        target: test add entity after compact
        method: after compact operation, add entity
        expected: status ok, entity added
        '''
        ids = connect.insert(collection, entities)
        assert len(ids) == nb
        connect.flush([collection])
        # get collection info before compact
        info = connect.get_collection_stats(collection)
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(collection)
        assert status.OK()
        # get collection info after compact
        info = connect.get_collection_stats(collection)
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)
        ids = connect.insert(collection, entity)
        connect.flush([collection])
        res = connect.count_entities(collection)
        assert res == nb+1

    @pytest.mark.skip(reason="delete not support yet")
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_index_creation_after_compact(self, connect, collection, get_simple_index):
        '''
        target: test index creation after compact
        method: after compact operation, create index
        expected: status ok, index description no change
        '''
        ids = connect.insert(collection, entities)
        connect.flush([collection])
        status = connect.delete_entity_by_id(collection, ids[:10])
        assert status.OK()
        connect.flush([collection])
        status = connect.compact(collection)
        assert status.OK()
        status = connect.create_index(collection, field_name, get_simple_index)
        assert status.OK()
        # status, result = connect.get_index_info(collection)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    @pytest.mark.skip(reason="delete not support yet")
    def test_delete_entities_after_compact(self, connect, collection):
        '''
        target: test delete entities after compact
        method: after compact operation, delete entities
        expected: status ok, entities deleted
        '''
        ids = connect.insert(collection, entities)
        assert len(ids) == nb
        connect.flush([collection])
        status = connect.compact(collection)
        assert status.OK()
        connect.flush([collection])
        status = connect.delete_entity_by_id(collection, ids)
        assert status.OK()
        connect.flush([collection])
        assert connect.count_entities(collection) == 0

    @pytest.mark.skip(reason="search not support yet")
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_search_after_compact(self, connect, collection):
        '''
        target: test search after compact
        method: after compact operation, search vector
        expected: status ok
        '''
        ids = connect.insert(collection, entities)
        assert len(ids) == nb
        connect.flush([collection])
        status = connect.compact(collection)
        assert status.OK()
        query = copy.deepcopy(default_single_query)
        query["bool"]["must"][0]["vector"][field_name]["query"] = [entity[-1]["values"][0], entities[-1]["values"][0],
                                                                   entities[-1]["values"][-1]]
        res = connect.search(collection, query)
        logging.getLogger().debug(res)
        assert len(res) == len(query["bool"]["must"][0]["vector"][field_name]["query"])
        assert res[0]._distances[0] > epsilon
        assert res[1]._distances[0] < epsilon
        assert res[2]._distances[0] < epsilon

    # TODO: enable
    @pytest.mark.skip(reason="delete not support yet")
    def _test_compact_server_crashed_recovery(self, connect, collection):
        '''
        target: test compact when server crashed unexpectedly and restarted
        method: add entities, delete and compact collection; server stopped and restarted during compact
        expected: status ok, request recovered
        '''
        entities = gen_vectors(nb * 100, dim)
        status, ids = connect.insert(collection, entities)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        delete_ids = ids[0:1000]
        status = connect.delete_entity_by_id(collection, delete_ids)
        assert status.OK()
        status = connect.flush([collection])
        assert status.OK()
        # start to compact, kill and restart server
        logging.getLogger().info("compact starting...")
        status = connect.compact(collection)
        # pdb.set_trace()
        assert status.OK()
        # get collection info after compact
        status, info = connect.get_collection_stats(collection)
        assert status.OK()
        assert info["partitions"][0].count == nb * 100 - 1000


class TestCompactBinary:
    """
    ******************************************************************
      The following cases are used to test `compact` function
    ******************************************************************
    """
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    # TODO
    @pytest.mark.level(2)
    def test_add_entity_and_compact(self, connect, binary_collection):
        '''
        target: test add binary vector and compact
        method: add vector and compact collection
        expected: status ok, vector added
        '''
        ids = connect.insert(binary_collection, binary_entity)
        assert len(ids) == 1
        connect.flush([binary_collection])
        # get collection info before compact
        info = connect.get_collection_stats(binary_collection)
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(binary_collection)
        assert status.OK()
        # get collection info after compact
        info = connect.get_collection_stats(binary_collection)
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)

    # TODO
    @pytest.mark.level(2)
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_insert_and_compact(self, connect, binary_collection):
        '''
        target: test add entities with binary vector and compact
        method: add entities and compact collection
        expected: status ok, entities added
        '''
        ids = connect.insert(binary_collection, binary_entities)
        assert len(ids) == nb
        connect.flush([binary_collection])
        # get collection info before compact
        info = connect.get_collection_stats(binary_collection)
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(binary_collection)
        assert status.OK()
        # get collection info after compact
        info = connect.get_collection_stats(binary_collection)
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    @pytest.mark.skip(reason="delete not support yet")
    def test_insert_delete_part_and_compact(self, connect, binary_collection):
        '''
        target: test add entities, delete part of them and compact 
        method: add entities, delete a few and compact collection
        expected: status ok, data size is smaller after compact
        '''
        ids = connect.insert(binary_collection, binary_entities)
        assert len(ids) == nb
        connect.flush([binary_collection])
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_entity_by_id(binary_collection, delete_ids)
        assert status.OK()
        connect.flush([binary_collection])
        # get collection info before compact
        info = connect.get_collection_stats(binary_collection)
        logging.getLogger().info(info["partitions"])
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        logging.getLogger().info(size_before)
        status = connect.compact(binary_collection)
        assert status.OK()
        # get collection info after compact
        info = connect.get_collection_stats(binary_collection)
        logging.getLogger().info(info["partitions"])
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        logging.getLogger().info(size_after)
        assert(size_before >= size_after)
    
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    @pytest.mark.skip(reason="delete not support yet")
    def test_insert_delete_all_and_compact(self, connect, binary_collection):
        '''
        target: test add entities, delete them and compact 
        method: add entities, delete all and compact collection
        expected: status ok, no data size in collection info because collection is empty
        '''
        ids = connect.insert(binary_collection, binary_entities)
        assert len(ids) == nb
        connect.flush([binary_collection])
        status = connect.delete_entity_by_id(binary_collection, ids)
        assert status.OK()
        connect.flush([binary_collection])
        # get collection info before compact
        info = connect.get_collection_stats(binary_collection)
        status = connect.compact(binary_collection)
        assert status.OK()
        # get collection info after compact
        info = connect.get_collection_stats(binary_collection)
        assert status.OK()
        logging.getLogger().info(info["partitions"])
        assert not info["partitions"][0]["segments"]

    # TODO
    @pytest.mark.level(2)
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_entity_and_compact_twice(self, connect, binary_collection):
        '''
        target: test add entity and compact twice
        method: add entity and compact collection twice
        expected: status ok
        '''
        ids = connect.insert(binary_collection, binary_entity)
        assert len(ids) == 1
        connect.flush([binary_collection])
        # get collection info before compact
        info = connect.get_collection_stats(binary_collection)
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(binary_collection)
        assert status.OK()
        # get collection info after compact
        info = connect.get_collection_stats(binary_collection)
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)
        status = connect.compact(binary_collection)
        assert status.OK()
        # get collection info after compact twice
        info = connect.get_collection_stats(binary_collection)
        size_after_twice = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_after == size_after_twice)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    @pytest.mark.skip(reason="delete not support yet")
    def test_insert_delete_part_and_compact_twice(self, connect, binary_collection):
        '''
        target: test add entities, delete part of them and compact twice
        method: add entities, delete part and compact collection twice
        expected: status ok, data size smaller after first compact, no change after second
        '''
        ids = connect.insert(binary_collection, binary_entities)
        assert len(ids) == nb
        connect.flush([binary_collection])
        delete_ids = [ids[0], ids[-1]]
        status = connect.delete_entity_by_id(binary_collection, delete_ids)
        assert status.OK()
        connect.flush([binary_collection])
        # get collection info before compact
        info = connect.get_collection_stats(binary_collection)
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(binary_collection)
        assert status.OK()
        # get collection info after compact
        info = connect.get_collection_stats(binary_collection)
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before >= size_after)
        status = connect.compact(binary_collection)
        assert status.OK()
        # get collection info after compact twice
        info = connect.get_collection_stats(binary_collection)
        size_after_twice = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_after == size_after_twice)

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    @pytest.mark.skip(reason="delete not support yet")
    def test_compact_multi_collections(self, connect):
        '''
        target: test compact works or not with multiple collections
        method: create 10 collections, add entities into them and compact in turn
        expected: status ok
        '''
        nq = 100
        num_collections = 10
        tmp, entities = gen_binary_entities(nq)
        collection_list = []
        for i in range(num_collections):
            collection_name = gen_unique_str("test_compact_multi_collection_%d" % i)
            collection_list.append(collection_name)
            connect.create_collection(collection_name, default_fields)
        for i in range(num_collections):
            ids = connect.insert(collection_list[i], entities)
            assert len(ids) == nq
            status = connect.delete_entity_by_id(collection_list[i], [ids[0], ids[-1]])
            assert status.OK()
            connect.flush([collection_list[i]])
            status = connect.compact(collection_list[i])
            assert status.OK()
            status = connect.drop_collection(collection_list[i])
            assert status.OK()

    @pytest.mark.level(2)
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_add_entity_after_compact(self, connect, binary_collection):
        '''
        target: test add entity after compact
        method: after compact operation, add entity
        expected: status ok, entity added
        '''
        ids = connect.insert(binary_collection, binary_entities)
        connect.flush([binary_collection])
        # get collection info before compact
        info = connect.get_collection_stats(binary_collection)
        size_before = info["partitions"][0]["segments"][0]["data_size"]
        status = connect.compact(binary_collection)
        assert status.OK()
        # get collection info after compact
        info = connect.get_collection_stats(binary_collection)
        size_after = info["partitions"][0]["segments"][0]["data_size"]
        assert(size_before == size_after)
        ids = connect.insert(binary_collection, binary_entity)
        connect.flush([binary_collection])
        res = connect.count_entities(binary_collection)
        assert res == nb + 1

    @pytest.mark.timeout(COMPACT_TIMEOUT)
    @pytest.mark.skip(reason="delete not support yet")
    def test_delete_entities_after_compact(self, connect, binary_collection):
        '''
        target: test delete entities after compact
        method: after compact operation, delete entities
        expected: status ok, entities deleted
        '''
        ids = connect.insert(binary_collection, binary_entities)
        connect.flush([binary_collection])
        status = connect.compact(binary_collection)
        assert status.OK()
        connect.flush([binary_collection])
        status = connect.delete_entity_by_id(binary_collection, ids)
        assert status.OK()
        connect.flush([binary_collection])
        res = connect.count_entities(binary_collection)
        assert res == 0

    @pytest.mark.skip(reason="search not support yet")
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_search_after_compact(self, connect, binary_collection):
        '''
        target: test search after compact
        method: after compact operation, search vector
        expected: status ok
        '''
        ids = connect.insert(binary_collection, binary_entities)
        assert len(ids) == nb
        connect.flush([binary_collection])
        status = connect.compact(binary_collection)
        assert status.OK()
        query_vecs = [raw_vectors[0]]
        distance = jaccard(query_vecs[0], raw_vectors[0])
        query = copy.deepcopy(default_single_query)
        query["bool"]["must"][0]["vector"][field_name]["query"] = [binary_entities[-1]["values"][0],
                                                                   binary_entities[-1]["values"][-1]]
        res = connect.search(binary_collection, query)
        assert abs(res[0]._distances[0]-distance) <= epsilon

    # TODO:
    @pytest.mark.skip(reason="search not support yet")
    @pytest.mark.timeout(COMPACT_TIMEOUT)
    def test_search_after_compact_ip(self, connect, collection):
        '''
        target: test search after compact
        method: after compact operation, search vector
        expected: status ok
        '''
        ids = connect.insert(collection, entities)
        assert len(ids) == nb
        connect.flush([collection])
        status = connect.compact(collection)
        query = ip_query()
        query["bool"]["must"][0]["vector"][field_name]["query"] = [entity[-1]["values"][0], entities[-1]["values"][0],
                                                                   entities[-1]["values"][-1]]
        res = connect.search(collection, query)
        logging.getLogger().info(res)
        assert len(res) == len(query["bool"]["must"][0]["vector"][field_name]["query"])
        assert res[0]._distances[0] < 1 - epsilon
        assert res[1]._distances[0] > 1 - epsilon
        assert res[2]._distances[0] > 1 - epsilon
